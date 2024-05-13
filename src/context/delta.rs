use crate::context::SeafowlContext;
#[cfg(test)]
use crate::frontend::http::tests::deterministic_uuid;
use crate::object_store::wrapped::InternalObjectStore;

use bytes::BytesMut;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::parquet::basic::{Compression, ZstdLevel};
use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    datasource::TableProvider,
    error::DataFusionError,
    execution::context::TaskContext,
    parquet::{arrow::ArrowWriter, file::properties::WriterProperties},
    physical_plan::{ExecutionPlan, ExecutionPlanProperties},
    sql::TableReference,
};
use deltalake::kernel::{Action, Add, Schema as DeltaSchema};
use deltalake::operations::{
    convert_to_delta::ConvertToDeltaBuilder, create::CreateBuilder,
    transaction::CommitBuilder,
};
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::writer::create_add;
use deltalake::DeltaTable;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use std::fs::File;
use std::sync::Arc;
use tempfile::{NamedTempFile, TempPath};

use tokio::fs::File as AsyncFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};
use uuid::Uuid;

// Max Parquet row group size, in rows. This is what the ArrowWriter uses to determine how many
// rows to buffer in memory before flushing them out to disk. The default for this is 1024^2, which
// means that we're effectively buffering a whole partition in memory, causing issues on RAM-limited
// environments.
const MAX_ROW_GROUP_SIZE: usize = 65536;

// Just a simple read buffer to reduce the number of syscalls when filling in the part buffer.
const PARTITION_FILE_BUFFER_SIZE: usize = 128 * 1024;
// This denotes the threshold size for an individual multipart request payload prior to upload.
// It dictates the memory usage, as we'll need to to keep each part in memory until sent.
const PARTITION_FILE_MIN_PART_SIZE: usize = 5 * 1024 * 1024;
// Controls how many multipart upload tasks we let run in parallel; this is in part dictated by the
// fact that object store concurrently uploads parts for each of our tasks. That concurrency in
// turn is hard coded to 8 (https://github.com/apache/arrow-rs/blob/master/object_store/src/aws/mod.rs#L145)
// meaning that with 2 partition upload tasks x 8 part upload tasks x 5MB we have 80MB of memory usage
const PARTITION_FILE_UPLOAD_MAX_CONCURRENCY: usize = 2;

#[cfg(test)]
fn get_uuid() -> Uuid {
    deterministic_uuid()
}

#[cfg(not(test))]
fn get_uuid() -> Uuid {
    Uuid::new_v4()
}

/// Open a temporary file to write partition and return a handle and a writer for it.
fn temp_partition_file_writer(
    arrow_schema: SchemaRef,
) -> Result<(TempPath, ArrowWriter<File>)> {
    let partition_file =
        NamedTempFile::new().expect("Open a temporary file to write partition");

    // Hold on to the path of the file, in case we need to just move it instead of
    // uploading the data to the object store. This can be a consistency/security issue, but the
    // worst someone can do is swap out the file with something else if the original temporary
    // file gets deleted and an attacker creates a temporary file with the same name. In that case,
    // we can end up copying an arbitrary file to the object store, which requires access to the
    // machine anyway (and at that point there's likely other things that the attacker can do, like
    // change the write access control settings).
    let path = partition_file.into_temp_path();

    let file_writer = File::options().write(true).open(&path)?;

    let writer_properties = WriterProperties::builder()
        .set_max_row_group_size(MAX_ROW_GROUP_SIZE)
        .set_compression(Compression::ZSTD(ZstdLevel::default())) // This defaults to MINIMUM_LEVEL
        .build();
    let writer =
        ArrowWriter::try_new(file_writer, arrow_schema, Some(writer_properties))?;
    Ok((path, writer))
}

/// Execute a plan and upload the results to object storage as Parquet files, indexing them.
/// Partially taken from DataFusion's plan_to_parquet with some additions (file stats, using a DiskManager)
pub async fn plan_to_object_store(
    state: &SessionState,
    plan: &Arc<dyn ExecutionPlan>,
    store: Arc<InternalObjectStore>,
    prefix: Path,
    max_partition_size: u32,
) -> Result<Vec<Add>> {
    let mut current_partition_size = 0;
    let (mut current_partition_file_path, mut writer) =
        temp_partition_file_writer(plan.schema())?;
    let mut partition_file_paths = vec![current_partition_file_path];
    let mut partition_metadata = vec![];
    let mut tasks = vec![];

    // Iterate over Datafusion partitions and re-chunk them, since we want to enforce a pre-defined
    // partition size limit, which is not guaranteed by DF.
    for i in 0..plan.output_partitioning().partition_count() {
        let task_ctx = Arc::new(TaskContext::from(state));
        let mut stream = plan.execute(i, task_ctx)?;

        while let Some(batch) = stream.next().await {
            let mut batch = batch?;

            let mut leftover_partition_capacity =
                (max_partition_size - current_partition_size) as usize;

            while batch.num_rows() > leftover_partition_capacity {
                if leftover_partition_capacity > 0 {
                    // Fill up the remaining capacity in the slice
                    writer
                        .write(&batch.slice(0, leftover_partition_capacity))
                        .map_err(DataFusionError::from)?;
                    // Trim away the part that made it to the current partition
                    batch = batch.slice(
                        leftover_partition_capacity,
                        batch.num_rows() - leftover_partition_capacity,
                    );
                }

                // Roll-over into the next partition: close partition writer, reset partition size
                // counter and open new temp file + writer.
                let file_metadata = writer.close().map_err(DataFusionError::from)?;
                partition_metadata.push(file_metadata);

                current_partition_size = 0;
                leftover_partition_capacity = max_partition_size as usize;

                (current_partition_file_path, writer) =
                    temp_partition_file_writer(plan.schema())?;
                partition_file_paths.push(current_partition_file_path);
            }

            current_partition_size += batch.num_rows() as u32;
            writer.write(&batch).map_err(DataFusionError::from)?;
        }
    }
    let file_metadata = writer.close().map_err(DataFusionError::from)?;
    partition_metadata.push(file_metadata);

    info!("Starting upload of partition objects");
    let partitions_uuid = get_uuid();

    let sem = Arc::new(Semaphore::new(PARTITION_FILE_UPLOAD_MAX_CONCURRENCY));
    for (part, (partition_file_path, metadata)) in partition_file_paths
        .into_iter()
        .zip(partition_metadata)
        .enumerate()
    {
        let permit = Arc::clone(&sem).acquire_owned().await.ok();

        let store = store.clone();
        let prefix = prefix.clone();
        let handle: tokio::task::JoinHandle<Result<Add>> =
            tokio::task::spawn(async move {
                // Move the ownership of the semaphore permit into the task
                let _permit = permit;

                // This is taken from delta-rs `PartitionWriter::next_data_path`
                let file_name =
                    format!("part-{part:0>5}-{partitions_uuid}-c000.snappy.parquet");
                // NB: in order to exploit the fast upload path for local FS store we need to use
                // the internal object store here. However it is not rooted at the table directory
                // root, so we need to fully qualify the path with the appropriate uuid prefix.
                // On the other hand, when creating deltalake `Add`s below we only need the relative
                // path (just the file name).
                let location = prefix.child(file_name.clone());

                let size = tokio::fs::metadata(
                    partition_file_path
                        .to_str()
                        .expect("Temporary Parquet file in the FS root"),
                )
                .await?
                .len() as i64;

                // For local FS stores, we can just move the file to the target location
                if let Some(result) =
                    store.fast_upload(&partition_file_path, &location).await
                {
                    result?;
                } else {
                    let file = AsyncFile::open(partition_file_path).await?;
                    let mut reader =
                        BufReader::with_capacity(PARTITION_FILE_BUFFER_SIZE, file);
                    let mut part_buffer =
                        BytesMut::with_capacity(PARTITION_FILE_MIN_PART_SIZE);

                    let (multipart_id, mut writer) =
                        store.inner.put_multipart(&location).await?;

                    let error: std::io::Error;
                    let mut eof_counter = 0;
                    loop {
                        match reader.read_buf(&mut part_buffer).await {
                            Ok(0) if part_buffer.is_empty() => {
                                // We've reached EOF and there are no pending writes to flush.
                                // As per the docs size = 0 doesn't seem to guarantee that we've reached EOF, so we use
                                // a heuristic: if we encounter Ok(0) 3 times in a row it's safe to assume it's EOF.
                                // Another potential workaround is to use `stream_position` + `stream_len` to determine
                                // whether we've reached the end (`stream_len` is nightly-only experimental API atm)
                                eof_counter += 1;
                                if eof_counter >= 3 {
                                    break;
                                } else {
                                    continue;
                                }
                            }
                            Ok(size)
                                if size != 0
                                    && part_buffer.len()
                                        < PARTITION_FILE_MIN_PART_SIZE =>
                            {
                                // Keep filling the part buffer until it surpasses the minimum required size
                                eof_counter = 0;
                                continue;
                            }
                            Ok(_) => {
                                let part_size = part_buffer.len();
                                debug!("Uploading part with {} bytes", part_size);
                                match writer.write_all(&part_buffer[..part_size]).await {
                                    Ok(_) => {
                                        part_buffer.clear();
                                        continue;
                                    }
                                    Err(err) => error = err,
                                }
                            }
                            Err(err) => error = err,
                        }

                        warn!(
                            "Aborting multipart partition upload due to an error: {:?}",
                            error
                        );
                        store
                            .inner
                            .abort_multipart(&location, &multipart_id)
                            .await
                            .ok();
                        return Err(DataFusionError::IoError(error));
                    }

                    writer.shutdown().await?;
                }

                // Create the corresponding Add action; currently we don't support partition columns
                // which simplifies things.
                let add = create_add(&Default::default(), file_name, size, &metadata)?;

                Ok(add)
            });
        tasks.push(handle);
    }

    futures::future::join_all(tasks)
        .await
        .into_iter()
        .map(|x| x.unwrap_or_else(|e| Err(DataFusionError::External(Box::new(e)))))
        .collect()
}

pub(super) enum CreateDeltaTableDetails {
    EmptyTable(Schema),
    FromPath(Path),
}

impl SeafowlContext {
    pub(super) async fn create_delta_table<'a>(
        &'a self,
        name: impl Into<TableReference<'a>>,
        details: CreateDeltaTableDetails,
    ) -> Result<Arc<DeltaTable>> {
        let resolved_ref = self.resolve_table_ref(name);
        let schema_name = resolved_ref.schema.clone();
        let table_name = resolved_ref.table.clone();

        let _ = self
            .metastore
            .schemas
            .get(&self.default_catalog, &schema_name)
            .await?;

        // NB: there's also a uuid generated below for table's `DeltaTableMetaData::id`, so it would
        // be nice if those two could match somehow
        let (table_uuid, table) = match details {
            CreateDeltaTableDetails::EmptyTable(schema) => {
                // TODO: we could be doing this inside the DB itself (i.e. `... DEFAULT gen_random_uuid()`
                // in Postgres and `... DEFAULT (uuid())` in SQLite) however we won't be able to do it until
                // sqlx 0.7 is released (which has libsqlite3-sys > 0.25, with the SQLite version that has
                // the `uuid()` function).
                // Then we could create the table in our catalog first and try to create the delta table itself
                // with the returned uuid (and delete the catalog entry if the object store creation fails).
                // On the other hand that would complicate etag testing logic.
                let table_uuid = get_uuid();
                let table_log_store = self
                    .internal_object_store
                    .get_log_store(&table_uuid.to_string());
                let delta_schema = DeltaSchema::try_from(&schema)?;

                let table = CreateBuilder::new()
                    .with_log_store(table_log_store)
                    .with_table_name(&*table_name)
                    .with_columns(delta_schema.fields().clone())
                    .with_comment(format!(
                        "Created by Seafowl {}",
                        env!("CARGO_PKG_VERSION")
                    ))
                    .await?;
                (table_uuid, table)
            }
            CreateDeltaTableDetails::FromPath(path) => {
                // For now interpret the path as containing only the final UUID table prefix,
                // in accordance with Seafowl convention
                let table_uuid = Uuid::try_parse(path.as_ref()).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Unable to parse the UUID path of the table: {e}"
                    ))
                })?;
                let table_log_store = self
                    .internal_object_store
                    .get_log_store(&table_uuid.to_string());
                let table = ConvertToDeltaBuilder::new()
                    .with_log_store(table_log_store)
                    .with_table_name(&*table_name)
                    .with_save_mode(SaveMode::Overwrite)
                    .with_comment(format!(
                        "Converted by Seafowl {}",
                        env!("CARGO_PKG_VERSION")
                    ))
                    .await?;
                (table_uuid, table)
            }
        };

        // We still persist the table into our own catalog, one reason is us being able to load all
        // tables and their schemas in bulk to satisfy information_schema queries.
        // Another is to keep track of table uuid's, which are used to construct the table uri.
        // We may look into doing this via delta-rs somehow eventually.
        self.metastore
            .tables
            .create(
                &self.default_catalog,
                &schema_name,
                &table_name,
                TableProvider::schema(&table).as_ref(),
                table_uuid,
            )
            .await?;

        let table = Arc::new(table);
        self.inner.register_table(resolved_ref, table.clone())?;
        debug!("Created new table {table}");
        Ok(table)
    }

    /// Generate the Delta table builder and execute the write
    pub(super) async fn plan_to_delta_table<'a>(
        &self,
        name: impl Into<TableReference<'a>>,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<DeltaTable> {
        let table_uuid = self.get_table_uuid(name).await?;
        let prefix = table_uuid.to_string();
        let table_log_store = self.internal_object_store.get_log_store(&prefix);
        let table_prefix = self.internal_object_store.table_prefix(&prefix);

        // Upload partition files to table's root directory
        let adds = plan_to_object_store(
            &self.inner.state(),
            plan,
            self.internal_object_store.clone(),
            table_prefix,
            self.config.misc.max_partition_size,
        )
        .await?;

        // Commit the write into a new version
        let mut table = DeltaTable::new(table_log_store.clone(), Default::default());
        table.load().await?;

        let actions: Vec<Action> = adds.into_iter().map(Action::Add).collect();
        let op = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: None,
            predicate: None,
        };

        let version = self.commit(actions, &table, op).await?;

        // TODO: if `DeltaTable::get_version_timestamp` was globally public we could also pass the
        // exact version timestamp, instead of creating one automatically in our own catalog (which
        // could lead to minor timestamp differences).
        self.metastore
            .tables
            .create_new_version(table_uuid, version)
            .await?;

        debug!("Written table version {} for {table}", version);
        Ok(table)
    }

    pub(super) async fn commit(
        &self,
        actions: Vec<Action>,
        table: &DeltaTable,
        op: DeltaOperation,
    ) -> Result<i64> {
        Ok(CommitBuilder::default()
            .with_actions(actions)
            .build(Some(table.snapshot()?), table.log_store(), op)
            .map_err(|e| {
                DataFusionError::Execution(format!("Transaction commit failed: {e}"))
            })?
            .await?
            .version)
    }

    // Cleanup the table objects in the storage
    pub async fn delete_delta_table<'a>(
        &self,
        table_name: impl Into<TableReference<'a>>,
    ) -> Result<()> {
        let table = self.try_get_delta_table(table_name).await?;
        let store = table.object_store();

        // List all objects with the table prefix...
        let objects = store.list(None).map_ok(|m| m.location).boxed();

        // ... and delete them in bulk (if applicable).
        let _paths = store
            .delete_stream(objects)
            .try_collect::<Vec<Path>>()
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_utils::{in_memory_context, in_memory_context_with_test_db};
    use crate::config::schema;
    use crate::context::delta::plan_to_object_store;
    use crate::object_store::wrapped::InternalObjectStore;
    use crate::testutils::assert_uploaded_objects;
    use arrow::{array::Int32Array, datatypes::DataType, record_batch::RecordBatch};
    use arrow_schema::{Field, Schema};
    use datafusion::physical_plan::{memory::MemoryExec, ExecutionPlan};
    use object_store::{local::LocalFileSystem, memory::InMemory, path::Path};
    use rstest::rstest;
    use serde_json::{json, Value};
    use std::sync::Arc;
    use tempfile::TempDir;
    use uuid::Uuid;

    const PART_0_FILE_NAME: &str =
        "part-00000-01020304-0506-4708-890a-0b0c0d0e0f10-c000.snappy.parquet";
    const PART_1_FILE_NAME: &str =
        "part-00001-01020304-0506-4708-890a-0b0c0d0e0f10-c000.snappy.parquet";

    #[rstest]
    #[case::in_memory_object_store_standard(false)]
    #[case::local_object_store_test_renames(true)]
    #[tokio::test]
    async fn test_plan_to_object_storage(#[case] is_local: bool) {
        let ctx = in_memory_context().await;

        // Make a SELECT VALUES(...) query
        let execution_plan = ctx
            .plan_query(
                r#"
                SELECT * FROM (VALUES
                    ('2022-01-01', 42, 'one'),
                    ('2022-01-02', 12, 'two'),
                    ('2022-01-03', 32, 'three'),
                    ('2022-01-04', 22, 'four'))
                AS t(timestamp, integer, varchar);"#,
            )
            .await
            .unwrap();

        let (object_store, _tmpdir) = if is_local {
            // Return tmp_dir to the upper scope so that we don't delete the temporary directory
            // until after the test is done
            let tmp_dir = TempDir::new().unwrap();

            (
                Arc::new(InternalObjectStore::new(
                    Arc::new(LocalFileSystem::new_with_prefix(tmp_dir.path()).unwrap()),
                    schema::ObjectStore::Local(schema::Local {
                        data_dir: tmp_dir.path().to_string_lossy().to_string(),
                    }),
                )),
                Some(tmp_dir),
            )
        } else {
            (
                Arc::new(InternalObjectStore::new(
                    Arc::new(InMemory::new()),
                    schema::ObjectStore::InMemory(schema::InMemory {}),
                )),
                None,
            )
        };

        let table_uuid = Uuid::default();
        let adds = plan_to_object_store(
            &ctx.inner.state(),
            &execution_plan,
            object_store.clone(),
            object_store.table_prefix(&table_uuid.to_string()),
            2,
        )
        .await
        .unwrap();

        assert_eq!(adds.len(), 2);
        assert_eq!(
            vec![
                (
                    adds[0].path.clone(),
                    adds[0].size,
                    adds[0].partition_values.is_empty(),
                    adds[0].data_change,
                    serde_json::from_str::<Value>(
                        adds[0].stats.clone().unwrap().as_str()
                    )
                    .unwrap(),
                ),
                (
                    adds[1].path.clone(),
                    adds[1].size,
                    adds[1].partition_values.is_empty(),
                    adds[1].data_change,
                    serde_json::from_str::<Value>(
                        adds[1].stats.clone().unwrap().as_str()
                    )
                    .unwrap(),
                )
            ],
            vec![
                (
                    PART_0_FILE_NAME.to_string(),
                    1298,
                    true,
                    true,
                    json!({
                        "numRecords": 2,
                        "minValues": {
                            "integer": 12,
                            "timestamp": "2022-01-01",
                            "varchar": "one",
                        },
                        "maxValues": {
                            "integer": 42,
                            "timestamp": "2022-01-02",
                            "varchar": "two",
                        },
                        "nullCount": {
                            "integer": 0,
                            "timestamp": 0,
                            "varchar": 0,
                        },
                    }),
                ),
                (
                    PART_1_FILE_NAME.to_string(),
                    1313,
                    true,
                    true,
                    json!({
                        "numRecords": 2,
                        "minValues": {
                            "integer": 22,
                            "timestamp": "2022-01-03",
                            "varchar": "four",
                        },
                        "maxValues": {
                            "integer": 32,
                            "timestamp": "2022-01-04",
                            "varchar": "three",
                        },
                        "nullCount": {
                            "integer": 0,
                            "timestamp": 0,
                            "varchar": 0,
                        },
                    }),
                )
            ]
        );

        assert_uploaded_objects(
            object_store
                .get_log_store(&table_uuid.to_string())
                .object_store(),
            vec![PART_0_FILE_NAME.to_string(), PART_1_FILE_NAME.to_string()],
        )
        .await;
    }

    #[rstest]
    #[case::record_batches_smaller_than_partitions(
    5,
    vec![vec![vec![0, 1, 2], vec![3, 4, 5]], vec![vec![6, 7, 8], vec![9, 10, 11]]],
    vec![vec![0, 1, 2, 3, 4], vec![5, 6, 7, 8, 9], vec![10, 11]])
    ]
    #[case::record_batches_same_size_as_partitions(
    3,
    vec![vec![vec![0, 1, 2], vec![3, 4, 5]], vec![vec![6, 7, 8], vec![9, 10, 11]]],
    vec![vec![0, 1, 2], vec![3, 4, 5], vec![6, 7, 8], vec![9, 10, 11]])
    ]
    #[case::record_batches_larer_than_partitions(
    2,
    vec![vec![vec![0, 1, 2], vec![3, 4, 5]], vec![vec![6, 7, 8], vec![9, 10, 11]]],
    vec![vec![0, 1], vec![2, 3], vec![4, 5], vec![6, 7], vec![8, 9], vec![10, 11]])
    ]
    #[case::record_batches_of_irregular_size(
    3,
    vec![vec![vec![0, 1], vec![2, 3, 4]], vec![vec![5]], vec![vec![6, 7, 8, 9], vec![10, 11]]],
    vec![vec![0, 1, 2], vec![3, 4, 5], vec![6, 7, 8], vec![9, 10, 11]])
    ]
    #[tokio::test]
    async fn test_plan_to_object_storage_partition_chunking(
        #[case] max_partition_size: u32,
        #[case] input_partitions: Vec<Vec<Vec<i32>>>,
        #[case] output_partitions: Vec<Vec<i32>>,
    ) {
        let ctx = in_memory_context_with_test_db().await;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "some_number",
            DataType::Int32,
            true,
        )]));

        let df_partitions: Vec<Vec<RecordBatch>> = input_partitions
            .iter()
            .map(|partition| {
                partition
                    .iter()
                    .map(|record_batch| {
                        RecordBatch::try_new(
                            schema.clone(),
                            vec![Arc::new(Int32Array::from(record_batch.clone()))],
                        )
                        .unwrap()
                    })
                    .collect()
            })
            .collect();

        // Make a dummy execution plan that will return the data we feed it
        let execution_plan: Arc<dyn ExecutionPlan> = Arc::new(
            MemoryExec::try_new(df_partitions.as_slice(), schema, None).unwrap(),
        );

        let object_store = Arc::new(InternalObjectStore::new(
            Arc::new(InMemory::new()),
            schema::ObjectStore::InMemory(schema::InMemory {}),
        ));
        let adds = plan_to_object_store(
            &ctx.inner.state(),
            &execution_plan,
            object_store,
            Path::from("test"),
            max_partition_size,
        )
        .await
        .unwrap();

        assert_eq!(adds.len(), output_partitions.len(),);

        for i in 0..output_partitions.len() {
            assert_eq!(
                serde_json::from_str::<Value>(adds[i].stats.clone().unwrap().as_str())
                    .unwrap(),
                json!({
                    "numRecords": output_partitions[i].len(),
                    "minValues": {
                        "some_number": output_partitions[i].iter().min(),
                    },
                    "maxValues": {
                        "some_number": output_partitions[i].iter().max(),
                    },
                    "nullCount": {
                        "some_number": 0,
                    },
                }),
            )
        }
    }

    #[tokio::test]
    async fn test_create_table_without_columns_fails() {
        let context = Arc::new(in_memory_context().await);
        let err = context
            .plan_query("CREATE TABLE test_table")
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("At least one column must be defined to create a table."));
    }
}
