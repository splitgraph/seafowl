use crate::statements::*;
use object_store::path::Path as ObjectStorePath;
use serial_test::serial;
use std::fs;
use std::io::{self, ErrorKind};
use std::path::Path;

/// Make a SeafowlContext that's connected to a legacy SQLite catalog copy
async fn make_context_with_local_sqlite(
    source_dir: &str,
    data_dir: String,
) -> DefaultSeafowlContext {
    assert_ne!(data_dir.as_str(), source_dir);

    // Copy the legacy catalog into the provided data directory
    copy_dir(Path::new(source_dir), Path::new(&data_dir)).unwrap();

    let config_text = format!(
        r#"
[object_store]
type = "local"
data_dir = "{data_dir}"
[catalog]
type = "sqlite"
dsn = "{data_dir}/seafowl.sqlite""#
    );

    let config = load_config_from_string(&config_text, true, None).unwrap();
    build_context(&config).await.unwrap()
}

fn copy_dir(src: &Path, dst: &Path) -> io::Result<()> {
    if !dst.exists() {
        fs::create_dir_all(dst)?;
    }

    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        let new_path = dst.join(path.file_name().ok_or_else(|| {
            io::Error::new(ErrorKind::Other, "Failed to get file name")
        })?);

        if entry.file_type()?.is_dir() {
            copy_dir(&path, &new_path)?;
        } else {
            fs::copy(&path, &new_path)?;
        }
    }

    Ok(())
}

#[rstest]
#[should_panic(
    expected = r#"There are still some legacy partitions that need to be removed before running migrations for Seafowl v0.4:
7fbfeeeade71978b4ae82cd3d97b8c1bd9ae7ab9a7a78ee541b66209cfd7722d.parquet
9ae6f4222893474551037d0e44ff223ca5ea8e703d141b14835025923a66ab50.parquet
ea192fa7ae3b4abca9ded70e480c188e2c260ece02a810e5f1e2be41b0d6c0f6.parquet
534e5cc396e5b24725993145821b864cbfb07c2d8d7116f3d60d28bc02900861.parquet
Please go through the migration instructions laid out in https://github.com/splitgraph/seafowl/issues/392."#
)]
#[case::with_legacy_v0_2("tests/data/seafowl-0.2-legacy-data/", None)]
#[should_panic(
    expected = r#"There are still some legacy tables that need to be removed before running migrations for Seafowl v0.4:
default.public.legacy_table
Please go through the migration instructions laid out in https://github.com/splitgraph/seafowl/issues/392."#
)]
#[case::with_legacy_v0_3("tests/data/seafowl-0.3-legacy-data/", None)]
#[case::with_legacy_and_env_var_v0_3("tests/data/seafowl-0.3-legacy-data/", Some("true"))]
#[case::without_legacy_v0_3("tests/data/seafowl-0.3-no-legacy-data/", None)]
#[tokio::test]
#[serial]
async fn test_legacy_tables(
    #[case] source_dir: &str,
    #[case] migration_var: Option<&str>,
) {
    let data_dir = TempDir::new().unwrap();

    if let Some(value) = migration_var {
        env::set_var("SEAFOWL_0_4_AUTODROP_LEGACY_TABLES", value);
    } else {
        env::remove_var("SEAFOWL_0_4_AUTODROP_LEGACY_TABLES");
    }

    let context =
        make_context_with_local_sqlite(source_dir, data_dir.path().display().to_string())
            .await;

    // Ensure no legacy Parquet files (located in in object store root)
    testutils::assert_uploaded_objects(
        context.internal_object_store.clone(),
        vec![
            ObjectStorePath::from("7ccc716e-a32e-4055-ad10-208484424861/_delta_log/00000000000000000000.json"),
            ObjectStorePath::from("7ccc716e-a32e-4055-ad10-208484424861/_delta_log/00000000000000000001.json"),
            ObjectStorePath::from("7ccc716e-a32e-4055-ad10-208484424861/_delta_log/00000000000000000002.json"),
            ObjectStorePath::from("7ccc716e-a32e-4055-ad10-208484424861/_delta_log/00000000000000000003.json"),
            ObjectStorePath::from("7ccc716e-a32e-4055-ad10-208484424861/_delta_log/00000000000000000004.json"),
            ObjectStorePath::from("7ccc716e-a32e-4055-ad10-208484424861/part-00000-0d277fdc-a127-4ae4-9ff3-9831cbcabfc4-c000.snappy.parquet"),
            ObjectStorePath::from("7ccc716e-a32e-4055-ad10-208484424861/part-00000-17387ea4-f6df-4a80-8bcb-69ce1eebe384-c000.snappy.parquet"),
            ObjectStorePath::from("7ccc716e-a32e-4055-ad10-208484424861/part-00000-5d7463c5-f612-4f7f-9cf9-76ad519ead13-c000.snappy.parquet"),
            ObjectStorePath::from("7ccc716e-a32e-4055-ad10-208484424861/part-00000-cf19c9ba-7c72-4dd9-97ad-ca0f9eab9ae1-c000.snappy.parquet"),
            ObjectStorePath::from("seafowl.sqlite"),
            ObjectStorePath::from("seafowl.sqlite-shm"),
            ObjectStorePath::from("seafowl.sqlite-wal"),
        ],
    )
        .await;

    let plan = context.plan_query("SELECT * FROM new_table").await.unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+---------------------+------------+------------------+-----------------+----------------+",
        "| some_time           | some_value | some_other_value | some_bool_value | some_int_value |",
        "+---------------------+------------+------------------+-----------------+----------------+",
        "| 2022-01-01T20:01:01 | 42.0       |                  |                 | 1111           |",
        "| 2022-01-01T20:02:02 | 43.0       |                  |                 | 2222           |",
        "| 2022-01-01T20:03:03 | 44.0       |                  |                 | 3333           |",
        "|                     | 45.0       |                  |                 |                |",
        "|                     | 46.0       |                  |                 |                |",
        "|                     | 47.0       |                  |                 |                |",
        "|                     | 46.0       |                  |                 |                |",
        "|                     | 47.0       |                  |                 |                |",
        "|                     | 48.0       |                  |                 |                |",
        "|                     | 42.0       |                  |                 |                |",
        "|                     | 41.0       |                  |                 |                |",
        "|                     | 40.0       |                  |                 |                |",
        "+---------------------+------------+------------------+-----------------+----------------+",
    ];

    assert_batches_eq!(expected, &results);
}
