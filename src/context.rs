// DataFusion bindings

use futures::TryStreamExt;

use hashbrown::HashMap;
use sqlparser::ast::{
    ColumnDef as SQLColumnDef, ColumnOption, DataType as SQLDataType, Ident, Statement,
    TableFactor, TableWithJoins,
};
use std::sync::Arc;

pub use datafusion::error::{DataFusionError as Error, Result};
use datafusion::{
    arrow::{
        datatypes::{
            DataType, Field, Schema, SchemaRef, TimeUnit, DECIMAL_DEFAULT_SCALE,
            DECIMAL_MAX_PRECISION,
        },
        record_batch::RecordBatch,
    },
    error::DataFusionError,
    execution::context::TaskContext,
    logical_plan::{plan::Extension, Column, CreateMemoryTable, DFSchema, LogicalPlan, ToDFSchema},
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, empty::EmptyExec, EmptyRecordBatchStream,
        ExecutionPlan, SendableRecordBatchStream,
    },
    prelude::SessionContext,
    sql::{parser::DFParser, planner::SqlToRel, TableReference},
};

use crate::{
    catalog::Catalog,
    data_types::{PhysicalRegion, PhysicalRegionColumn},
    nodes::{Assignment, CreateTable, Delete, Insert, Update},
    schema::Schema as SeafowlSchema,
};

// Copied from datafusion::sql::utils (private)

/// Returns a validated `DataType` for the specified precision and
/// scale
fn make_decimal_type(precision: Option<u64>, scale: Option<u64>) -> Result<DataType> {
    // postgres like behavior
    let (precision, scale) = match (precision, scale) {
        (Some(p), Some(s)) => (p as usize, s as usize),
        (Some(p), None) => (p as usize, 0),
        (None, Some(_)) => {
            return Err(DataFusionError::Internal(
                "Cannot specify only scale for decimal data type".to_string(),
            ))
        }
        (None, None) => (DECIMAL_MAX_PRECISION, DECIMAL_DEFAULT_SCALE),
    };

    // Arrow decimal is i128 meaning 38 maximum decimal digits
    if precision > DECIMAL_MAX_PRECISION || scale > precision {
        return Err(DataFusionError::Internal(format!(
            "For decimal(precision, scale) precision must be less than or equal to 38 and scale can't be greater than precision. Got ({}, {})",
            precision, scale
        )));
    } else {
        Ok(DataType::Decimal(precision, scale))
    }
}

// Normalize an identifier to a lowercase string unless the identifier is quoted.
fn normalize_ident(id: &Ident) -> String {
    match id.quote_style {
        Some(_) => id.value.clone(),
        None => id.value.to_ascii_lowercase(),
    }
}

// Copied from SqlRel (private there)
fn build_schema(columns: Vec<SQLColumnDef>) -> Result<Schema> {
    let mut fields = Vec::with_capacity(columns.len());

    for column in columns {
        let data_type = make_data_type(&column.data_type)?;
        let allow_null = column
            .options
            .iter()
            .any(|x| x.option == ColumnOption::Null);
        fields.push(Field::new(
            &normalize_ident(&column.name),
            data_type,
            allow_null,
        ));
    }

    Ok(Schema::new(fields))
}

/// Maps the SQL type to the corresponding Arrow `DataType`
fn make_data_type(sql_type: &SQLDataType) -> Result<DataType> {
    match sql_type {
        SQLDataType::BigInt(_) => Ok(DataType::Int64),
        SQLDataType::Int(_) => Ok(DataType::Int32),
        SQLDataType::SmallInt(_) => Ok(DataType::Int16),
        SQLDataType::Char(_) | SQLDataType::Varchar(_) | SQLDataType::Text => Ok(DataType::Utf8),
        SQLDataType::Decimal(precision, scale) => make_decimal_type(*precision, *scale),
        SQLDataType::Float(_) => Ok(DataType::Float32),
        SQLDataType::Real => Ok(DataType::Float32),
        SQLDataType::Double => Ok(DataType::Float64),
        SQLDataType::Boolean => Ok(DataType::Boolean),
        SQLDataType::Date => Ok(DataType::Date32),
        SQLDataType::Time => Ok(DataType::Time64(TimeUnit::Millisecond)),
        SQLDataType::Timestamp => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        _ => Err(DataFusionError::NotImplemented(format!(
            "The SQL data type {:?} is not implemented",
            sql_type
        ))),
    }
}

/// End copied functions

fn compound_identifier_to_column(ids: &[Ident]) -> Result<Column> {
    // OK, this one is partially taken from the planner for SQLExpr::CompoundIdentifier
    let mut var_names: Vec<_> = ids.iter().map(normalize_ident).collect();
    match (var_names.pop(), var_names.pop()) {
        (Some(name), Some(relation)) if var_names.is_empty() => Ok(Column {
            relation: Some(relation),
            name,
        }),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported compound identifier '{:?}'",
            var_names,
        ))),
    }
}

struct SeafowlContext {
    inner: SessionContext,
    catalog: Arc<dyn Catalog>,
    database: String,
}

impl SeafowlContext {
    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    pub async fn plan_query(&self, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
        let mut statements = DFParser::parse_sql(sql)?;

        if statements.len() != 1 {
            return Err(Error::NotImplemented(
                "The context currently only supports a single SQL statement".to_string(),
            ));
        }

        let state = self.inner.state.read().clone();
        let query_planner = SqlToRel::new(&state);

        let logical_plan = match statements.pop_front().unwrap() {
            datafusion::sql::parser::Statement::Statement(s) => match *s {
                // Delegate SELECT / EXPLAIN to the basic DataFusion logical planner
                // (though note EXPLAIN [our custom query] will mean we have to implement EXPLAIN ourselves)
                Statement::Explain { .. }
                | Statement::Query { .. }
                | Statement::ShowVariable { .. }
                | Statement::ShowColumns { .. }
                | Statement::CreateView { .. }
                | Statement::CreateSchema { .. }
                | Statement::CreateDatabase { .. }
                | Statement::Drop { .. } => query_planner.sql_statement_to_plan(*s),

                // CREATE TABLE (create empty table with columns)
                Statement::CreateTable {
                    query: None,
                    name,
                    columns,
                    constraints,
                    table_properties,
                    with_options,
                    if_not_exists,
                    or_replace: _,
                    ..
                } if constraints.is_empty()
                    && table_properties.is_empty()
                    && with_options.is_empty() =>
                {
                    let cols = build_schema(columns)?;
                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(CreateTable {
                            schema: cols.to_dfschema_ref()?,
                            name: name.to_string(),
                            if_not_exists,
                        }),
                    }))
                }

                // Other CREATE TABLE: SqlToRel only allows CreateTableAs statements and makes
                // a CreateMemoryTable node. We're fine with that, but we'll execute it differently.
                Statement::CreateTable { .. } => query_planner.sql_statement_to_plan(*s),

                // This DML is defined by us
                Statement::Insert {
                    table_name,
                    columns,
                    source,
                    ..
                } => {
                    let plan = query_planner.query_to_plan(*source, &mut HashMap::new())?;

                    let column_exprs = columns
                        .iter()
                        .map(|id| {
                            Column::from_name(normalize_ident(id))
                        })
                        .collect();

                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(Insert {
                            name: table_name.to_string(),
                            columns: column_exprs,
                            input: Arc::new(plan),
                        }),
                    }))
                    // Add a new partition to the table
                    // Index the partitions
                    // Create a new table version
                }
                Statement::Update {
                    table: TableWithJoins {relation: TableFactor::Table { name, alias: None, args: None, with_hints }, joins },
                    assignments,
                    from: None,
                    selection,
                }
                // We only support the most basic form of UPDATE (no aliases or FROM or joins)
                    if with_hints.is_empty() && joins.is_empty()
                => {
                    // Scan through the original table (with selection) and:
                    // SELECT [for each col, "col AS col" if not an assignment, otherwise "expr AS col"]
                    //   FROM original_table WHERE [selection]
                    // Somehow also split the result by existing partition boundaries and leave unchanged partitions alone

                    // TODO we need to load the table object here in order to validate the UPDATE clauses
                    let table_schema: DFSchema = DFSchema::empty();

                    let selection_expr = match selection {
                        None => None,
                        Some(expr) => Some(query_planner.sql_to_rex(expr, &table_schema, &mut HashMap::new())?),
                    };

                    let assignments_expr = assignments.iter().map(|a| {
                        Ok(Assignment { column: compound_identifier_to_column(&a.id)?, expr: query_planner.sql_to_rex(a.value.clone(), &table_schema, &mut HashMap::new())? })
                    }).collect::<Result<Vec<Assignment>>>()?;

                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(Update {
                            name: name.to_string(),
                            selection: selection_expr,
                            assignments: assignments_expr,
                        }),
                    }))
                }
                Statement::Delete {
                    table_name,
                    selection,
                } => {
                    // Same as Update but we just filter out the selection
                    let table_schema: DFSchema = DFSchema::empty();

                    let selection_expr = match selection {
                        None => None,
                        Some(expr) => Some(query_planner.sql_to_rex(expr, &table_schema, &mut HashMap::new())?),
                    };

                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(Delete {
                            name: table_name.to_string(),
                            selection: selection_expr,
                        }),
                    }))
                }
                _ => Err(Error::NotImplemented(format!(
                    "Unsupported SQL statement: {:?}",
                    sql
                ))),
            },
            datafusion::sql::parser::Statement::DescribeTable(s) => {
                query_planner.describe_table_to_plan(s)
            }
            // Stub out the standard DataFusion CREATE EXTERNAL TABLE statements since we don't support them
            datafusion::sql::parser::Statement::CreateExternalTable(_) => {
                return Err(Error::NotImplemented(format!(
                    "Unsupported SQL statement: {:?}",
                    sql
                )))
            }
        }?;

        // Similarly to DataFrame::sql, run certain logical plans outside of the actual execution flow
        // and produce a dummy physical plan instead
        match logical_plan {
            LogicalPlan::CreateExternalTable(_) => {
                // We're not supposed to reach this since we filtered it out above
                panic!("No plan for CreateExternalTable");
            }
            LogicalPlan::CreateCatalogSchema(_) => {
                // CREATE SCHEMA
                // Create a schema and register it
                Ok(Arc::new(EmptyExec::new(
                    false,
                    SchemaRef::new(Schema::empty()),
                )))
            }
            LogicalPlan::CreateCatalog(_) => {
                // CREATE DATABASE
                Ok(Arc::new(EmptyExec::new(
                    false,
                    SchemaRef::new(Schema::empty()),
                )))
            }
            LogicalPlan::CreateMemoryTable(_) => {
                // This is actually CREATE TABLE AS
                Ok(Arc::new(EmptyExec::new(
                    false,
                    SchemaRef::new(Schema::empty()),
                )))
            }
            LogicalPlan::DropTable(_) => {
                // DROP TABLE
                Ok(Arc::new(EmptyExec::new(
                    false,
                    SchemaRef::new(Schema::empty()),
                )))
            }
            LogicalPlan::CreateView(_) => {
                // CREATE VIEW
                Ok(Arc::new(EmptyExec::new(
                    false,
                    SchemaRef::new(Schema::empty()),
                )))
            }
            LogicalPlan::Extension(Extension { ref node }) => {
                // Other custom nodes we made like CREATE TABLE/INSERT/UPDATE/DELETE/ALTER
                let any = node.as_any();

                if let Some(CreateTable {
                    schema,
                    name,
                    if_not_exists,
                }) = any.downcast_ref::<CreateTable>()
                {
                    let table_ref = TableReference::from(name.as_str());
                    let (schema_name, table_name) = match table_ref {
                        TableReference::Bare { table: _ } => Err(Error::NotImplemented(
                            "Cannot CREATE TABLE without a schema qualifier!".to_string(),
                        )),
                        TableReference::Partial { schema, table } => Ok((schema, table)),
                        TableReference::Full {
                            catalog: _,
                            schema,
                            table,
                        } => Ok((schema, table)),
                    }?;

                    let sf_schema = SeafowlSchema {
                        arrow_schema: Arc::new(schema.as_ref().into()),
                    };

                    let collection_id = self
                        .catalog
                        .get_collection_id_by_name(&self.database, schema_name)
                        .await
                        .ok_or_else(|| {
                            Error::Plan(format!("Schema {:?} does not exist!", schema_name))
                        })?;

                    self.catalog
                        .create_table(collection_id, table_name, sf_schema)
                        .await;

                    Ok(Arc::new(EmptyExec::new(
                        false,
                        SchemaRef::new(Schema::empty()),
                    )))
                } else {
                    self.inner.create_physical_plan(&logical_plan).await
                }
            }
            _ => self.inner.create_physical_plan(&logical_plan).await,
        }
    }

    pub async fn create_physical_plan(&self, plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_plan = self.inner.create_physical_plan(plan).await?;
        Ok(physical_plan)
    }

    // Copied from DataFusion's physical_plan
    pub async fn collect(&self, physical_plan: Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>> {
        let stream = self.execute_stream(physical_plan).await?;
        stream.err_into().try_collect().await
    }

    pub async fn execute_stream(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        match physical_plan.output_partitioning().partition_count() {
            0 => Ok(Box::pin(EmptyRecordBatchStream::new(
                physical_plan.schema(),
            ))),
            1 => self.execute_stream_partitioned(&physical_plan, 0).await,
            _ => {
                let plan: Arc<dyn ExecutionPlan> =
                    Arc::new(CoalescePartitionsExec::new(physical_plan));
                self.execute_stream_partitioned(&plan, 0).await
            }
        }
    }

    pub async fn execute_stream_partitioned(
        &self,
        physical_plan: &Arc<dyn ExecutionPlan>,
        partition: usize,
    ) -> Result<SendableRecordBatchStream> {
        let task_context = Arc::new(TaskContext::from(self.inner()));
        physical_plan.execute(partition, task_context)
    }
}
