use std::{any::Any, collections::HashMap, sync::Arc};

use arrow_schema::SchemaRef;
use async_trait::async_trait;

use dashmap::DashMap;
use datafusion::execution::context::{ExecutionProps, SessionState};
use datafusion::physical_expr::expressions::{case, cast, col};
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::{
    arrow::datatypes::Schema as ArrowSchema,
    catalog::{
        schema::{MemorySchemaProvider, SchemaProvider},
        CatalogProvider,
    },
    common::{DataFusionError, Result},
    datasource::TableProvider,
};
use datafusion_common::{DFSchema, Statistics};
use datafusion_expr::{expr::Alias, Expr};
use datafusion_expr::{LogicalPlan, TableProviderFilterPushDown, TableType};
use deltalake::DeltaTable;

use tracing::warn;

use crate::repository::interface::FunctionId;
use crate::system_tables::{SystemSchemaProvider, SYSTEM_SCHEMA};
use crate::{catalog::STAGING_SCHEMA, wasm_udf::data_types::CreateFunctionDetails};

pub struct SeafowlDatabase {
    pub name: Arc<str>,
    pub schemas: HashMap<Arc<str>, Arc<SeafowlSchema>>,
    pub staging_schema: Arc<MemorySchemaProvider>,
    pub system_schema: Arc<SystemSchemaProvider>,
}

impl CatalogProvider for SeafowlDatabase {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas
            .keys()
            .map(|s| s.to_string())
            .chain([STAGING_SCHEMA.to_string(), SYSTEM_SCHEMA.to_string()])
            .collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == STAGING_SCHEMA {
            Some(self.staging_schema.clone())
        } else if name == SYSTEM_SCHEMA {
            Some(self.system_schema.clone())
        } else {
            self.schemas.get(name).map(|c| Arc::clone(c) as _)
        }
    }
}

pub struct SeafowlSchema {
    pub name: Arc<str>,
    pub tables: DashMap<Arc<str>, Arc<dyn TableProvider>>,
}

#[async_trait]
impl SchemaProvider for SeafowlSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables
            .iter()
            .map(|s| s.key().to_string())
            .collect::<Vec<_>>()
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        // TODO: This is kind of meh: rebuilding the table from scratch and over-writing the existing entry, instead of just `load`-ing
        // the existing one (which we can't because it's behind of an Arc, and `load` needs `mut`).
        // We may be able get away with it by:
        //    1. removing the `Arc` from the value in the map
        //    2. enclosing the entire map inside of an `Arc`
        //    3. using `entry` for in-place mutation
        // Ultimately though, since the map gets re-created for each query the only point in
        // updating the existing table is to optimize potential multi-lookups during processing of
        // a single query.
        let mut delta_table = match self.tables.get(name) {
            None => return None,
            Some(table) => match table.as_any().downcast_ref::<DeltaTable>() {
                // This shouldn't happen since we store only DeltaTable's in the map
                None => return Some(table.clone()),
                Some(delta_table) => {
                    if delta_table.version() != -1 {
                        // Table was already loaded.
                        return Some(table.clone());
                    } else {
                        // A negative table version indicates that the table was never loaded; we need
                        // to do it before returning it.
                        delta_table.clone()
                    }
                }
            },
        };

        if let Err(err) = delta_table.load().await {
            warn!("Failed to load table {name}: {err}");
            return None;
        }

        let table = Arc::from(delta_table) as Arc<dyn TableProvider>;
        self.tables.insert(Arc::from(name), table.clone());
        Some(table)
    }

    // Used for registering versioned tables via `SessionContext::register_table`.
    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        if self.table_exist(name.as_str()) {
            return Err(DataFusionError::Execution(format!(
                "The table {name} already exists"
            )));
        }

        Ok(self.tables.insert(Arc::from(name), table))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

pub struct InstrumentingDeltaTable {
    inner: DeltaTable,
}

#[async_trait]
impl TableProvider for InstrumentingDeltaTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        TableProvider::schema(&self.inner)
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        self.inner.get_logical_plan()
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // i think we need to make a brand new DeltaTable here because we
        // don't have access to the DeltaScanBuilder and can't inject a different LogStore
        // with a separate metrics collector

        // but in that case, schema and statistics already expect the table to be loaded
        // and we don't want to load it all the time

        // so uuh we have a get_loaded_table in this struct that loads it once
        // and another one that has to load it every scan because we can't get at the
        // DeltaScanBuilder otherwise

        // here's another reason why this won't work: the object stores are registered
        // inside of DF's global object store registry and DeltaTable just makes a ParquetScan
        // that references that object store "hostname", i.e. "delta-rs://{}-{}{}", with the
        // full path to the table root - all our attempts to inject a custom object store for each
        // instance of a scan don't actually do anything (it's hashed to the same table name)

        // so uu FUCK

        self.inner.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }
}

// Create a complete projection expression for all columns by enveloping CAST (for fixing mistypes)
// with a CASE expression to scope down the rows to which the assignment is applied
pub fn project_expressions(
    exprs: &[Expr],
    df_schema: &DFSchema,
    schema: &ArrowSchema,
    selection_expr: Option<Arc<dyn PhysicalExpr>>,
) -> Result<Vec<(Arc<dyn PhysicalExpr>, String)>> {
    exprs
        .iter()
        .zip(schema.fields())
        .map(|(expr, f)| {
            // De-alias the expression
            let expr = if let Expr::Alias(Alias { expr, .. }) = expr {
                expr
            } else {
                expr
            };

            let mut proj_expr =
                create_physical_expr(expr, df_schema, &ExecutionProps::new())?;

            let data_type = f.data_type().clone();
            if proj_expr.data_type(schema)? != data_type {
                // Literal value potentially mistyped; try to re-cast it
                proj_expr = cast(proj_expr, schema, data_type)?;
            }

            // If the selection was specified, use a CASE WHEN
            // (selection expr) THEN (assignment expr) ELSE (old column value)
            // approach
            if let Some(sel_expr) = &selection_expr {
                proj_expr = case(
                    None,
                    vec![(sel_expr.clone(), proj_expr)],
                    Some(col(f.name(), schema)?),
                )?;
            }

            Ok((proj_expr, f.name().to_string()))
        })
        .collect()
}

#[derive(Debug)]
pub struct SeafowlFunction {
    pub function_id: FunctionId,
    pub name: String,
    pub details: CreateFunctionDetails,
}
