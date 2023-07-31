use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;

use datafusion::execution::context::ExecutionProps;
use datafusion::physical_expr::expressions::{case, cast, col};
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion::{
    arrow::datatypes::Schema as ArrowSchema,
    catalog::{
        schema::{MemorySchemaProvider, SchemaProvider},
        CatalogProvider,
    },
    common::{DataFusionError, Result},
    datasource::TableProvider,
};
use datafusion_common::DFSchema;
use datafusion_expr::{expr::Alias, Expr};
use deltalake::DeltaTable;

use log::warn;
use parking_lot::RwLock;

use crate::data_types::FunctionId;
use crate::system_tables::{SystemSchemaProvider, SYSTEM_SCHEMA};
use crate::{catalog::STAGING_SCHEMA, wasm_udf::data_types::CreateFunctionDetails};

pub struct SeafowlDatabase {
    pub name: Arc<str>,
    pub collections: HashMap<Arc<str>, Arc<SeafowlCollection>>,
    pub staging_schema: Arc<MemorySchemaProvider>,
    pub system_schema: Arc<SystemSchemaProvider>,
}

impl CatalogProvider for SeafowlDatabase {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.collections
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
            self.collections.get(name).map(|c| Arc::clone(c) as _)
        }
    }
}

pub struct SeafowlCollection {
    pub name: Arc<str>,
    // TODO: consider using DashMap instead of RwLock<HashMap<_>>: https://github.com/xacrimon/conc-map-bench
    pub tables: RwLock<HashMap<Arc<str>, Arc<dyn TableProvider>>>,
}

#[async_trait]
impl SchemaProvider for SeafowlCollection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables
            .read()
            .keys()
            .map(|s| s.to_string())
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
        let table_object_store = match self.tables.read().get(name) {
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
                        delta_table.object_store()
                    }
                }
            },
        };

        let mut delta_table = DeltaTable::new(table_object_store, Default::default());

        if let Err(err) = delta_table.load().await {
            warn!("Failed to load table {name}: {err}");
            return None;
        }

        let table = Arc::from(delta_table) as Arc<dyn TableProvider>;
        self.tables.write().insert(Arc::from(name), table.clone());
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

        Ok(self.tables.write().insert(Arc::from(name), table))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.read().contains_key(name)
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
                create_physical_expr(expr, df_schema, schema, &ExecutionProps::new())?;

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
