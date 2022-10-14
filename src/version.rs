use crate::catalog::TableCatalog;
use crate::data_types::{TableVersionId, Timestamp};
use chrono::DateTime;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::sql::TableReference;
use hashbrown::HashMap;
use itertools::Itertools;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, Ident, ObjectName, TableAlias, Value,
};
use std::sync::Arc;

use crate::datafusion::visit::{visit_table_table_factor, VisitorMut};

// A struct for walking the query AST, visiting all tables and rewriting any table reference that
// uses time travel syntax (i.e. table function syntax) from `table('2022-01-01 20:01:01Z')` into
// "table:2022-01-01 20:01:01Z".
pub struct TableVersionProcessor {
    pub default_catalog: String,
    pub default_schema: String,
    pub tables_visited: Vec<ObjectName>,
    pub tables_renamed: HashMap<(ObjectName, String), Option<TableVersionId>>,
}

impl TableVersionProcessor {
    pub fn new(default_catalog: String, default_schema: String) -> Self {
        Self {
            default_catalog,
            default_schema,
            tables_visited: vec![],
            tables_renamed: HashMap::<(ObjectName, String), Option<TableVersionId>>::new(
            ),
        }
    }

    pub fn table_with_version(name: &ObjectName, version: &String) -> String {
        format!("{}:{}", name.0.last().unwrap().value, version)
    }

    // // Try to parse the specified version timestamp into a Unix epoch
    pub fn version_to_epoch(version: &String) -> Result<Timestamp> {
        // TODO: Extend the supported formats for specifying the datetime
        let dt =
            DateTime::parse_from_str(version, "%Y-%m-%d %H:%M:%S %z").map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to parse version {} as timestamp: {:?}",
                    version, e
                ))
            })?;

        Ok(dt.timestamp() as Timestamp)
    }

    fn get_version_id(
        version: &String,
        table_versions: &[(TableVersionId, Timestamp)],
    ) -> Result<TableVersionId> {
        if version == "oldest" {
            // Fetch the first recorded table version id.
            return Ok(table_versions[0].0);
        }

        let timestamp = TableVersionProcessor::version_to_epoch(version)?;
        match table_versions.binary_search_by_key(&timestamp, |&(_, t)| t) {
            Ok(n) => Ok(table_versions[n].0),
            Err(n) => {
                // We haven't found an exact match, which is expected. Instead we have the index at
                // which the provided timestamp would fit in the versions vector.
                if n >= 1 {
                    // We're guaranteed to have at least 1 table version prior to the timestamp specified.
                    // Return that version.
                    return Ok(table_versions[n - 1].0);
                }

                // The timestamp specified occurs prior to earliest available table version.
                Err(DataFusionError::Execution(format!(
                    "No recorded table versions for the provided timestamp {}",
                    version
                )))
            }
        }
    }

    pub async fn load_versions(
        &mut self,
        table_catalog: Arc<dyn TableCatalog>,
    ) -> Result<Vec<(TableReference, Arc<dyn TableProvider>)>> {
        // Get a unique list of table names that have versions specified, as some may have
        // more than one
        let versioned_tables = self
            .tables_renamed
            .iter()
            .map(|((t, _), _)| t.0.last().unwrap().value.clone())
            .unique()
            .collect();

        // Fetch all available versions for the versioned tables from the metadata store, and
        // collect them into a table: [..., (vi, ti), ...] map (vi being the table version id
        // and ti the Unix epoch when that version was created for the i-th version).
        let table_versions: HashMap<ObjectName, Vec<(TableVersionId, Timestamp)>> =
            table_catalog
                .get_all_table_versions(versioned_tables)
                .await?
                .into_iter()
                .group_by(|tv| {
                    ObjectName(vec![
                        Ident::new(&tv.database_name),
                        Ident::new(&tv.collection_name),
                        Ident::new(&tv.table_name),
                    ])
                })
                .into_iter()
                .map(|(t, tvs)| {
                    (
                        t,
                        tvs.sorted_by_key(|tv| tv.creation_time)
                            .map(|tv| (tv.table_version_id, tv.creation_time))
                            .collect(),
                    )
                })
                .collect();

        // Update the map with renamed tables with exact the corresponding table_version_ids which should
        // be loaded for the specified versions
        for (table_version, table_version_id) in self.tables_renamed.iter_mut() {
            let table = &table_version.0;
            let version = &table_version.1;
            let id = TableVersionProcessor::get_version_id(
                version,
                table_versions.get(table).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "No versions found for table {}",
                        table
                    ))
                })?,
            )?;

            *table_version_id = Some(id);
        }

        Ok(vec![])
    }
}

impl<'ast> VisitorMut<'ast> for TableVersionProcessor {
    fn visit_table_table_factor(
        &mut self,
        name: &'ast mut ObjectName,
        alias: Option<&'ast mut TableAlias>,
        args: &'ast mut Option<Vec<FunctionArg>>,
        with_hints: &'ast mut [Expr],
    ) {
        self.tables_visited.push(name.clone());
        if let Some(func_args) = args {
            // TODO: Support named func args for more flexible syntax?
            // TODO: if func_args length is not exactly 1 error out
            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                Value::SingleQuotedString(value),
            ))) = &func_args[0]
            {
                let full_name = name.to_string();
                let resolved_ref = TableReference::from(full_name.as_str())
                    .resolve(&*self.default_catalog, &*self.default_schema);
                self.tables_renamed.insert(
                    (
                        ObjectName(vec![
                            Ident::new(resolved_ref.catalog),
                            Ident::new(resolved_ref.schema),
                            Ident::new(resolved_ref.table),
                        ]),
                        value.to_string(),
                    ),
                    None,
                );
                name.0.last_mut().unwrap().value =
                    TableVersionProcessor::table_with_version(name, value);
                *args = None;
            }
        }
        visit_table_table_factor(self, name, alias, args, with_hints)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::parser::Statement;
    use sqlparser::ast::Statement as SQLStatement;
    use std::ops::Deref;
    use test_case::test_case;

    use crate::datafusion::parser::DFParser;
    use crate::datafusion::visit::VisitorMut;
    use crate::version::TableVersionProcessor;

    #[test_case(
        "SELECT * FROM test_table";
        "Basic select with bare table name")
    ]
    #[test_case(
        "SELECT * FROM some_schema.test_table";
        "Basic select with schema and table name")
    ]
    #[test_case(
        "SELECT * FROM some_db.some_schema.test_table";
        "Basic select with fully qualified table name")
    ]
    fn test_table_version_rewrite(query: &str) {
        let version_timestamp = "2022-01-01 20:01:01Z";
        let stmts =
            DFParser::parse_sql(format!("{}('{}')", query, version_timestamp).as_str())
                .unwrap();

        let mut q = if let Statement::Statement(stmt) = &stmts[0] {
            if let SQLStatement::Query(query) = stmt.deref() {
                query.clone()
            } else {
                panic!("Expected Query not matched!");
            }
        } else {
            panic!("Expected Statement not matched!");
        };

        let mut rewriter = TableVersionProcessor::new(
            "test_catalog".to_string(),
            "test_schema".to_string(),
        );
        rewriter.visit_query(&mut q);

        // Ensure table name in the original query has been renamed to appropriate version
        assert_eq!(format!("{}", q), format!("{}:{}", query, version_timestamp))
    }
}
