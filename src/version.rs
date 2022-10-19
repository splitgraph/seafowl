use crate::catalog::TableCatalog;
use crate::data_types::{TableVersionId, Timestamp};
use chrono::DateTime;
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
// uses time travel syntax (i.e. table function syntax). It does so in two runs, first collecting
// all table references such as `table('2022-01-01 20:01:01Z')`, and then (after collecting the
// corresponding version ids via `triage_version_ids`) renaming the table to "table:<table_version_id>"
// (and also removing the table func expressions from the statement).
// The reason we do this in 2 runs is so that we that we can parse and triage the version specifier
// into a corresponding table_version_id, with which we will rename the table. By doing so, we make
// sure that no redundant entries to schema provider's table map will be made, given that many different
// version specifiers can point to the same table_version_id.
pub struct TableVersionProcessor {
    pub default_catalog: String,
    pub default_schema: String,
    pub table_versions: HashMap<(ObjectName, String), Option<TableVersionId>>,
    rewrite_ready: bool,
}

impl TableVersionProcessor {
    pub fn new(default_catalog: String, default_schema: String) -> Self {
        Self {
            default_catalog,
            default_schema,
            table_versions: HashMap::<(ObjectName, String), Option<TableVersionId>>::new(
            ),
            rewrite_ready: false,
        }
    }

    pub fn table_with_version(&self, name: &ObjectName, version: &str) -> String {
        let version_id = self.table_versions[&(name.clone(), version.to_owned())];
        format!("{}:{}", name.0.last().unwrap().value, version_id.unwrap())
    }

    // Try to parse the specified version timestamp into a Unix epoch
    pub fn version_to_epoch(version: &String) -> Result<Timestamp> {
        // TODO: Extend the supported formats for specifying the datetime
        let dt = if let Ok(dt_rfc3339) = DateTime::parse_from_rfc3339(version) {
            dt_rfc3339
        } else if let Ok(dt_str_1) =
            DateTime::parse_from_str(version, "%Y-%m-%d %H:%M:%S %z")
        {
            dt_str_1
        } else if let Ok(dt_rfc2822) = DateTime::parse_from_rfc2822(version) {
            dt_rfc2822
        } else {
            return Err(DataFusionError::Execution(format!(
                "Failed to parse version {} as timestamp",
                version
            )));
        };

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

    pub async fn triage_version_ids(
        &mut self,
        table_catalog: Arc<dyn TableCatalog>,
    ) -> Result<()> {
        // Fetch all available versions for the versioned tables from the metadata store, and
        // collect them into a table: [..., (vi, ti), ...] map (vi being the table version id
        // and ti the Unix epoch when that version was created for the i-th version).
        let table_versions: HashMap<ObjectName, Vec<(TableVersionId, Timestamp)>> =
            table_catalog
                .get_all_table_versions(self.get_versioned_tables())
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

        // Update the map of the renamed tables with the corresponding table_version_id which should
        // be loaded for the specified versions
        for (table_version, table_version_id) in self.table_versions.iter_mut() {
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

        self.rewrite_ready = true;

        Ok(())
    }

    // Get a unique list of table names that have versions specified, as some may have
    // more than one.
    fn get_versioned_tables(&self) -> Vec<String> {
        self.table_versions
            .iter()
            .map(|((t, _), _)| t.0.last().unwrap().value.clone())
            .unique()
            .collect()
    }

    // Get a unique list of table_version_ids (if collected), as some version references may point to
    // more than one.
    pub fn table_version_ids(&self) -> Vec<TableVersionId> {
        self.table_versions
            .values()
            .filter_map(|&table_version_id| table_version_id)
            .collect()
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
        if let Some(func_args) = args {
            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                Value::SingleQuotedString(value),
            ))) = &func_args[0]
            {
                let unresolved_name = name.to_string();
                let resolved_ref = TableReference::from(unresolved_name.as_str())
                    .resolve(&*self.default_catalog, &*self.default_schema);
                let full_object_name = ObjectName(vec![
                    Ident::new(resolved_ref.catalog),
                    Ident::new(resolved_ref.schema),
                    Ident::new(resolved_ref.table),
                ]);
                if !self.rewrite_ready {
                    // We haven't yet fetched/triaged the table versions ids; for now just collect
                    // all table raw versions specified.

                    self.table_versions
                        .insert((full_object_name, value.to_string()), None);
                } else {
                    // Do the actual name rewrite
                    name.0.last_mut().unwrap().value =
                        self.table_with_version(&full_object_name, value);
                    // Void the function table arg struct to leave a clean printable statement
                    *args = None;
                }
            }
        }
        visit_table_table_factor(self, name, alias, args, with_hints)
    }
}

#[cfg(test)]
mod tests {
    use crate::data_types::TableVersionId;
    use datafusion::sql::parser::Statement;
    use sqlparser::ast::Statement as SQLStatement;
    use std::ops::Deref;
    use test_case::test_case;

    use crate::datafusion::parser::DFParser;
    use crate::datafusion::visit::VisitorMut;
    use crate::version::TableVersionProcessor;

    #[test_case(
        "SELECT * FROM test_table('test_version')";
        "Basic select with bare table name")
    ]
    #[test_case(
        "SELECT * FROM some_schema.test_table('test_version')";
        "Basic select with schema and table name")
    ]
    #[test_case(
        "SELECT * FROM some_db.some_schema.test_table('test_version')";
        "Basic select with fully qualified table name")
    ]
    #[test_case(
        "WITH some_cte AS (SELECT 1 AS k) SELECT t.*, k.* FROM some_schema.test_table('test_version') AS t JOIN some_cte AS c ON t.k = c.k";
        "CTE without a version reference")
    ]
    #[test_case(
        "WITH some_cte AS (SELECT * FROM other_table('test_version') AS k) SELECT t.*, k.* FROM some_schema.test_table('test_version') AS t JOIN some_cte AS c ON t.k = c.k";
        "CTE with a version reference")
    ]
    fn test_table_version_rewrite(query: &str) {
        let stmts = DFParser::parse_sql(query).unwrap();

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
        // Do a first pass
        rewriter.visit_query(&mut q);

        // Set a mock table version id and do another pass for actually renaming the table
        let id = 42 as TableVersionId;
        for table_version_id in rewriter.table_versions.values_mut() {
            *table_version_id = Some(id);
        }
        rewriter.rewrite_ready = true;
        rewriter.visit_query(&mut q);

        // Ensure table name in the original query has been renamed to appropriate version
        assert_eq!(
            format!("{}", q),
            query.replace("('test_version')", format!(":{}", id).as_str())
        )
    }
}
