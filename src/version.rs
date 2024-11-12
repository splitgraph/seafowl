use chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};
use datafusion::error::{DataFusionError, Result};
use datafusion::sql::TableReference;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, Ident, ObjectName, TableFactor,
    TableFunctionArgs, Value, VisitorMut,
};
use std::collections::HashSet;
use std::ops::ControlFlow;

// A struct for walking the query AST, visiting all tables and rewriting any table reference that
// uses time travel syntax (i.e. table function syntax such as `table('2022-01-01 20:01:01Z')`).
pub struct TableVersionProcessor {
    pub default_catalog: String,
    pub default_schema: String,
    pub table_versions: HashSet<(ObjectName, String)>,
}

impl TableVersionProcessor {
    pub fn new(default_catalog: String, default_schema: String) -> Self {
        Self {
            default_catalog,
            default_schema,
            table_versions: HashSet::<(ObjectName, String)>::new(),
        }
    }

    pub fn table_with_version(name: &ObjectName, version: &str) -> String {
        format!(
            "{}:{}",
            name.0.last().unwrap().value,
            version.to_ascii_lowercase()
        )
    }

    // Try to parse the specified version timestamp into a Unix epoch
    pub fn version_to_datetime(version: &str) -> Result<DateTime<Utc>> {
        let dt = if let Ok(dt_rfc3339) = DateTime::parse_from_rfc3339(version) {
            dt_rfc3339
        } else if let Ok(dt_rfc2822) = DateTime::parse_from_rfc2822(version) {
            dt_rfc2822
        } else if let Ok(dt) = DateTime::parse_from_str(version, "%Y-%m-%d %H:%M:%S %z") {
            dt
        } else if let Ok(dt_naive) =
            NaiveDateTime::parse_from_str(version, "%Y-%m-%d %H:%M:%S")
        {
            DateTime::from_naive_utc_and_offset(
                dt_naive,
                FixedOffset::east_opt(0).unwrap(),
            )
        } else {
            return Err(DataFusionError::Execution(format!(
                "Failed to parse version {version} as timestamp"
            )));
        };

        Ok(DateTime::<Utc>::from(dt))
    }
}

impl VisitorMut for TableVersionProcessor {
    type Break = ();

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<()> {
        if let TableFactor::Table {
            name, ref mut args, ..
        } = table_factor
        {
            if let Some(TableFunctionArgs {
                args: func_args, ..
            }) = args
            {
                // If a function arg expression is a single string interpret this as a version specifier
                if let [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    Value::SingleQuotedString(value),
                )))] = &func_args[..]
                {
                    let unresolved_name = name.to_string();
                    let resolved_ref = TableReference::from(unresolved_name.as_str())
                        .resolve(&self.default_catalog, &self.default_schema);
                    let full_object_name = ObjectName(vec![
                        Ident::new(resolved_ref.catalog.as_ref()),
                        Ident::new(resolved_ref.schema.as_ref()),
                        Ident::new(resolved_ref.table.as_ref()),
                    ]);

                    self.table_versions
                        .insert((full_object_name.clone(), value.clone()));
                    // Do the actual name rewrite
                    name.0.last_mut().unwrap().value =
                        TableVersionProcessor::table_with_version(
                            &full_object_name,
                            value,
                        );
                    // Void the function table arg struct to leave a clean printable statement
                    *args = None;
                }
            }
        }

        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::parser::Statement;
    use rstest::rstest;
    use sqlparser::ast::{Statement as SQLStatement, VisitMut};
    use std::ops::Deref;

    use crate::datafusion::parser::DFParser;
    use crate::version::TableVersionProcessor;

    #[rstest]
    #[case::basic_select_bare_table_name("SELECT * FROM test_table('test_version')")]
    #[case::basic_select_schema_and_table_name(
        "SELECT * FROM some_schema.test_table('test_version')"
    )]
    #[case::basic_select_fully_qualified_table_name(
        "SELECT * FROM some_db.some_schema.test_table('test_version')"
    )]
    #[case::cte_without_a_version_reference(
        "WITH some_cte AS (SELECT 1 AS k) \
        SELECT t.*, k.* FROM some_schema.test_table('test_version') AS t \
        JOIN some_cte AS c ON t.k = c.k"
    )]
    #[case::cte_with_a_version_reference(
        "WITH some_cte AS (SELECT * FROM other_table('test_version') AS k) \
        SELECT t.*, k.* FROM some_schema.test_table('test_version') AS t \
        JOIN some_cte AS c ON t.k = c.k"
    )]
    fn test_table_version_rewrite(#[case] query: &str) {
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
        q.visit(&mut rewriter);

        // Ensure table name in the original query has been renamed to appropriate version
        assert_eq!(
            format!("{q}"),
            query.replace("('test_version')", ":test_version")
        )
    }

    #[rstest]
    #[case::rfc_3339("2017-07-14T02:40:00+00:00")]
    #[case::rfc_3339_shifted("2017-07-14T04:40:00+02:00")]
    #[case::custom_with_tz("2017-07-14 02:40:00 +00:00")]
    #[case::custom_with_tz_shifted("2017-07-14 04:40:00 +02:00")]
    #[case::naive_datetime("2017-07-14 02:40:00")]
    #[case::rfc_2822("Fri, 14 Jul 2017 02:40:00 +0000")]
    #[case::rfc_2822_shifted("Fri, 14 Jul 2017 04:40:00 +0200")]
    fn test_version_timestamp_parsing(#[case] version: &str) {
        assert_eq!(
            TableVersionProcessor::version_to_datetime(version)
                .unwrap()
                .timestamp(),
            1_500_000_000,
        )
    }
}
