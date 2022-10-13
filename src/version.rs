use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, ObjectName, TableAlias, Value};
use std::collections::HashSet;

use crate::datafusion::visit::{visit_table_table_factor, VisitorMut};

// A struct for walking the query AST, visiting all tables and rewriting any table reference that
// uses time travel syntax (i.e. table function syntax) from `table('2022-01-01 20:01:01Z')` into
// "table:2022-01-01 20:01:01Z".
pub struct TableVersionProcessor {
    pub tables_visited: Vec<ObjectName>,
    pub tables_renamed: HashSet<(ObjectName, String)>,
}

impl Default for TableVersionProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl TableVersionProcessor {
    pub fn new() -> Self {
        Self {
            tables_visited: vec![],
            tables_renamed: HashSet::<(ObjectName, String)>::new(),
        }
    }

    pub fn table_with_version(name: &ObjectName, version: &String) -> String {
        format!("{}:{}", name.0.last().unwrap().value, version)
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
                self.tables_renamed
                    .insert((name.clone(), value.to_string()));
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

        let mut rewriter = TableVersionProcessor::new();
        rewriter.visit_query(&mut q);

        // Ensure table name in the original query has been renamed to appropriate version
        assert_eq!(format!("{}", q), format!("{}:{}", query, version_timestamp))
    }
}
