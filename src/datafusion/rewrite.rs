use sqlparser::ast::{Expr, FunctionArg, ObjectName, TableAlias};

use crate::datafusion::visit::{visit_table_table_factor, VisitorMut};

pub struct TableRenameVisitor<F>
where
    F: FnMut(&mut ObjectName),
{
    pub rename_fn: F,
}

impl<'ast, F> VisitorMut<'ast> for TableRenameVisitor<F>
where
    F: FnMut(&mut ObjectName),
{
    fn visit_table_table_factor(
        &mut self,
        name: &'ast mut ObjectName,
        alias: Option<&'ast mut TableAlias>,
        args: &'ast mut Option<Vec<FunctionArg>>,
        with_hints: &'ast mut [Expr],
    ) {
        (self.rename_fn)(name);
        visit_table_table_factor(self, name, alias, args, with_hints)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::parser::Statement;
    use itertools::Itertools;
    use sqlparser::ast::{ObjectName, SetExpr, Statement as SQLStatement, TableFactor};
    use std::ops::Deref;
    use test_case::test_case;

    use crate::datafusion::parser::DFParser;
    use crate::datafusion::rewrite::TableRenameVisitor;
    use crate::datafusion::visit::VisitorMut;

    #[test_case(
        "test_table";
        "Bare table name")
    ]
    #[test_case(
        "some_schema.test_table";
        "Schema + table name")
    ]
    #[test_case(
        "some_db.some_schema.test_table";
        "Fully qualified table name")
    ]
    fn test_table_name_rewrite(table_name: &str) {
        let stmts = DFParser::parse_sql(format!("SELECT * FROM {}", table_name).as_str())
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

        let rename_table_to_aaaa = |name: &mut ObjectName| {
            let table_ind = name.0.len() - 1;
            name.0[table_ind].value = "aaaa".to_string()
        };

        let mut rewriter = TableRenameVisitor {
            rename_fn: rename_table_to_aaaa,
        };
        rewriter.visit_query(&mut q);

        if let SetExpr::Select(select) = *q.body {
            if let TableFactor::Table { name, .. } = &select.from[0].relation {
                assert_eq!(
                    name.0.iter().map(|i| i.value.clone()).join("."),
                    table_name.replace("test_table", "aaaa"),
                )
            }
        } else {
            panic!("Expected SetExpr not matched!");
        }
    }
}
