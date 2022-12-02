use arrow::temporal_conversions::{date32_to_datetime, timestamp_ns_to_datetime};
use datafusion::common::{Column, DataFusionError};
use datafusion::error::Result;
use datafusion::scalar::ScalarValue;
use datafusion_expr::expr_visitor::{ExprVisitable, ExpressionVisitor, Recursion};
use datafusion_expr::{BinaryExpr, Expr, Operator};

pub struct FilterPushdown<T: FilterPushdownVisitor> {
    pub source: T,
    // LIFO stack for keeping the intermediate SQL expression results to be used in interpolation
    // of the parent nodes. After a successful visit, it should contain exactly one element, which
    // represents the complete SQL statement corresponding to the given expression.
    pub sql_exprs: Vec<String>,
}

impl<T: FilterPushdownVisitor> FilterPushdown<T> {
    // Intended to be used in the node post-visit phase, ensuring that SQL representation of inner
    // nodes is on the stack.
    fn pop_sql_expr(&mut self) -> String {
        self.sql_exprs
            .pop()
            .unwrap_or_else(|| panic!("No SQL expression in the stack"))
    }
}

pub struct PostgresFilterPushdown {}
pub struct SQLiteFilterPushdown {}
pub struct MySQLFilterPushdown {}

impl FilterPushdownVisitor for PostgresFilterPushdown {}

impl FilterPushdownVisitor for SQLiteFilterPushdown {
    fn op_to_sql(&self, op: &Operator) -> Option<String> {
        match op {
            Operator::RegexMatch
            | Operator::RegexIMatch
            | Operator::RegexNotMatch
            | Operator::RegexNotIMatch
            | Operator::BitwiseXor => None,
            _ => Some(op.to_string()),
        }
    }
}

impl FilterPushdownVisitor for MySQLFilterPushdown {
    fn col_to_sql(&self, col: &Column) -> String {
        quote_identifier_backticks(&col.name)
    }

    fn op_to_sql(&self, op: &Operator) -> Option<String> {
        match op {
            // TODO: see if there's a way to convert the non-case sensitive match
            Operator::RegexIMatch | Operator::RegexNotIMatch => None,
            Operator::RegexMatch => Some("RLIKE".to_string()),
            Operator::RegexNotMatch => Some("NOT RLIKE".to_string()),
            Operator::BitwiseXor => Some("^".to_string()),
            _ => Some(op.to_string()),
        }
    }
}

pub trait FilterPushdownVisitor {
    fn col_to_sql(&self, col: &Column) -> String {
        quote_identifier_double_quotes(&col.name)
    }

    fn scalar_value_to_sql(&self, value: &ScalarValue) -> Option<String> {
        match value {
            ScalarValue::Utf8(Some(val)) | ScalarValue::LargeUtf8(Some(val)) => {
                Some(format!("'{}'", val.replace('\'', "''")))
            }
            ScalarValue::Date32(Some(days)) => {
                let date = date32_to_datetime(*days)?.date();
                Some(format!("'{}'", date.format("%Y-%m-%d")))
            }
            ScalarValue::TimestampNanosecond(Some(ns), None) => {
                let timestamp = timestamp_ns_to_datetime(*ns)?;
                Some(format!("'{}'", timestamp))
            }
            // TODO: See which of these makes sense to implement
            ScalarValue::Date64(_)
            | ScalarValue::TimestampSecond(_, _)
            | ScalarValue::TimestampMillisecond(_, _)
            | ScalarValue::TimestampMicrosecond(_, _) => None,
            _ => Some(format!("{}", value)),
        }
    }

    fn op_to_sql(&self, op: &Operator) -> Option<String> {
        Some(op.to_string())
    }
}

impl<T: FilterPushdownVisitor> ExpressionVisitor for FilterPushdown<T> {
    fn pre_visit(self, expr: &Expr) -> Result<Recursion<Self>> {
        match expr {
            Expr::Column(_)
            | Expr::Literal(_)
            | Expr::Not(_)
            | Expr::Negative(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_) => {}
            Expr::BinaryExpr(BinaryExpr { op, .. }) => {
                // Check if operator pushdown supported; left and right expressions will be checked
                // through further recursion.
                if self.source.op_to_sql(op).is_none() {
                    return Err(DataFusionError::Execution(format!(
                        "Operator {} not shippable",
                        op,
                    )));
                }
            }
            _ => {
                // Expression is not supported, no need to visit any remaining child or parent nodes
                return Err(DataFusionError::Execution(format!(
                    "Expression {:?} not shippable",
                    expr,
                )));
            }
        };
        Ok(Recursion::Continue(self))
    }

    fn post_visit(mut self, expr: &Expr) -> Result<Self> {
        match expr {
            // Column and Literal are the only two leaf nodes atm - they don't depend on any SQL
            // expression being on the stack.
            Expr::Column(col) => self.sql_exprs.push(self.source.col_to_sql(col)),
            Expr::Literal(val) => {
                let sql_val = self.source.scalar_value_to_sql(val).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "ScalarValue {:?} not shippable",
                        val,
                    ))
                })?;
                self.sql_exprs.push(sql_val)
            }
            Expr::BinaryExpr(be @ BinaryExpr { .. }) => {
                // The visitor has been through left and right sides in that order, so the topmost
                // item on the SQL expression stack is the right expression
                let mut right_sql = self.pop_sql_expr();
                let mut left_sql = self.pop_sql_expr();

                // Similar as in Display impl for BinaryExpr: since the Expr has an implicit operator
                // precedence we need to convert it to an explicit one using extra parenthesis if the
                // left/right expression is also a BinaryExpr of lower operator precedence.
                if let Expr::BinaryExpr(right_be @ BinaryExpr { .. }) = &*be.right {
                    let p = right_be.precedence();
                    if p == 0 || p < be.precedence() {
                        right_sql = format!("({})", right_sql)
                    }
                }
                if let Expr::BinaryExpr(left_be @ BinaryExpr { .. }) = &*be.left {
                    let p = left_be.precedence();
                    if p == 0 || p < be.precedence() {
                        left_sql = format!("({})", left_sql)
                    }
                }

                let op_sql = self.source.op_to_sql(&be.op).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Couldn't convert operator {:?} to a compatible one for the remote system",
                        be.op,
                    ))
                })?;

                self.sql_exprs
                    .push(format!("{} {} {}", left_sql, op_sql, right_sql))
            }
            Expr::Not(_) => {
                let inner_sql = self.pop_sql_expr();
                self.sql_exprs.push(format!("NOT {inner_sql}"));
            }
            Expr::Negative(_) => {
                let inner_sql = self.pop_sql_expr();
                self.sql_exprs.push(format!("(- {inner_sql})"));
            }
            Expr::IsNull(_) => {
                let inner_sql = self.pop_sql_expr();
                self.sql_exprs.push(format!("{inner_sql} IS NULL"));
            }
            Expr::IsNotNull(_) => {
                let inner_sql = self.pop_sql_expr();
                self.sql_exprs.push(format!("{inner_sql} IS NOT NULL"));
            }
            Expr::IsTrue(_) => {
                let inner_sql = self.pop_sql_expr();
                self.sql_exprs.push(format!("{inner_sql} IS TRUE"));
            }
            Expr::IsFalse(_) => {
                let inner_sql = self.pop_sql_expr();
                self.sql_exprs.push(format!("{inner_sql} IS FALSE"));
            }
            Expr::IsNotTrue(_) => {
                let inner_sql = self.pop_sql_expr();
                self.sql_exprs.push(format!("{inner_sql} IS NOT TRUE"));
            }
            Expr::IsNotFalse(_) => {
                let inner_sql = self.pop_sql_expr();
                self.sql_exprs.push(format!("{inner_sql} IS NOT FALSE"));
            }
            _ => {}
        };
        Ok(self)
    }
}

pub fn quote_identifier_double_quotes(name: &str) -> String {
    format!("\"{}\"", name.replace('\"', "\"\""))
}

pub fn quote_identifier_backticks(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

// Walk the filter expression AST for a particular remote source type and see if the expression is
// ship-able, at the same time converting elements (e.g. operators) to the native representation if
// needed.
pub fn filter_expr_to_sql<T: FilterPushdownVisitor>(
    filter: &Expr,
    source_pushdown: T,
) -> Result<String> {
    // Construct the initial visitor state
    let visitor = FilterPushdown {
        source: source_pushdown,
        sql_exprs: vec![],
    };

    // Perform the walk through the expr AST trying to construct the equivalent SQL for the
    // particular source type at hand.
    let FilterPushdown { sql_exprs, .. } = filter.accept(visitor)?;

    if sql_exprs.len() != 1 {
        return Err(DataFusionError::Execution(format!(
            "Expected exactly one SQL expression for filter {}, found: {:?}",
            filter, sql_exprs,
        )));
    }

    Ok(sql_exprs
        .first()
        .expect("Exactly 1 SQL expression expected")
        .clone())
}

#[cfg(test)]
mod tests {
    use datafusion::error::Result;
    use datafusion::logical_expr::{and, col, lit, or, Expr};
    use datafusion::scalar::ScalarValue;
    use rstest::rstest;

    use crate::remote_tables::pushdown_visitor::{
        filter_expr_to_sql, MySQLFilterPushdown, PostgresFilterPushdown,
        SQLiteFilterPushdown,
    };

    fn get_result_for_source_type(expr: &Expr, source_type: &str) -> Result<String> {
        if source_type == "postgres" {
            filter_expr_to_sql(expr, PostgresFilterPushdown {})
        } else if source_type == "sqlite" {
            filter_expr_to_sql(expr, SQLiteFilterPushdown {})
        } else {
            filter_expr_to_sql(expr, MySQLFilterPushdown {})
        }
    }

    #[rstest]
    #[case::simple_binary_expression(
        col("a").gt_eq(lit(25)),
        r#""a" >= 25"#)
    ]
    #[case::complex_binary_expression(
        or(and(or(col("a").eq(lit(1)), col("b").gt(lit(10))), col("c").lt_eq(lit(15))), col("d").not_eq(lit("some_string"))),
        r#"("a" = 1 OR "b" > 10) AND "c" <= 15 OR "d" != 'some_string'"#)
    ]
    #[case::simple_not(Expr::Not(Box::new(col("a"))), r#"NOT "a""#)]
    #[case::simple_negative(
        Expr::Negative(Box::new(col("a"))).lt(lit(0)),
        r#"(- "a") < 0"#)
    ]
    #[case::simple_is_null(
        col("a").is_null(),
        r#""a" IS NULL"#)
    ]
    #[case::simple_is_not_null(
        col("a").is_not_null(),
        r#""a" IS NOT NULL"#)
    ]
    #[case::simple_is_true(
        col("a").is_true(),
        r#""a" IS TRUE"#)
    ]
    #[case::simple_is_false(
        col("a").is_false(),
        r#""a" IS FALSE"#)
    ]
    #[case::simple_is_not_true(
        col("a").is_not_true(),
        r#""a" IS NOT TRUE"#)
    ]
    fn test_filter_expr_to_sql(
        #[case] expr: Expr,
        #[case] expr_sql: &str,
        #[values("postgres", "sqlite", "mysql")] source_type: &str,
    ) {
        let result_sql = get_result_for_source_type(&expr, source_type).unwrap();

        let expected_sql = if source_type == "mysql" {
            expr_sql.replace('"', "`")
        } else {
            expr_sql.to_string()
        };

        assert_eq!(result_sql, expected_sql)
    }

    #[rstest]
    fn test_filter_expr_to_sql_special_column_names(
        #[values("postgres", "sqlite", "mysql")] source_type: &str,
    ) {
        let expr = if source_type == "mysql" {
            col("a quoted `column name` with spaces").lt(lit(42))
        } else {
            col(r#"a quoted "column name" with spaces"#).lt(lit(42))
        };

        let result_sql = get_result_for_source_type(&expr, source_type).unwrap();

        if source_type == "mysql" {
            assert_eq!(result_sql, "`a quoted ``column name`` with spaces` < 42")
        } else {
            assert_eq!(result_sql, r#""a quoted ""column name"" with spaces" < 42"#);
        };
    }

    #[rstest]
    #[should_panic(expected = r#"ScalarValue Date64(\"1000\") not shippable"#)]
    #[case(col("a").gt(lit(ScalarValue::Date64(Some(1000)))))]
    #[should_panic(expected = "ScalarValue TimestampSecond(1000, None) not shippable")]
    #[case(col("a").gt(lit(ScalarValue::TimestampSecond(Some(1000), None))))]
    #[should_panic(
        expected = "ScalarValue TimestampMillisecond(1000, None) not shippable"
    )]
    #[case(col("a").gt(lit(ScalarValue::TimestampMillisecond(Some(1000), None))))]
    #[should_panic(
        expected = "ScalarValue TimestampMicrosecond(1000, None) not shippable"
    )]
    #[case(col("a").gt(lit(ScalarValue::TimestampMicrosecond(Some(1000), None))))]
    fn test_filter_expr_to_sql_unsupported_datetime_formats(
        #[case] expr: Expr,
        #[values("postgres", "sqlite", "mysql")] source_type: &str,
    ) {
        get_result_for_source_type(&expr, source_type).unwrap();
    }
}
