use datafusion::common::DataFusionError;
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
    fn scalar_value_to_sql(&self, value: &ScalarValue) -> Option<String> {
        match value {
            ScalarValue::Utf8(Some(val)) | ScalarValue::LargeUtf8(Some(val)) => {
                Some(format!("'{}'", val.replace('\'', "''")))
            }
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
            Expr::Column(_) | Expr::Literal(_) => {}
            Expr::BinaryExpr(BinaryExpr { op, .. }) => {
                // Check if operator pushdown supported; left and right expressions will be checked
                // through further recursion.
                if self.source.op_to_sql(op).is_none() {
                    return Err(DataFusionError::Execution(format!(
                        "Operator {} not shipable",
                        op,
                    )));
                }
            }
            _ => {
                // Expression is not supported, no need to visit any remaining child or parent nodes
                return Err(DataFusionError::Execution(format!(
                    "Expression {:?} not shipable",
                    expr,
                )));
            }
        };
        Ok(Recursion::Continue(self))
    }

    fn post_visit(mut self, expr: &Expr) -> Result<Self> {
        match expr {
            Expr::Column(col) => self.sql_exprs.push(col.name.clone()),
            Expr::Literal(val) => {
                let sql_val = self.source.scalar_value_to_sql(val).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Couldn't convert ScalarValue {:?} to a compatible one for the remote system",
                        val,
                    ))
                })?;
                self.sql_exprs.push(sql_val)
            }
            Expr::BinaryExpr(be @ BinaryExpr { .. }) => {
                // The visitor has been through left and right sides in that order, so the topmost
                // item on the SQL expression stack is the right expression
                let mut right_sql = self.sql_exprs.pop().unwrap_or_else(|| {
                    panic!("Missing right sub-expression of {}", expr)
                });
                let mut left_sql = self
                    .sql_exprs
                    .pop()
                    .unwrap_or_else(|| panic!("Missing left sub-expression of {}", expr));

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
            _ => {}
        };
        Ok(self)
    }
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
            "Failed constructing SQL for expression {}",
            filter
        )));
    }

    Ok(sql_exprs
        .first()
        .expect("Exactly 1 SQL expression expected")
        .clone())
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::{and, col, lit, or, Expr};
    use rstest::rstest;

    use crate::remote_tables::pushdown_visitor::{
        filter_expr_to_sql, MySQLFilterPushdown, PostgresFilterPushdown,
        SQLiteFilterPushdown,
    };

    #[rstest]
    #[case::simple_binary_expression(
        col("a").gt_eq(lit(25)),
        "a >= 25")
    ]
    #[case::complex_binary_expression(
        or(and(or(col("a").eq(lit(1)), col("b").gt(lit(10))), col("c").lt_eq(lit(15.0))), col("d").not_eq(lit("some_string"))),
        "(a = 1 OR b > 10) AND c <= 15 OR d != 'some_string'")
    ]
    fn test_filter_expr_to_sql(
        #[case] expr: Expr,
        #[case] expr_sql: &str,
        #[values("postgres", "sqlite", "mysql")] source_type: &str,
    ) {
        let sql_result = if source_type == "postgres" {
            filter_expr_to_sql(&expr, PostgresFilterPushdown {}).unwrap()
        } else if source_type == "sqlite" {
            filter_expr_to_sql(&expr, SQLiteFilterPushdown {}).unwrap()
        } else {
            filter_expr_to_sql(&expr, MySQLFilterPushdown {}).unwrap()
        };

        assert_eq!(sql_result, expr_sql)
    }
}
