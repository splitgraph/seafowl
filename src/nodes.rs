use datafusion::common::DFSchemaRef;
use datafusion::error::DataFusionError;
use datafusion_expr::expr_rewriter::{ExprRewritable, ExprRewriter, RewriteRecursion};

use std::{any::Any, fmt, sync::Arc, vec};

use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNode};

use crate::data_types::TableId;
use crate::{provider::SeafowlTable, wasm_udf::data_types::CreateFunctionDetails};

#[derive(Debug, Clone)]
pub struct CreateTable {
    /// The table schema
    pub schema: DFSchemaRef,
    /// The table name
    pub name: String,
    /// Option to not error if table already exists
    pub if_not_exists: bool,

    /// Dummy result schema for the plan (empty)
    pub output_schema: DFSchemaRef,
}

#[derive(Debug, Clone)]
pub struct Insert {
    /// The table to insert into
    pub table: Arc<SeafowlTable>,
    /// Result of a query to insert (with a type-compatible schema that is a subset of the target table)
    pub input: Arc<LogicalPlan>,
    /// Dummy result schema for the plan (empty)
    pub output_schema: DFSchemaRef,
}

#[derive(Debug, Clone)]
pub struct Update {
    /// The table to update
    pub table: Arc<SeafowlTable>,
    /// WHERE clause
    pub selection: Option<Expr>,
    /// Subplan for a table scan without a WHERE clause (used by the query optimizer)
    pub table_plan: Arc<LogicalPlan>,
    /// Columns to update
    pub assignments: Vec<(String, Expr)>,
    /// Dummy result schema for the plan (empty)
    pub output_schema: DFSchemaRef,
}

#[derive(Debug, Clone)]
pub struct Delete {
    /// The table to delete from
    pub table: Arc<SeafowlTable>,
    /// Subplan for a table scan without a WHERE clause (used by the query optimizer)
    pub table_plan: Arc<LogicalPlan>,
    /// WHERE clause
    pub selection: Option<Expr>,
    /// Dummy result schema for the plan (empty)
    pub output_schema: DFSchemaRef,
}

#[derive(Debug, Clone)]
pub struct CreateFunction {
    /// The function name
    pub name: String,
    pub details: CreateFunctionDetails,
    /// Dummy result schema for the plan (empty)
    pub output_schema: DFSchemaRef,
}

#[derive(Debug, Clone)]
pub struct RenameTable {
    /// The table to rename
    pub table: Arc<SeafowlTable>,
    /// New name (including the schema name)
    pub new_name: String,
    /// Dummy result schema for the plan (empty)
    pub output_schema: DFSchemaRef,
}

#[derive(Debug, Clone)]
pub struct DropSchema {
    /// The schema to drop
    pub name: String,
    /// Dummy result schema for the plan (empty)
    pub output_schema: DFSchemaRef,
}

#[derive(Debug, Clone)]
pub struct Vacuum {
    /// Denotes whether to vacuum the partitions
    pub partitions: bool,
    /// If the vacuum target are not the partitions, denotes whether it applies to all tables, or a
    /// specific one
    pub table_id: Option<TableId>,
    /// Dummy result schema for the plan (empty)
    pub output_schema: DFSchemaRef,
}

#[derive(Debug, Clone)]
pub enum SeafowlExtensionNode {
    CreateTable(CreateTable),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    CreateFunction(CreateFunction),
    RenameTable(RenameTable),
    DropSchema(DropSchema),
    Vacuum(Vacuum),
}

impl SeafowlExtensionNode {
    pub fn from_dynamic(node: &Arc<dyn UserDefinedLogicalNode>) -> Option<&Self> {
        node.as_any().downcast_ref::<Self>()
    }
}

// Code for removing aliases in DELETE/UPDATE predicates
// Copied from `datafusion_expr::utils::from_plan` for Filter
// The query optimizer adds aliases to rewritten expressions in order
// to make them keep the same names. This is not needed in filters and, in our
// case, breaks partition pruning, which makes updates/deletes less efficient.

// To fix this, we remove all aliases in all expressions used by UPDATE/DELETE
struct RemoveAliases {}

impl ExprRewriter for RemoveAliases {
    fn pre_visit(&mut self, expr: &Expr) -> Result<RewriteRecursion, DataFusionError> {
        match expr {
            Expr::Exists { .. } | Expr::ScalarSubquery(_) | Expr::InSubquery { .. } => {
                // subqueries could contain aliases so we don't recurse into those
                Ok(RewriteRecursion::Stop)
            }
            Expr::Alias(_, _) => Ok(RewriteRecursion::Mutate),
            _ => Ok(RewriteRecursion::Continue),
        }
    }

    fn mutate(&mut self, expr: Expr) -> Result<Expr, DataFusionError> {
        Ok(expr.unalias())
    }
}

fn remove_aliases(predicate: Expr) -> Expr {
    let mut remove_aliases = RemoveAliases {};
    // NB we can't propagate errors in our logical nodes' from_template
    // (unlike vanilla DataFusion's from_plan), so we have to cross our fingers
    // and panic if something went wrong during the rewrite.
    predicate.rewrite(&mut remove_aliases).unwrap()
}

impl UserDefinedLogicalNode for SeafowlExtensionNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        match self {
            SeafowlExtensionNode::Insert(Insert { input, .. }) => vec![input.as_ref()],

            // For UPDATE/DELETE, the optimizer needs to know the schema of the "input" (in this
            // case, our tables we're updating) in order to optimize this node's expressions (in our
            // case, the WHERE ... and the SET col = expr clauses).
            SeafowlExtensionNode::Update(Update { table_plan, .. }) => {
                vec![table_plan.as_ref()]
            }
            SeafowlExtensionNode::Delete(Delete { table_plan, .. }) => {
                vec![table_plan.as_ref()]
            }
            _ => vec![],
        }
    }

    fn schema(&self) -> &DFSchemaRef {
        // These plans don't produce an output schema but we still
        // need to write out the match arms here, as we can't create a &DFSchemaRef
        // (& means it has to have been borrowed and we can't own anything, since this
        // function will exit soon)
        match self {
            SeafowlExtensionNode::Insert(Insert { output_schema, .. }) => output_schema,
            SeafowlExtensionNode::CreateTable(CreateTable { output_schema, .. }) => {
                output_schema
            }
            SeafowlExtensionNode::Update(Update { output_schema, .. }) => output_schema,
            SeafowlExtensionNode::Delete(Delete { output_schema, .. }) => output_schema,
            SeafowlExtensionNode::CreateFunction(CreateFunction {
                output_schema,
                ..
            }) => output_schema,
            SeafowlExtensionNode::RenameTable(RenameTable { output_schema, .. }) => {
                output_schema
            }
            SeafowlExtensionNode::DropSchema(DropSchema { output_schema, .. }) => {
                output_schema
            }
            SeafowlExtensionNode::Vacuum(Vacuum { output_schema, .. }) => output_schema,
        }
    }

    fn expressions(&self) -> Vec<Expr> {
        // NB: this is used by the plan optimizer (gets expressions(), optimizes them,
        // calls from_template(optimized_exprs) and we'll need to expose our expressions here
        // and support from_template for a given node if we want them to be optimized.
        match self {
            // For UPDATEs, we pack a list of all SET col = expr expressions and append
            // the WHERE clause expression at the end, if it exists.
            SeafowlExtensionNode::Update(Update {
                assignments,
                selection,
                ..
            }) => assignments
                .iter()
                .map(|(_, e)| e.clone())
                .chain(selection.clone().into_iter())
                .collect::<Vec<Expr>>(),
            SeafowlExtensionNode::Delete(Delete {
                selection: Some(e), ..
            }) => vec![e.clone()],
            _ => vec![],
        }
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SeafowlExtensionNode::Insert(Insert { table, .. }) => {
                write!(f, "Insert: {}", table.name)
            }
            SeafowlExtensionNode::CreateTable(CreateTable { name, .. }) => {
                write!(f, "Create: {}", name)
            }
            SeafowlExtensionNode::Update(Update {
                table,
                assignments,
                selection,
                ..
            }) => {
                write!(
                    f,
                    "Update: {}, SET: {}",
                    table.name,
                    assignments
                        .iter()
                        .map(|(c, e)| format!("{} = {}", c, e))
                        .collect::<Vec<String>>()
                        .join(", ")
                )?;
                if let Some(s) = selection {
                    write!(f, " WHERE {}", s)?;
                }
                Ok(())
            }
            SeafowlExtensionNode::Delete(Delete {
                table,
                selection: None,
                ..
            }) => {
                write!(f, "Delete: {}", table.name)
            }
            SeafowlExtensionNode::Delete(Delete {
                table,
                selection: Some(e),
                ..
            }) => {
                write!(f, "Delete: {} WHERE {}", table.name, e)
            }
            SeafowlExtensionNode::CreateFunction(CreateFunction { name, .. }) => {
                write!(f, "CreateFunction: {}", name)
            }
            SeafowlExtensionNode::RenameTable(RenameTable {
                table, new_name, ..
            }) => {
                write!(f, "RenameTable: {} to {}", table.name, new_name)
            }
            SeafowlExtensionNode::DropSchema(DropSchema { name, .. }) => {
                write!(f, "DropSchema: {}", name)
            }
            SeafowlExtensionNode::Vacuum(Vacuum { partitions, .. }) => {
                write!(
                    f,
                    "Vacuum: {}",
                    if *partitions { "partitions" } else { "tables" }
                )
            }
        }
    }

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        match self {
            SeafowlExtensionNode::Insert(Insert {
                table,
                input,
                output_schema,
            }) => Arc::new(SeafowlExtensionNode::Insert(Insert {
                table: table.clone(),
                input: match inputs.first() {
                    Some(new_input) => Arc::new(new_input.clone()),
                    None => input.clone(),
                },
                output_schema: output_schema.clone(),
            })),

            SeafowlExtensionNode::Update(Update {
                table,
                selection,
                table_plan,
                assignments,
                output_schema,
            }) => {
                // Defensive assertion to make sure that DataFusion gave us back the correct number
                // of expressions.
                let expected_len =
                    assignments.len() + (if selection.is_some() { 1 } else { 0 });
                if exprs.len() != expected_len {
                    // DataFusion doesn't let us give back an Error and this really shouldn't
                    // happen. Other alternatives (like partially initializing the node) might hide
                    // errors downstream, so panic here instead.
                    panic!("DataFusion optimizer returned incorrect number of expressions. Expected {}, got {}", expected_len, exprs.len())
                };

                let exprs: Vec<Expr> =
                    exprs.iter().map(|e| remove_aliases(e.clone())).collect();

                Arc::new(SeafowlExtensionNode::Update(Update {
                    // We ignore the optimized "inputs" in this case (it's just a TableScan without
                    // filters) and keep our old one
                    table: table.clone(),
                    table_plan: table_plan.clone(),
                    output_schema: output_schema.clone(),

                    // Unpack the assignments and the selection expression
                    assignments: assignments
                        .iter()
                        .zip(exprs.iter())
                        .map(|((col, _), new_expr)| (col.clone(), new_expr.clone()))
                        .collect(),
                    // If we have a selection expression in this node, the last entry in the list is
                    // the optimized expression
                    selection: if selection.is_none() {
                        None
                    } else {
                        exprs.get(assignments.len()).cloned()
                    },
                }))
            }

            SeafowlExtensionNode::Delete(Delete {
                table,
                table_plan,
                selection,
                output_schema,
            }) => Arc::new(SeafowlExtensionNode::Delete(Delete {
                table: table.clone(),
                table_plan: table_plan.clone(),
                output_schema: output_schema.clone(),
                selection: if selection.is_none() {
                    None
                } else {
                    let e = exprs.first();
                    if e.is_none() {
                        panic!("DataFusion optimizer returned incorrect number of expressions. Expected 1, got 0");
                    }
                    e.cloned().map(remove_aliases)
                },
            })),
            _ => Arc::from(self.clone()),
        }
    }
}
