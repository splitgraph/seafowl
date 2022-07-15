use std::{any::Any, fmt, sync::Arc, vec};

use datafusion::logical_plan::{Column, DFSchemaRef, Expr, LogicalPlan, UserDefinedLogicalNode};

use crate::provider::SeafowlTable;

#[derive(Debug)]
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

#[derive(Debug)]
pub struct Insert {
    /// The table to insert into
    pub table: Arc<SeafowlTable>,
    /// Result of a query to insert (with a type-compatible schema that is a subset of the target table)
    pub input: Arc<LogicalPlan>,
    /// Dummy result schema for the plan (empty)
    pub output_schema: DFSchemaRef,
}

#[derive(Debug)]
pub struct Assignment {
    pub column: Column,
    pub expr: Expr,
}

#[derive(Debug)]
pub struct Update {
    /// The table name (TODO: should this be a table ref?)
    pub name: String,
    /// WHERE clause
    pub selection: Option<Expr>,
    /// Columns to update
    pub assignments: Vec<Assignment>,
    /// Dummy result schema for the plan (empty)
    pub output_schema: DFSchemaRef,
}

#[derive(Debug)]
pub struct Delete {
    /// The table name (TODO: should this be a table ref?)
    pub name: String,
    /// WHERE clause
    pub selection: Option<Expr>,
    /// Dummy result schema for the plan (empty)
    pub output_schema: DFSchemaRef,
}

#[derive(Debug)]
pub enum SeafowlExtensionNode {
    CreateTable(CreateTable),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
}

impl SeafowlExtensionNode {
    pub fn from_dynamic(node: &Arc<dyn UserDefinedLogicalNode>) -> Option<&Self> {
        node.as_any().downcast_ref::<Self>()
    }
}

impl UserDefinedLogicalNode for SeafowlExtensionNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        match self {
            SeafowlExtensionNode::Insert(Insert { input, .. }) => vec![input.as_ref()],
            // TODO Update/Delete will probably have children
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
            SeafowlExtensionNode::CreateTable(CreateTable { output_schema, .. }) => output_schema,
            SeafowlExtensionNode::Update(Update { output_schema, .. }) => output_schema,
            SeafowlExtensionNode::Delete(Delete { output_schema, .. }) => output_schema,
        }
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SeafowlExtensionNode::Insert(Insert { table, .. }) => {
                write!(f, "Insert: {}", table.name)
            }
            SeafowlExtensionNode::CreateTable(CreateTable { name, .. }) => {
                write!(f, "Create: {}", name)
            }
            SeafowlExtensionNode::Update(Update { name, .. }) => write!(f, "Update: {}", name),
            SeafowlExtensionNode::Delete(Delete { name, .. }) => {
                write!(f, "Delete: {}", name)
            }
        }
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        _inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        todo!()
    }
}
