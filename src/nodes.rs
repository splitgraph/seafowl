use datafusion::common::DFSchemaRef;

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

impl UserDefinedLogicalNode for SeafowlExtensionNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        // These plans don't produce an output schema but we still
        // need to write out the match arms here, as we can't create a &DFSchemaRef
        // (& means it has to have been borrowed and we can't own anything, since this
        // function will exit soon)
        match self {
            SeafowlExtensionNode::CreateTable(CreateTable { output_schema, .. }) => {
                output_schema
            }
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
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SeafowlExtensionNode::CreateTable(CreateTable { name, .. }) => {
                write!(f, "Create: {name}")
            }
            SeafowlExtensionNode::CreateFunction(CreateFunction { name, .. }) => {
                write!(f, "CreateFunction: {name}")
            }
            SeafowlExtensionNode::RenameTable(RenameTable {
                table, new_name, ..
            }) => {
                write!(f, "RenameTable: {} to {}", table.name, new_name)
            }
            SeafowlExtensionNode::DropSchema(DropSchema { name, .. }) => {
                write!(f, "DropSchema: {name}")
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
        _exprs: &[Expr],
        _inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::from(self.clone())
    }
}
