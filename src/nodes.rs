use datafusion::common::DFSchemaRef;

use arrow_schema::Schema;
use std::hash::{Hash, Hasher};
use std::{any::Any, fmt, sync::Arc, vec};

use crate::wasm_udf::data_types::CreateFunctionDetails;
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNode};
use strum_macros::AsRefStr;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ConvertTable {
    /// Location from which to convert
    pub location: String,
    /// Name of the table to convert to
    pub name: String,
    /// Dummy result schema for the plan (empty)
    pub output_schema: DFSchemaRef,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CreateTable {
    /// The table schema
    pub schema: Schema,
    /// The table name
    pub name: String,
    /// Option to not error if table already exists
    pub if_not_exists: bool,

    /// Dummy result schema for the plan (empty)
    pub output_schema: DFSchemaRef,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CreateFunction {
    /// The function name
    pub name: String,
    pub or_replace: bool,
    pub details: CreateFunctionDetails,
    /// Dummy result schema for the plan (empty)
    pub output_schema: DFSchemaRef,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct DropFunction {
    pub if_exists: bool,
    pub func_names: Vec<String>,
    /// Dummy result schema for the plan (empty)
    pub output_schema: DFSchemaRef,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct RenameTable {
    /// Old name
    pub old_name: String,
    /// New name (including the schema name)
    pub new_name: String,
    /// Dummy result schema for the plan (empty)
    pub output_schema: DFSchemaRef,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Vacuum {
    /// Denotes whether to vacuum dropped tables in a particular database
    pub database: Option<String>,
    /// If the vacuum target are not the partitions or the db, denotes which table it applies to
    pub table_name: Option<String>,
    /// Dummy result schema for the plan (empty)
    pub output_schema: DFSchemaRef,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Truncate {
    pub table_name: String,
    /// Dummy result schema for the plan (empty)
    pub output_schema: DFSchemaRef,
}

#[derive(AsRefStr, Debug, Clone, Hash, PartialEq, Eq)]
pub enum SeafowlExtensionNode {
    ConvertTable(ConvertTable),
    CreateTable(CreateTable),
    CreateFunction(CreateFunction),
    DropFunction(DropFunction),
    RenameTable(RenameTable),
    Truncate(Truncate),
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

    fn name(&self) -> &str {
        self.as_ref()
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
            SeafowlExtensionNode::ConvertTable(ConvertTable {
                output_schema, ..
            }) => output_schema,
            SeafowlExtensionNode::CreateTable(CreateTable { output_schema, .. }) => {
                output_schema
            }
            SeafowlExtensionNode::CreateFunction(CreateFunction {
                output_schema,
                ..
            }) => output_schema,
            SeafowlExtensionNode::DropFunction(DropFunction {
                output_schema, ..
            }) => output_schema,
            SeafowlExtensionNode::RenameTable(RenameTable { output_schema, .. }) => {
                output_schema
            }
            SeafowlExtensionNode::Truncate(Truncate { output_schema, .. }) => {
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
            SeafowlExtensionNode::ConvertTable(ConvertTable {
                location, name, ..
            }) => {
                write!(f, "Convert: {location} to {name}")
            }
            SeafowlExtensionNode::CreateTable(CreateTable { name, .. }) => {
                write!(f, "Create: {name}")
            }
            SeafowlExtensionNode::CreateFunction(CreateFunction { name, .. }) => {
                write!(f, "CreateFunction: {name}")
            }
            SeafowlExtensionNode::DropFunction(DropFunction { func_names, .. }) => {
                let names_str = func_names.join(", ");
                write!(f, "DropFunction: {names_str}")
            }
            SeafowlExtensionNode::RenameTable(RenameTable {
                old_name, new_name, ..
            }) => {
                write!(f, "RenameTable: {} to {}", old_name, new_name)
            }
            SeafowlExtensionNode::Truncate(Truncate { table_name, .. }) => {
                write!(f, "Truncate: {table_name}")
            }
            SeafowlExtensionNode::Vacuum(Vacuum { database, .. }) => {
                write!(
                    f,
                    "Vacuum: {}",
                    if database.is_some() {
                        "database"
                    } else {
                        "tables"
                    }
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

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Arc<dyn UserDefinedLogicalNode>> {
        Ok(Arc::from(self.clone()))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s)
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        match other.as_any().downcast_ref::<Self>() {
            Some(o) => self == o,
            None => false,
        }
    }
}
