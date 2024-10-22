use crate::sync::{SyncError, SyncResult};
use arrow_schema::{DataType, FieldRef, Fields, SchemaRef};
use clade::sync::{ColumnDescriptor, ColumnRole};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

pub(super) mod join;
pub(super) mod merge;
pub(super) mod union;

#[derive(Clone, Debug, PartialEq)]
pub struct SyncSchema {
    columns: Vec<SyncColumn>,
    indices: HashMap<ColumnRole, HashMap<Arc<str>, usize>>,
}

impl SyncSchema {
    pub fn try_new(
        column_descriptors: Vec<ColumnDescriptor>,
        schema: SchemaRef,
        validate_pks: bool,
    ) -> SyncResult<Self> {
        if column_descriptors.len() != schema.flattened_fields().len() {
            return Err(SyncError::SchemaError {
                reason: "Column descriptors do not match the schema".to_string(),
            });
        }

        // Validate field role's are parsable, we have the correct number of old/new PKs,
        // and Changed role is a boolean type which points to an existing column
        // TODO: Validate a column can not be a PK and Value at the same time
        let mut old_pk_types = HashSet::new();
        let mut new_pk_types = HashSet::new();
        for (col_desc, field) in column_descriptors.iter().zip(schema.fields()) {
            match ColumnRole::try_from(col_desc.role) {
                Ok(ColumnRole::OldPk) => {
                    old_pk_types.insert((&col_desc.name, field.data_type().clone()));
                }
                Ok(ColumnRole::NewPk) => {
                    new_pk_types.insert((&col_desc.name, field.data_type().clone()));
                }
                Ok(ColumnRole::Value) => {}
                Ok(ColumnRole::Changed) => {
                    let err = if field.data_type() != &DataType::Boolean {
                        format!(
                            "Field for column {} with `Changed` role must be of type boolean, got {}",
                            field.name(),
                            field.data_type(),
                        )
                    } else if !column_descriptors.iter().any(|other_cd| {
                        col_desc.name == other_cd.name
                            && other_cd.role == ColumnRole::Value as i32
                    }) {
                        format!(
                            "Column with `Changed` role must point to an existing column with `Value` role: {}",
                            &col_desc.name
                        )
                    } else {
                        // All good
                        continue;
                    };
                    return Err(SyncError::SchemaError { reason: err });
                }
                Err(err) => {
                    return Err(SyncError::SchemaError {
                        reason: format!("Failed parsing role: {err:?}"),
                    });
                }
            }
        }

        if validate_pks {
            if old_pk_types.is_empty() || new_pk_types.is_empty() {
                return Err(SyncError::SchemaError {
                    reason:
                        "Change requested but batches do not contain old/new PK columns"
                            .to_string(),
                });
            }

            if old_pk_types != new_pk_types {
                return Err(SyncError::SchemaError {
                    reason:
                        "Change requested but old and new PK columns are not the same"
                            .to_string(),
                });
            }
        }

        let mut indices = HashMap::new();
        let columns = column_descriptors
            .into_iter()
            .zip(schema.fields())
            .enumerate()
            .map(|(idx, (column_descriptor, field))| {
                let role = column_descriptor.role();
                let name: Arc<str> = Arc::from(column_descriptor.name.as_str());

                let sync_column = SyncColumn {
                    role,
                    name: name.clone(),
                    field: field.clone(),
                };

                indices
                    .entry(role)
                    .or_insert(HashMap::new())
                    .insert(name.clone(), idx);

                sync_column
            })
            .collect();

        Ok(Self { columns, indices })
    }

    pub fn empty() -> Self {
        SyncSchema {
            columns: vec![],
            indices: Default::default(),
        }
    }

    // Replace the existing field references for sync columns with new ones.
    pub fn with_fields(&mut self, fields: &Fields) -> SyncResult<()> {
        if fields.len() != self.columns.len() {
            return Err(SyncError::SchemaError {
                reason: "New and old field count is different".to_string(),
            });
        }

        for (col, field) in self.columns.iter_mut().zip(fields.iter()) {
            if col.field.data_type() != field.data_type() {
                return Err(SyncError::SchemaError {
                    reason: format!(
                        "Expected {} but got {} for column {col:?}",
                        col.field.data_type(),
                        field.data_type()
                    ),
                });
            }

            col.field = field.clone();
        }

        Ok(())
    }

    pub fn column(&self, name: &str, role: ColumnRole) -> Option<&SyncColumn> {
        self.indices
            .get(&role)
            .and_then(|cols| cols.get(name))
            .map(|idx| &self.columns[*idx])
    }

    pub fn columns(&self) -> &[SyncColumn] {
        &self.columns
    }

    // Map over all columns with a specific role
    pub fn map_columns<F, T>(&self, role: ColumnRole, f: F) -> Vec<T>
    where
        Self: Sized,
        F: Fn(&SyncColumn) -> T,
    {
        self.indices
            .get(&role)
            .map(|role_cols| {
                role_cols
                    .values()
                    .map(|idx| f(&self.columns[*idx]))
                    .collect::<Vec<T>>()
            })
            .unwrap_or_default()
    }

    // Check whether this and the other sync schemas are the same.
    //
    // This is a shallower check than the equality check, since it checks for the sync column roles,
    // names, data types and order (and doesn't inspect the underlying arrows field equality).
    pub fn is_compatible_with(&self, other: &SyncSchema) -> bool {
        self.columns()
            .iter()
            .zip(other.columns().iter())
            .all(|(this, other)| {
                this.name == other.name
                    && this.role == other.role
                    && this.field().data_type() == other.field().data_type()
            })
    }
}

impl Display for SyncSchema {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.columns()
                .iter()
                .map(|sync_col| format!(
                    "{{{}, {}, {}, {}}}",
                    sync_col.name,
                    sync_col.role.as_str_name(),
                    sync_col.field.data_type(),
                    sync_col.field.name(),
                ))
                .collect::<Vec<String>>()
                .join(", "),
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SyncColumn {
    role: ColumnRole,
    name: Arc<str>,
    field: FieldRef,
}

impl SyncColumn {
    // Get the role of the column
    pub fn role(&self) -> ColumnRole {
        self.role
    }

    // Name of the column that this descriptor refers to. Note that in general this differs from the
    // underlying arrow field name. You can think of them as physical and logical names, respectively.
    //
    // For all roles except `Changed`, the sync column refers to itself, but for `Changed` it refers
    // to a `Value` column whose field name is the same as this name.
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    // Get the field from the arrow schema
    pub fn field(&self) -> &FieldRef {
        &self.field
    }

    // Returns a canonical `SyncColumn` field (logical) name, used to name columns in a projection
    pub fn canonical_field_name(role: ColumnRole, name: &str) -> String {
        format!("{}_{name}", role.as_str_name().to_lowercase())
    }

    // Returns a corresponding `ColumnDescriptor` for this column
    pub fn column_descriptor(&self) -> ColumnDescriptor {
        ColumnDescriptor {
            role: self.role as _,
            name: self.name.to_string(),
        }
    }
}

// A test helper to avoid having to explicitly specify verbose column descriptors for each case, but
// instead have them be implicitly defined through arrow field names.
#[cfg(test)]
pub fn arrow_to_sync_schema(schema: SchemaRef) -> crate::sync::SyncResult<SyncSchema> {
    let col_desc = schema
        .fields
        .iter()
        .map(|f| {
            let (role, name) = f
                .name()
                .rsplit_once("_")
                .expect("Test field names have <role>_<name> format");
            ColumnDescriptor {
                role: ColumnRole::from_str_name(&role.to_uppercase())
                    .expect("Test field name with valid role") as _,
                name: name.to_string(),
            }
        })
        .collect();

    SyncSchema::try_new(col_desc, schema, true)
}
