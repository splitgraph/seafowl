use crate::frontend::flight::sync::SyncError;
use arrow_schema::{DataType, FieldRef, SchemaRef};
use clade::sync::{ColumnDescriptor, ColumnRole};
use std::collections::HashSet;

#[derive(Clone, Debug)]
pub struct SyncSchema {
    columns: Vec<SyncColumn>,
}

impl SyncSchema {
    pub fn try_new(
        column_descriptors: Vec<ColumnDescriptor>,
        schema: SchemaRef,
    ) -> Result<Self, SyncError> {
        if column_descriptors.len() != schema.all_fields().len() {
            return Err(SyncError::SchemaError {
                reason: "Column descriptors do not match the schema".to_string(),
            });
        }

        // Validate field role's are parsable, we have the correct number of old/new PKs,
        // and Changed role is non-nullable boolean type which points to an existing column
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
                            "Field for column with `Changed` role must be of type boolean: {}",
                            field.name()
                        )
                    } else if field.is_nullable() {
                        format!(
                            "Field for column with `Changed` role can not be nullable: {}",
                            field.name()
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

        if old_pk_types.is_empty() || new_pk_types.is_empty() {
            return Err(SyncError::SchemaError {
                reason: "Change requested but batches do not contain old/new PK columns"
                    .to_string(),
            });
        }

        if old_pk_types != new_pk_types {
            return Err(SyncError::SchemaError {
                reason: "Change requested but old and new PK columns are not the same"
                    .to_string(),
            });
        }

        let columns = column_descriptors
            .iter()
            .zip(schema.fields())
            .map(|(column_descriptor, field)| SyncColumn {
                role: column_descriptor.role(),
                name: column_descriptor.name.clone(),
                field: field.clone(),
            })
            .collect();

        Ok(Self { columns })
    }

    pub fn column(&self, name: &str, role: ColumnRole) -> Option<&SyncColumn> {
        self.columns()
            .iter()
            .find(|col| col.name == name && col.role == role)
    }

    pub fn columns(&self) -> &[SyncColumn] {
        &self.columns
    }

    // Map over all columns with a specific role
    pub fn map_columns<F, T>(&self, role: ColumnRole, f: F) -> Vec<T>
    where
        Self: Sized,
        F: FnMut(&SyncColumn) -> T,
    {
        self.columns
            .iter()
            .filter(|sc| sc.role == role)
            .map(f)
            .collect::<Vec<T>>()
    }
}

#[derive(Clone, Debug)]
pub struct SyncColumn {
    role: ColumnRole,
    name: String,
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
    pub fn name(&self) -> &String {
        &self.name
    }

    // Get the field from the arrow schema
    pub fn field(&self) -> &FieldRef {
        &self.field
    }
}
