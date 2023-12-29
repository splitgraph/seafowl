use std::sync::Arc;

use arrow_schema::Schema;
use async_trait::async_trait;
use itertools::Itertools;
use uuid::Uuid;

use clade::catalog::TableObject;
use clade::schema::{ListSchemaResponse, SchemaObject};

use crate::catalog::{
    CatalogError, CatalogResult, CatalogStore, FunctionStore, SchemaStore, TableStore,
    STAGING_SCHEMA,
};
use crate::repository::interface::{
    AllDatabaseFunctionsResult, CollectionRecord, Error as RepositoryError, Repository,
    TableId, TableVersionId, TableVersionsResult,
};
use crate::repository::interface::{
    DatabaseRecord, DroppedTableDeletionStatus, DroppedTablesResult, TableRecord,
};
use crate::wasm_udf::data_types::CreateFunctionDetails;

// The native catalog implementation for Seafowl.
pub struct RepositoryStore {
    pub repository: Arc<dyn Repository>,
}

impl From<RepositoryError> for CatalogError {
    fn from(err: RepositoryError) -> CatalogError {
        CatalogError::SqlxError(match err {
            RepositoryError::UniqueConstraintViolation(e) => e,
            RepositoryError::FKConstraintViolation(e) => e,
            RepositoryError::SqlxError(e) => e,
        })
    }
}

#[async_trait]
impl CatalogStore for RepositoryStore {
    async fn create(&self, name: &str) -> CatalogResult<()> {
        self.repository
            .create_database(name)
            .await
            .map_err(|e| match e {
                RepositoryError::UniqueConstraintViolation(_) => {
                    CatalogError::CatalogAlreadyExists {
                        name: name.to_string(),
                    }
                }
                e => e.into(),
            })?;

        Ok(())
    }

    async fn get(&self, name: &str) -> CatalogResult<DatabaseRecord> {
        self.repository
            .get_database(name)
            .await
            .map_err(|e| match e {
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    CatalogError::CatalogDoesNotExist {
                        name: name.to_string(),
                    }
                }
                e => e.into(),
            })
    }

    async fn delete(&self, name: &str) -> CatalogResult<()> {
        let database = CatalogStore::get(self, name).await?;

        self.repository
            .delete_database(database.id)
            .await
            .map_err(|e| match e {
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    CatalogError::CatalogDoesNotExist {
                        name: name.to_string(),
                    }
                }
                e => e.into(),
            })
    }
}

#[async_trait]
impl SchemaStore for RepositoryStore {
    async fn create(&self, catalog_name: &str, schema_name: &str) -> CatalogResult<()> {
        if schema_name == STAGING_SCHEMA {
            return Err(CatalogError::UsedStagingSchema);
        }

        let database = CatalogStore::get(self, catalog_name).await?;

        self.repository
            .create_collection(database.id, schema_name)
            .await
            .map_err(|e| match e {
                RepositoryError::UniqueConstraintViolation(_) => {
                    CatalogError::SchemaAlreadyExists {
                        name: schema_name.to_string(),
                    }
                }
                e => e.into(),
            })?;

        Ok(())
    }

    async fn list(&self, catalog_name: &str) -> CatalogResult<ListSchemaResponse> {
        let cols = self.repository.list_collections(catalog_name).await?;

        let schemas = cols
            .iter()
            .group_by(|col| &col.collection_name)
            .into_iter()
            .map(|(cn, ct)| SchemaObject {
                catalog: None,
                name: cn.clone(),
                tables: ct
                    .into_iter()
                    .filter_map(|t| {
                        if let Some(name) = &t.table_name
                            && let Some(uuid) = t.table_uuid
                        {
                            Some(TableObject {
                                schema: None,
                                name: name.clone(),
                                location: uuid.to_string(),
                            })
                        } else {
                            None
                        }
                    })
                    .collect(),
            })
            .collect();

        Ok(ListSchemaResponse { schemas })
    }

    async fn get(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> CatalogResult<CollectionRecord> {
        if schema_name == STAGING_SCHEMA {
            return Err(CatalogError::UsedStagingSchema);
        }

        self.repository
            .get_collection(catalog_name, schema_name)
            .await
            .map_err(|e| match e {
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    CatalogError::SchemaDoesNotExist {
                        name: schema_name.to_string(),
                    }
                }
                e => e.into(),
            })
    }

    async fn delete(&self, catalog_name: &str, schema_name: &str) -> CatalogResult<()> {
        let schema = SchemaStore::get(self, catalog_name, schema_name).await?;

        self.repository
            .delete_collection(schema.id)
            .await
            .map_err(|e| match e {
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    CatalogError::SchemaDoesNotExist {
                        name: schema_name.to_string(),
                    }
                }
                e => e.into(),
            })
    }
}

#[async_trait]
impl TableStore for RepositoryStore {
    async fn create(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        schema: &Schema,
        uuid: Uuid,
    ) -> CatalogResult<(TableId, TableVersionId)> {
        let collection = SchemaStore::get(self, catalog_name, schema_name).await?;

        self.repository
            .create_table(collection.id, table_name, schema, uuid)
            .await
            .map_err(|e| match e {
                RepositoryError::UniqueConstraintViolation(_) => {
                    CatalogError::TableAlreadyExists {
                        name: table_name.to_string(),
                    }
                }
                RepositoryError::FKConstraintViolation(_) => {
                    CatalogError::SchemaDoesNotExist {
                        name: schema_name.to_string(),
                    }
                }
                RepositoryError::SqlxError(e) => CatalogError::SqlxError(e),
            })
    }

    async fn get(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> CatalogResult<TableRecord> {
        self.repository
            .get_table(catalog_name, schema_name, table_name)
            .await
            .map_err(|e| match e {
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    CatalogError::TableDoesNotExist {
                        name: table_name.to_string(),
                    }
                }
                e => e.into(),
            })
    }

    async fn create_new_version(
        &self,
        uuid: Uuid,
        version: i64,
    ) -> CatalogResult<TableVersionId> {
        self.repository
            .create_new_version(uuid, version)
            .await
            .map_err(|e| match e {
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    CatalogError::TableUuidDoesNotExist { uuid }
                }
                e => e.into(),
            })
    }

    async fn delete_old_versions(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> CatalogResult<u64> {
        let table = TableStore::get(self, catalog_name, schema_name, table_name).await?;

        Ok(self.repository.delete_old_versions(table.id).await?)
    }

    async fn get_all_versions(
        &self,
        catalog_name: &str,
        table_names: Option<Vec<String>>,
    ) -> CatalogResult<Vec<TableVersionsResult>> {
        Ok(self
            .repository
            .get_all_versions(catalog_name, table_names)
            .await?)
    }

    async fn update(
        &self,
        old_catalog_name: &str,
        old_schema_name: &str,
        old_table_name: &str,
        new_catalog_name: &str,
        new_schema_name: &str,
        new_table_name: &str,
    ) -> CatalogResult<()> {
        assert_eq!(
            old_catalog_name, new_catalog_name,
            "Moving across catalogs not yet supported"
        );

        let table =
            TableStore::get(self, old_catalog_name, old_schema_name, old_table_name)
                .await?;
        let new_schema_id = if new_schema_name != old_schema_name {
            let schema =
                SchemaStore::get(self, old_catalog_name, new_schema_name).await?;
            Some(schema.id)
        } else {
            None
        };

        self.repository
            .rename_table(table.id, new_table_name, new_schema_id)
            .await
            .map_err(|e| match e {
                RepositoryError::FKConstraintViolation(_) => {
                    // We only FK on collection_id, so this will be Some
                    CatalogError::SchemaDoesNotExist {
                        name: new_schema_name.to_string(),
                    }
                }
                RepositoryError::UniqueConstraintViolation(_) => {
                    CatalogError::TableAlreadyExists {
                        name: new_table_name.to_string(),
                    }
                }
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    CatalogError::TableDoesNotExist {
                        name: old_table_name.to_string(),
                    }
                }
                e => e.into(),
            })
    }

    async fn delete(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> CatalogResult<()> {
        let table = TableStore::get(self, catalog_name, schema_name, table_name).await?;

        self.repository
            .delete_table(table.id)
            .await
            .map_err(|e| match e {
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    CatalogError::TableDoesNotExist {
                        name: table_name.to_string(),
                    }
                }
                e => e.into(),
            })
    }

    async fn get_dropped_tables(
        &self,
        catalog_name: Option<String>,
    ) -> CatalogResult<Vec<DroppedTablesResult>> {
        Ok(self.repository.get_dropped_tables(catalog_name).await?)
    }

    async fn update_dropped_table(
        &self,
        uuid: Uuid,
        deletion_status: DroppedTableDeletionStatus,
    ) -> CatalogResult<()> {
        self.repository
            .update_dropped_table(uuid, deletion_status)
            .await
            .map_err(|e| match e {
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    CatalogError::TableUuidDoesNotExist { uuid }
                }
                e => e.into(),
            })
    }

    async fn delete_dropped_table(&self, uuid: Uuid) -> CatalogResult<()> {
        self.repository
            .delete_dropped_table(uuid)
            .await
            .map_err(|e| match e {
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    CatalogError::TableUuidDoesNotExist { uuid }
                }
                e => e.into(),
            })
    }
}

#[async_trait]
impl FunctionStore for RepositoryStore {
    async fn create(
        &self,
        catalog_name: &str,
        function_name: &str,
        or_replace: bool,
        details: &CreateFunctionDetails,
    ) -> CatalogResult<()> {
        let database = CatalogStore::get(self, catalog_name).await?;

        self.repository
            .create_function(database.id, function_name, or_replace, details)
            .await
            .map_err(|e| match e {
                RepositoryError::FKConstraintViolation(_) => {
                    CatalogError::CatalogDoesNotExist {
                        name: catalog_name.to_string(),
                    }
                }
                RepositoryError::UniqueConstraintViolation(_) => {
                    CatalogError::FunctionAlreadyExists {
                        name: function_name.to_string(),
                    }
                }
                e => e.into(),
            })?;

        Ok(())
    }

    async fn list(
        &self,
        catalog_name: &str,
    ) -> CatalogResult<Vec<AllDatabaseFunctionsResult>> {
        let database = CatalogStore::get(self, catalog_name).await?;

        Ok(self
            .repository
            .get_all_functions_in_database(database.id)
            .await?)
    }

    async fn delete(
        &self,
        catalog_name: &str,
        if_exists: bool,

        func_names: &[String],
    ) -> CatalogResult<()> {
        let database = CatalogStore::get(self, catalog_name).await?;

        match self.repository.drop_function(database.id, func_names).await {
            Ok(id) => Ok(id),
            Err(RepositoryError::FKConstraintViolation(_)) => {
                Err(CatalogError::CatalogDoesNotExist {
                    name: catalog_name.to_string(),
                })
            }
            Err(RepositoryError::SqlxError(sqlx::error::Error::RowNotFound)) => {
                if if_exists {
                    Ok(())
                } else {
                    Err(CatalogError::FunctionNotFound {
                        names: func_names.join(", "),
                    })
                }
            }
            Err(e) => Err(e.into()),
        }
    }
}
