// Get database by name / id
// Get collection by name / id
// Get all collections in a database
// Get table by name/id + collection (latest version)
//   - including all regions/partitions
// Get partitions matching a certain predicate?
// Get total table row count
//
//

use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{Error, PgPool, Row};

use crate::data_types::DatabaseId;

#[async_trait]
pub trait Repository: Send + Sync {
    async fn get_collections_in_database(
        &mut self,
        database_id: DatabaseId,
    ) -> Result<Vec<String>, Error>;
}

struct PostgresRepository {
    executor: PgPool,
}

#[async_trait]
impl Repository for PostgresRepository {
    async fn get_collections_in_database(
        &mut self,
        database_id: DatabaseId,
    ) -> Result<Vec<String>, Error> {
        let names = sqlx::query("SELECT name FROM collection WHERE database_id = $1")
            .bind(database_id)
            .fetch(&self.executor)
            .map_ok(|row| row.get("name"))
            .try_collect()
            .await?;
        Ok(names)
    }
}
