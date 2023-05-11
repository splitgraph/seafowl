pub type DatabaseId = i64;
pub type CollectionId = i64;
pub type TableId = i64;
pub type TableVersionId = i64;
pub type Timestamp = i64;
pub type TableColumnId = i64;
pub type FunctionId = i64;

// TODO: most of these structs currently aren't used (we use versions
// without IDs since they can be passed to db-writing routines before
// we know the ID)

pub struct Database {
    pub id: DatabaseId,
    pub name: String,
}

pub struct Collection {
    pub id: CollectionId,
    pub database_id: DatabaseId,
    pub name: String,
}

pub struct Table {
    pub id: TableId,
    pub collection_id: CollectionId,
    pub name: String,
}

pub struct TableVersion {
    pub id: TableVersionId,
    pub table_id: TableId,
    pub creation_time: Timestamp,
    // TODO is_deleted flag?
}

pub struct TableColumn {
    pub id: TableColumnId,
    pub table_version_id: TableVersionId,
    pub name: String,
    // TODO enum?
    pub r#type: String,
    // TODO ordinal?
}
