pub type DatabaseId = i64;
pub type CollectionId = i64;
pub type TableId = i64;
pub type TableVersionId = i64;
pub type Timestamp = i64;
pub type TableColumnId = i64;
pub type PhysicalPartitionId = i64;
pub type PhysicalPartitionColumnId = i64;
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

#[derive(Debug, PartialEq, Eq)]
pub struct PhysicalPartition {
    pub id: PhysicalPartitionId,
    pub row_count: i32,
    pub object_storage_id: String,
}

#[derive(Debug, PartialEq, Eq)]
pub struct PhysicalPartitionColumn {
    pub id: PhysicalPartitionColumnId,
    pub physical_partition_id: PhysicalPartitionId,
    pub name: String,
    pub r#type: String,
    pub min_value: Option<Vec<u8>>,
    pub max_value: Option<Vec<u8>>,
}

pub struct TablePartition {
    pub table_version_id: TableVersionId,
    pub physical_partition_id: PhysicalPartitionId,
}
