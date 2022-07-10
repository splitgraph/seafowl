pub type DatabaseId = i64;
pub type CollectionId = i64;
pub type TableId = i64;
pub type TableVersionId = i64;
pub type Timestamp = i64;
pub type TableColumnId = i64;
pub type PhysicalRegionId = i64;
pub type PhysicalRegionColumnId = i64;

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
pub struct PhysicalRegion {
    pub id: PhysicalRegionId,
    pub row_count: i32,
    pub object_storage_id: String,
}

#[derive(Debug, PartialEq, Eq)]
pub struct PhysicalRegionColumn {
    pub id: PhysicalRegionColumnId,
    pub physical_region_id: PhysicalRegionId,
    pub name: String,
    pub r#type: String,
    pub min_value: Option<Vec<u8>>,
    pub max_value: Option<Vec<u8>>,
}

pub struct TableRegion {
    pub table_version_id: TableVersionId,
    pub physical_region_id: PhysicalRegionId,
}
