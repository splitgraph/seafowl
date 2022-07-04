pub struct DatabaseId(i64);
pub struct CollectionId(i64);
pub struct TableId(i64);
pub struct Timestamp(i64);
pub struct TableVersionId(i64);
pub struct TableRegionId(i64);
pub struct ColumnId(i64);

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
    pub database_id: DatabaseId,
    pub collection_id: CollectionId,
    pub name: String,
}

pub struct TableVersion {
    pub id: TableVersionId,
    pub table_id: TableId,
    // TODO do we need other IDs
    pub creation_time: Timestamp,
    // TODO is_deleted flag?
}

pub struct TableRegion {
    pub id: TableRegionId,
    pub table_version_id: TableVersionId,
    // TODO: json? a table with TableRegionId + ColumnId? + min/max values as binary?
    // Should we duplicate these for every region OR for every file?
    // Separate PhysicalRegion entity; make this a join table instead?
    pub min_max: String,
    // TODO object storage path (ID) here?
    pub row_count: u32,
    // TODO can we somehow store ordering here / in the TableVersion doc?
}

pub struct Column {
    pub id: ColumnId,
    pub table_version_id: TableVersionId,
    pub name: String,
    // TODO enum?
    pub column_type: String,
    // TODO ordinal?
}

// Get database by name / id
// Get collection by name / id
// Get all collections in a database
// Get table by name/id + collection (latest version)
//   - including all regions/partitions
// Get partitions matching a certain predicate?
// Get total table row count
//
//
