-- SQLite INTEGER PRIMARY KEY columns are automagically
-- aliased to the ROWID which is a unique autoincrementing
-- identifier. The only issue with is is that rowids can be reused
-- if a row gets deleted and recreated again (as opposed to PG's
-- IDENTITY rows). This means we need to make sure to have proper FKs and
-- deletion cascades.

CREATE TABLE database (
    id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE
);

CREATE TABLE collection (
    id INTEGER NOT NULL PRIMARY KEY,
    database_id BIGINT NOT NULL REFERENCES database(id) ON DELETE CASCADE,
    name VARCHAR NOT NULL,
    CONSTRAINT collection_name_unique UNIQUE(name, database_id)
);

CREATE TABLE "table" (
    id INTEGER NOT NULL PRIMARY KEY,
    collection_id BIGINT NOT NULL REFERENCES collection(id) ON DELETE CASCADE,
    name VARCHAR NOT NULL,
    CONSTRAINT table_name_unique UNIQUE(name, collection_id)
);

CREATE TABLE table_version (
    id INTEGER NOT NULL PRIMARY KEY,
    table_id BIGINT NOT NULL REFERENCES "table"(id) ON DELETE CASCADE,
    creation_time INTEGER(4) NOT NULL DEFAULT((strftime('%s','now')))
);

CREATE TABLE table_column (
    id INTEGER NOT NULL PRIMARY KEY,
    table_version_id BIGINT NOT NULL REFERENCES table_version(id) ON DELETE CASCADE,
    name VARCHAR NOT NULL,
    type VARCHAR NOT NULL,
    CONSTRAINT column_name_unique UNIQUE(name, table_version_id)
);

CREATE TABLE physical_region (
    id INTEGER NOT NULL PRIMARY KEY,
    row_count INTEGER(4) NOT NULL,
    object_storage_id VARCHAR NOT NULL
);

CREATE TABLE physical_region_column (
    id INTEGER NOT NULL PRIMARY KEY,
    physical_region_id BIGINT NOT NULL REFERENCES physical_region(id) ON DELETE CASCADE,
    name VARCHAR NOT NULL,
    type VARCHAR NOT NULL,
    min_value BLOB,
    max_value BLOB
);

CREATE TABLE table_region (
    table_version_id BIGINT NOT NULL REFERENCES table_version(id) ON DELETE CASCADE,
    -- Don't CASCADE deletions of region <> table references as a safeguard against
    -- deleting regions that are still referenced by something
    physical_region_id BIGINT NOT NULL REFERENCES physical_region(id),
    PRIMARY KEY(table_version_id, physical_region_id)
);