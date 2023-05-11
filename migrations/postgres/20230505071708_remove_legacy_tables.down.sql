CREATE TABLE physical_partition (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    row_count INTEGER NOT NULL,
    object_storage_id VARCHAR NOT NULL
);

CREATE TABLE physical_partition_column (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    physical_partition_id BIGINT NOT NULL REFERENCES physical_partition(id) ON DELETE CASCADE,
    name VARCHAR NOT NULL,
    type VARCHAR NOT NULL,
    min_value BYTEA,
    max_value BYTEA,
    null_count INTEGER
);

CREATE TABLE table_partition (
    table_version_id BIGINT NOT NULL REFERENCES table_version(id) ON DELETE CASCADE,
    -- Don't CASCADE deletions of partition <> table references as a safeguard against
    -- deleting partitions that are still referenced by something
    physical_partition_id BIGINT NOT NULL REFERENCES physical_partition(id),
    PRIMARY KEY(table_version_id, physical_partition_id)
);
