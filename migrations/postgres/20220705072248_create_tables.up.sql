CREATE TABLE database (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE
);

CREATE TABLE collection (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    database_id BIGINT NOT NULL REFERENCES database(id) ON DELETE CASCADE,
    name VARCHAR NOT NULL,
    CONSTRAINT collection_name_unique UNIQUE(name, database_id)
);

CREATE TABLE "table" (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    collection_id BIGINT NOT NULL REFERENCES collection(id) ON DELETE CASCADE,
    name VARCHAR NOT NULL,
    CONSTRAINT table_name_unique UNIQUE(name, collection_id)
);

CREATE TABLE table_version (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_id BIGINT NOT NULL REFERENCES "table"(id) ON DELETE CASCADE,
    creation_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT(now())
);

CREATE TABLE table_column (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_version_id BIGINT NOT NULL REFERENCES table_version(id) ON DELETE CASCADE,
    name VARCHAR NOT NULL,
    type VARCHAR NOT NULL,
    CONSTRAINT column_name_unique UNIQUE(name, table_version_id)
);

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
    max_value BYTEA
);

CREATE TABLE table_partition (
    table_version_id BIGINT NOT NULL REFERENCES table_version(id) ON DELETE CASCADE,
    -- Don't CASCADE deletions of partition <> table references as a safeguard against
    -- deleting partitions that are still referenced by something
    physical_partition_id BIGINT NOT NULL REFERENCES physical_partition(id),
    PRIMARY KEY(table_version_id, physical_partition_id)
);

CREATE TABLE "function" (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    database_id BIGINT NOT NULL REFERENCES database(id) ON DELETE CASCADE,
    name VARCHAR NOT NULL,
    entrypoint VARCHAR NOT NULL,
    language VARCHAR NOT NULL,
    input_types VARCHAR NOT NULL,
    return_type VARCHAR NOT NULL,
    data VARCHAR NOT NULL,
    volatility VARCHAR NOT NULL,
    CONSTRAINT function_name_unique UNIQUE(name, database_id)
);