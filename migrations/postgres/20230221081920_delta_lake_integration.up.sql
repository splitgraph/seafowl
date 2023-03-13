-- Add up migration script here
ALTER TABLE "table" ADD COLUMN legacy BOOLEAN DEFAULT FALSE;
ALTER TABLE "table" ADD COLUMN uuid UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000';
UPDATE "table" SET legacy = TRUE;

-- Table for facilitating soft-dropping of tables, via deferring the actual file deletion for later.
CREATE TABLE dropped_table (
    database_name VARCHAR NOT NULL,
    collection_name VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    uuid UUID NOT NULL,
    deletion_status VARCHAR DEFAULT 'PENDING' CHECK ( deletion_status in ('PENDING', 'RETRY', 'FAILED') ),
    drop_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT(now())
);
