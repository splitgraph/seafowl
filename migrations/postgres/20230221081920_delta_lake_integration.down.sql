-- Add down migration script here
ALTER TABLE "table" DROP COLUMN legacy;
ALTER TABLE "table" DROP COLUMN uuid;

DROP TABLE dropped_table;
