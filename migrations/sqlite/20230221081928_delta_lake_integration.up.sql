-- Add up migration script here
ALTER TABLE "table" ADD COLUMN legacy BOOLEAN DEFAULT FALSE;
-- The main reason to go with BLOB (which isn't human readable) instead of TEXT is not performance but instead the lack
-- of support for decoding TEXT to uuid::Uuid in SQLite by sqlx: https://github.com/launchbadge/sqlx/issues/1083
-- On the other hand, while decoding TEXT to uuid::fmt::Hyphenated is supported in SQLite it isn't in Postgres, so this is
-- the only approach that works for now.
ALTER TABLE "table" ADD COLUMN uuid BLOB NOT NULL DEFAULT x'00000000000000000000000000000000';

UPDATE "table" SET legacy = TRUE;
