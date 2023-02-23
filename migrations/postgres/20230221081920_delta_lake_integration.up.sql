-- Add up migration script here
ALTER TABLE "table" ADD COLUMN legacy BOOLEAN DEFAULT FALSE;

UPDATE "table" SET legacy = TRUE;
