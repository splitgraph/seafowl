UPDATE table_column
SET type = type->>'type';

UPDATE physical_partition_column
SET type = type->>'type';