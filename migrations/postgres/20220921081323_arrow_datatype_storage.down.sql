UPDATE table_column
SET type = (type::json)->>'type';

UPDATE physical_partition_column
SET type = (type::json)->>'type';