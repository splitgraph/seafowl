-- Patch column data types to be the full Arrow field (rather than
-- just the data type) serialization

UPDATE table_column
SET type = json_build_object(
    'name', name,
    'children', '[]'::json,
    'type', type::json,
    'nullable', true
)::text;

UPDATE physical_partition_column
SET type = json_build_object(
    'name', name,
    'children', '[]'::json,
    'type', type::json,
    'nullable', true
)::text;