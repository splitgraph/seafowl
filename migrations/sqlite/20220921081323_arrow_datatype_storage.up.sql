-- Patch column data types to be the full Arrow field (rather than
-- just the data type) serialization

UPDATE table_column
SET type = json_object(
    'name', name,
    'children', json('[]'),
    'type', json(type),
    'nullable', json('true')
);

UPDATE physical_partition_column
SET type = json_object(
    'name', name,
    'children', json('[]'),
    'type', json(type),
    'nullable', json('true')
);
