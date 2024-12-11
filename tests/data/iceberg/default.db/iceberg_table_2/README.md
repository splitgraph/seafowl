This table was generated with the following code:

```python
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType
catalog = load_catalog(
    "s3",
    **{
        "type": "sql",
        "uri": "sqlite:///tests/data/iceberg/iceberg_catalog.db",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "warehouse": "s3://seafowl-test-bucket/test-data/iceberg",
    },
)
schema = Schema(
    NestedField(field_id=1, name='key', field_type=IntegerType(), required=False),
    NestedField(field_id=2, name='value', field_type=StringType(), required=False),
)
catalog.create_namespace_if_not_exists('default')
iceberg_table = catalog.create_table_if_not_exists(identifier='default.iceberg_table_2', schema=schema)
pa_table_data = pa.Table.from_pylist([
    {'key': 1, 'value': 'one'},
    {'key': 2, 'value': 'two'},
    {'key': 3, 'value': 'three'},
    {'key': 4, 'value': 'four'},
], schema=iceberg_table.schema().as_arrow())
iceberg_table.append(df=pa_table_data)
```
