## DataFusion remote table querying mechanism

This crate provides support for creating DataFusion tables that proxy all the queries to an external
table stored in a remote data source, in analogy with Postgres FDWs.

It does so by implementing the TableProvider trait, whereby a SQL query corresponding to the remote
data source is re-constructed from the provided primitives (projections, filters, etc.), This query
is then executed on the remote data source using the
[connector-x](https://docs.rs/connectorx/latest/connectorx/) library.

## Usage

To create a remote table one needs to provide the exact name of the table on the remote data source
(escaping the identifier in the remote-compatible fashion if needed), and the corresponding
connection string.

In addition, an optional schema reference can be supplied; if the schema is empty the schema will be
inferred upon instantiation, otherwise the necessary casting of any column whose type doesn't match
the specified one will be performed on each query.

```rust
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion_remote_tables::provider::RemoteTable;

let origin_table_name = "\"splitgraph-demo/seafowl:latest\".issue_reactions";
let conn_str = "postgresql://$SPLITGRAPH_API_KEY:$SPLITGRAPH_API_SECRET@data.splitgraph.com:5432/ddn";
let remote_table = RemoteTable::new(
    origin_table_name.to_string(),
    conn_str.to_string(),
    SchemaRef::from(Schema::empty()),
)
.await?;
```

Next, you need to register the table in the DataFusion (either in a `SchemaProvider` or
`SessionContext`) using a table name that will be used for querying, for instance:

```rust
...
schema_provider.register_table(
    "seafowl_issue_reactions".to_string(),
    Arc::new(remote_table),
);
...
```

Finally, you can query the table:

```sql
SELECT content FROM my_schema.seafowl_issue_reactions
WHERE issue_number = 122 OR created_at >= '2022-10-26 19:18:10' LIMIT 2;
+---------+
| content |
|---------|
| hooray  |
| heart   |
+---------+
```

## Filter pushdown

Besides projection pushdown (i.e. scoping down the required columns for each particular query), the
remote table mechanism also supports filter pushdown. This pushdown mechanism aims to convert the
`WHERE` (and `LIMIT` if any) clause to the corresponding remote SQL string. It does so by walking
the filter expression AST, trying to determine whether each node in the tree is shippable and if so
convert it to a SQL form native to the remote data source.

This can provide significant performance increase, as it can drastically reduce the total amount of
data that has to be transferred over the network. When a filter is not shippable, it is kept in the
DataFusion plan so that the appropriate filtration can be performed locally after fetching the
result rows.
