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
connection string by:

1. Using the DataFusion `CREATE EXTERNAL TABLE` command after registering the corresponding factory:

   ```rust
   use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
   use datafusion_remote_tables::factory::RemoteTableFactory;

   let mut table_factories: HashMap<String, Arc<dyn TableProviderFactory>> =
       HashMap::new();
   table_factories.insert("TABLE".to_string(), Arc::new(RemoteTableFactory {}));

   let mut runtime_env = RuntimeEnv::new(RuntimeConfig::new())?;
   runtime_env.register_table_factories(table_factories);
   ```

   ```sql
   CREATE EXTERNAL TABLE seafowl_issue_reactions [column schema definition]
   STORED AS TABLE
   OPTIONS ('name' '\"splitgraph-demo/seafowl:latest\".issue_reactions')
   LOCATION 'postgresql://$SPLITGRAPH_API_KEY:$SPLITGRAPH_API_SECRET@data.splitgraph.com:5432/ddn'
   ```

2. Alternatively, you need to instantiate the table manually, and register it with DataFusion
   (either in a `SchemaProvider` or `SessionContext`):

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
   schema_provider.register_table(
       "seafowl_issue_reactions".to_string(),
       Arc::new(remote_table),
   );
   ...
   ```

The optional column schema definition can alsso be supplied; if the schema is empty the schema will
be inferred upon instantiation, otherwise the necessary casting of any column whose type doesn't
match the specified one will be performed on each query.

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
