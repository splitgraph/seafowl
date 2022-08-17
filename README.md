![Seafowl](./docs/static/logotype.svg)

[Home page](https://seafowl.io) |
[Documentation](https://www.splitgraph.com/docs/seafowl/getting-started/introduction) |
[Nightly builds](https://nightly.link/splitgraph/seafowl/workflows/nightly/main)
[Download](https://github.com/splitgraph/seafowl/releases)

Seafowl is an analytical database designed for modern data-driven Web applications.

## Work in progress

**This repository is an active work in progress. Read on to find out more about our goals for the
initial release or star/watch this repository to stay informed on our progress!**

### Nightly builds

While we do not yet provide official release builds or build instructions, we produce nightly builds
after each merge to `main`. You can find them in GitHub Actions artifacts (only if you're logged in,
see [this issue](https://github.com/actions/upload-artifact/issues/51)) or **via
[nightly.link](https://nightly.link/splitgraph/seafowl/workflows/nightly/main)**:

- [Linux (x86_64-unknown-linux-gnu)](https://nightly.link/splitgraph/seafowl/workflows/nightly/main/seafowl-nightly-x86_64-unknown-linux-gnu.zip)
- [OSX (x86_64-apple-darwin)](https://nightly.link/splitgraph/seafowl/workflows/nightly/main/seafowl-nightly-x86_64-apple-darwin.zip)
- [Windows (x86_64-pc-windows-msvc)](https://nightly.link/splitgraph/seafowl/workflows/nightly/main/seafowl-nightly-x86_64-pc-windows-msvc.zip)

## Initial release roadmap

### Fast analytical queries

Seafowl is built around [Apache DataFusion](https://github.com/apache/arrow-datafusion), a powerful
and extensible SQL query engine that uses Apache Arrow and Apache Parquet.

Besides fast analytical `SELECT` queries, Seafowl also supports writes and `CREATE TABLE AS`
statements, making it easy to ingest your data into it.

Seafowl's architecture is inspired by modern cloud data warehouses like Snowflake and Google
BigQuery, as well as the lessons we learned over the past few years of working on
[Splitgraph](https://www,splitgraph.com/) and [sgr](https://github.com/splitgraph/sgr/):

- **Separation of storage and compute**. You can store Seafowl data in object storage or on a
  persistent volume and spin up Seafowl instances on-demand to satisfy incoming queries.
- **Partition pruning**. Seafowl splits tables into partitions (stored as Parquet files) and indexes
  them to satisfy filter queries without scanning through the whole table.
- **Extensibility**. You can write user-defined-functions (UDFs) in any language that compiles to
  WebAssembly (WASM) and add them to Seafowl.

### Designed for data-driven Web applications

Seafowl lets you provide data for your dashboards and visualizations without building complex API
endpoints, just by executing SQL straight from the user's browser.

Seafowl's query execution endpoint is HTTP cache and CDN-friendly. You can put Seafowl behind a
cache like Varnish or a CDN like Cloudflare and have query results cached globally and delivered to
your users in milliseconds.

### Runnable on serverless providers

We're intending for Seafowl to be runnable on modern serverless platforms like Fly.io, Cloudflare
Workers and Deno Deploy.

## Post-initial release roadmap

There are many features we're planning for Seafowl after the initial release. Where appropriate,
we'll also aim to upstream these changes into DataFusion itself.

### Support for JSON functions and storage

We're planning on adding the JSON datatype to Seafowl, as well as a suite of functions to
manipulate/access JSON data, similar to the
[functions supported by PostgreSQL](https://www.postgresql.org/docs/current/functions-json.html) .

### PostgreSQL-compatible endpoint

This will make Seafowl queryable by existing BI tools like Metabase/Superset/Looker.

### Federated querying

You will be able to add other databases and external data sources to Seafowl, letting it query them
"live".
