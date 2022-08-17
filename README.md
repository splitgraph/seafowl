![Seafowl](./docs/static/logotype.svg)

[Home page](https://seafowl.io) |
[Documentation](https://www.splitgraph.com/docs/seafowl/getting-started/introduction) |
[Nightly builds](https://nightly.link/splitgraph/seafowl/workflows/nightly/main) |
[Download](https://github.com/splitgraph/seafowl/releases)

Seafowl is an analytical database for modern data-driven Web applications.

It lets you deliver data to your visualizations, dashboards and notebooks by running SQL straight
from the user's browser, without having to write custom API endpoints.

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

### Fast analytics...

Seafowl is built around
[Apache DataFusion](https://arrow.apache.org/datafusion/user-guide/introduction.html), a fast and
extensible query execution framework. It uses [Apache Parquet](https://parquet.apache.org/) columnar
storage, making it perfect for analytical workloads.

For `SELECT` queries, Seafowl supports a large subset of the PostgreSQL dialect. If there's
something missing, you can
[write a user-defined function](https://splitgraph.com/docs/seafowl/guides/custom-udf-wasi) for
Seafowl in anything that compiles to WebAssembly.

In addition, you can write data to Seafowl by
[uploading a CSV or a Parquet file](https://splitgraph.com/docs/seafowl/guides/uploading-csv-parquet),
creating an external table or using
[standard SQL statements](https://splitgraph.com/docs/seafowl/guides/writing-sql-queries).

### ...at the edge

Seafowl is designed to be deployed to modern serverless environments. It ships as a single binary,
making it simple to run anywhere.

Seafowl's architecture is inspired by modern cloud data warehouses like Snowflake or BigQuery,
letting you separate storage and compute. You can store Seafowl data in an object storage like S3 or
Minio and scale to zero. Or, you can
[build a self-contained Docker image](https://splitgraph.com/docs/seafowl/guides/baking-dataset-docker-image)
with Seafowl and your data, letting you deploy your data to any platform that supports Docker.

Seafowl's query execution API follows HTTP cache semantics. This means you can
[put Seafowl behind a CDN](https://splitgraph.com/docs/seafowl/guides/querying-cache-cdn) like
Cloudflare or a cache like Varnish and have query results cached and delivered to your users in
milliseconds. Even without a cache, you can get the benefits of caching query results in your user's
browser.

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
