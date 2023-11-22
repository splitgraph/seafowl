![Seafowl](./docs/static/logotype.svg)

![CI](https://github.com/splitgraph/seafowl/workflows/CI/badge.svg)
[![Docker Pulls](https://img.shields.io/docker/pulls/splitgraph/seafowl)](https://hub.docker.com/r/splitgraph/seafowl)
[![Docker Image Size (latest by date)](https://img.shields.io/docker/image-size/splitgraph/seafowl)](https://hub.docker.com/r/splitgraph/seafowl)
[![GitHub all releases](https://img.shields.io/github/downloads/splitgraph/seafowl/total)](https://github.com/splitgraph/seafowl/releases)
[![GitHub release (latest by date including pre-releases)](https://img.shields.io/github/v/release/splitgraph/seafowl?include_prereleases&sort=semver)](https://github.com/splitgraph/seafowl/releases)

**[Home page](https://seafowl.io) |
[Docs](https://www.splitgraph.com/docs/seafowl/getting-started/introduction) |
[Benchmarks](https://observablehq.com/@seafowl/benchmarks) |
[Demo](https://observablehq.com/@seafowl/interactive-visualization-demo) |
[Nightly builds](https://nightly.link/splitgraph/seafowl/workflows/nightly/main) |
[Download](https://github.com/splitgraph/seafowl/releases)**

Seafowl is an analytical database for modern data-driven Web applications.

Its CDN and HTTP cache-friendly query execution API lets you deliver data to your visualizations,
dashboards and notebooks by running SQL straight from the user's browser.

## Features

### Fast analytics...

Seafowl is built around
[Apache DataFusion](https://arrow.apache.org/datafusion/user-guide/introduction.html), a fast and
extensible query execution framework. It uses [Apache Parquet](https://parquet.apache.org/) columnar
storage, adhering to the [Delta Lake](https://delta.io/) protocol, making it perfect for analytical
workloads.

For `SELECT` queries, Seafowl supports a large subset of the PostgreSQL dialect. If there's
something missing, you can
[write a user-defined function](https://splitgraph.com/docs/seafowl/guides/custom-udf-wasm) for
Seafowl in anything that compiles to WebAssembly.

In addition, you can write data to Seafowl by:

- [uploading a CSV or a Parquet file](https://splitgraph.com/docs/seafowl/guides/uploading-csv-parquet)...
- pointing a table to a local or an
  [externally hosted CSV or a Parquet file](https://seafowl.io/docs/guides/csv-parquet-http-external)...
- pointing a table to a [remote database](https://seafowl.io/docs/guides/remote-tables)...
- or using
  [standard SQL DML statements](https://splitgraph.com/docs/seafowl/guides/writing-sql-queries).

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

## Quickstart

Start Seafowl:

```bash
docker run --rm -p 8080:8080 \
    -e SEAFOWL__FRONTEND__HTTP__WRITE_ACCESS=any \
    splitgraph/seafowl:nightly
```

Or download it from [the releases page](https://github.com/splitgraph/seafowl/releases) and run it
without Docker:

```bash
SEAFOWL__FRONTEND__HTTP__WRITE_ACCESS=any ./seafowl
```

Add a Parquet dataset from HTTP:

```bash
curl -i -H "Content-Type: application/json" localhost:8080/q -d@- <<EOF
{"query": "CREATE EXTERNAL TABLE tripdata \
STORED AS PARQUET \
LOCATION 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet';
CREATE TABLE tripdata AS SELECT * FROM staging.tripdata;
"}
EOF
```

Run a query:

```bash
curl -i -H "Content-Type: application/json" localhost:8080/q \
  -d@-<<EOF
{"query": "SELECT
    EXTRACT(hour FROM tpep_dropoff_datetime) AS hour,
    COUNT(*) AS trips,
    SUM(total_amount) AS total_amount,
    AVG(tip_amount / total_amount) AS tip_fraction
  FROM tripdata
  WHERE total_amount != 0
  GROUP BY 1
  ORDER BY 4 DESC"}
EOF

{"hour":21,"trips":109685,"total_amount":2163599.240000029,"tip_fraction":0.12642660660636984}
{"hour":22,"trips":107252,"total_amount":2154126.55000003,"tip_fraction":0.12631676747865359}
{"hour":19,"trips":159241,"total_amount":3054993.040000063,"tip_fraction":0.1252992155287979}
{"hour":18,"trips":183020,"total_amount":3551738.5100000845,"tip_fraction":0.1248666037263193}
{"hour":20,"trips":122613,"total_amount":2402858.8600000343,"tip_fraction":0.12414978866883832}
{"hour":1,"trips":45485,"total_amount":940333.4000000034,"tip_fraction":0.12336981088023881}
...
```

## CLI

Seafowl also provides a CLI to accommodate frictionless prototyping, troubleshooting and testing of
the core features :

```bash
$ ./seafowl --cli -c /path/to/seafowl.toml
default> CREATE TABLE t
AS VALUES
(1, 'one'),
(2, 'two');
Time: 0.021s
default> SELECT * FROM t;
+---------+---------+
| column1 | column2 |
+---------+---------+
| 1       | one     |
| 2       | two     |
+---------+---------+
Time: 0.009s
default> \d t
+---------------+--------------+------------+-------------+-----------+-------------+
| table_catalog | table_schema | table_name | column_name | data_type | is_nullable |
+---------------+--------------+------------+-------------+-----------+-------------+
| default       | public       | t          | column1     | Int64     | YES         |
| default       | public       | t          | column2     | Utf8      | YES         |
+---------------+--------------+------------+-------------+-----------+-------------+
Time: 0.005s
default> \q
$
```

It does so by circumventing Seafowl's primary HTTP interface, which involves properly formatting
HTTP requests with queries, authentication, as well as dealing with potentially faulty networking
setup, which can sometimes be too tedious for a quick manual interactive session.

## Documentation

See the [documentation](https://www.splitgraph.com/docs/seafowl/getting-started/introduction) for
more guides and examples. This includes a longer
[tutorial](https://www.splitgraph.com/docs/seafowl/getting-started/tutorial-fly-io/introduction),
following which you will:

- Deploy Seafowl to [Fly.io](https://fly.io)
- Put it behind Cloudflare CDN or Varnish
- Build an interactive [Observable](https://observablehq.com) notebook querying data on it, just
  like [this one](https://observablehq.com/@seafowl/interactive-visualization-demo)

## Tests

Please consult the dedicated [README](./tests/README.md) for more info on how to run the Seafowl
test suite locally.

## Pre-built binaries and Docker images

We do not yet provide full build instructions, but we do produce binaries and Docker images as
prebuilt artifacts.

### Release builds

You can find release binaries on our [releases page](https://github.com/splitgraph/seafowl/releases)

### Nightly builds

We produce nightly binaries after every merge to `main`. You can find them in GitHub Actions
artifacts (only if you're logged in, see
[this issue](https://github.com/actions/upload-artifact/issues/51)) or **via
[nightly.link](https://nightly.link/splitgraph/seafowl/workflows/nightly/main)**:

- [Linux (x86_64-unknown-linux-gnu)](https://nightly.link/splitgraph/seafowl/workflows/nightly/main/seafowl-nightly-x86_64-unknown-linux-gnu.zip)
- [OSX (x86_64-apple-darwin)](https://nightly.link/splitgraph/seafowl/workflows/nightly/main/seafowl-nightly-x86_64-apple-darwin.zip)
- [Windows (x86_64-pc-windows-msvc)](https://nightly.link/splitgraph/seafowl/workflows/nightly/main/seafowl-nightly-x86_64-pc-windows-msvc.zip)

### Docker images

We produce [Docker images](https://hub.docker.com/r/splitgraph/seafowl/tags) on every merge to
`main`.

- Release builds are tagged according to their version, e.g. `v0.1.0` results in
  `splitgraph/seafowl:0.1.0` and `0.1`.
- Nightly builds are tagged as `splitgraph/seafowl:nightly`

## Long-term feature roadmap

There are many features we're planning for Seafowl. Where appropriate, we'll also aim to upstream
these changes into DataFusion itself.

### Support for JSON functions and storage

We're planning on adding the JSON datatype to Seafowl, as well as a suite of functions to
manipulate/access JSON data, similar to the
[functions supported by PostgreSQL](https://www.postgresql.org/docs/current/functions-json.html) .

### PostgreSQL-compatible endpoint

This will make Seafowl queryable by existing BI tools like Metabase/Superset/Looker.
