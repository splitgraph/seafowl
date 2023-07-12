# Change Log

<!-- next-header -->

## [Unreleased] - ReleaseDate

## [0.5.0] - 2023-07-12

- Upgrade to DataFusion 27 and Arrow 42 (<https://github.com/splitgraph/seafowl/pull/453>)
- Load schemas without tables and double-check missing DB ids in a given context
  (<https://github.com/splitgraph/seafowl/pull/446>)
- Implement schema coercion and file streaming in the upload endpoint
  (<https://github.com/splitgraph/seafowl/pull/439>)
- Return timing data in response headers (<https://github.com/splitgraph/seafowl/pull/438>)

## [0.4.3] - 2023-06-07

- Enable creating external tables on cloud object stores
  (<https://github.com/splitgraph/seafowl/pull/433>)
- Implement streaming of result rows (<https://github.com/splitgraph/seafowl/pull/424>)

## [0.4.2] - 2023-05-26

- Enable specifying the GET endpoint cache-control header via config
  (<https://github.com/splitgraph/seafowl/pull/422>)
- Upgrade to DataFusion 25 and Arrow 39 (<https://github.com/splitgraph/seafowl/pull/415>)

## [0.4.1] - 2023-05-17

- Upgrade to DataFusion 24 and Arrow 38 (<https://github.com/splitgraph/seafowl/pull/403>)
- Enable passing query as a path parameter in the GET endpoint
  (<https://github.com/splitgraph/seafowl/pull/402>)

## [0.4.0] - 2023-05-11

- Remove legacy table reading logic (<https://github.com/splitgraph/seafowl/pull/390>)
- Upgrade DataFusion to 23 and Arrow to 37 (<https://github.com/splitgraph/seafowl/pull/386>)
- Add support for GCS bucket object stores (<https://github.com/splitgraph/seafowl/pull/379>)

## [0.3.4] - 2023-04-27

- Return result type info in the `content-type` header, and bump `object_store` crate
  (<https://github.com/splitgraph/seafowl/pull/367>)

## [0.3.3] - 2023-04-20

- Fix table renaming to quoted identifiers with special characters
  (<https://github.com/splitgraph/seafowl/pull/362>)
- Upgrade DataFusion to v22.0.0 and Arrow to v36.0.0
  (<https://github.com/splitgraph/seafowl/pull/360>)

## [0.3.2] - 2023-04-04

- Enable caching of S3 objects in the local FS (<https://github.com/splitgraph/seafowl/pull/341>)
- Fix the silent panic in the cache eviction hook rendering it useless
  (<https://github.com/splitgraph/seafowl/pull/334>)

## [0.3.1] - 2023-03-29

- Fix unimplemented method in AWS object store, and broaden integration test coverage by extending
  target object stores to include a MinIO instance
  (<https://github.com/splitgraph/seafowl/pull/331>)
- Enable cached GET endpoint for authorized requests, and enable dropping of external tables
  (<https://github.com/splitgraph/seafowl/pull/326>)

## [0.3.0] - 2023-03-22

- Migration of storage layer to delta-rs (<https://github.com/splitgraph/seafowl/pull/307>)
- Purge legacy logic (<https://github.com/splitgraph/seafowl/pull/313>)
- Deltify parquet writing logic (<https://github.com/splitgraph/seafowl/pull/316>)
- Implement `VACUUM DATABASE` command (<https://github.com/splitgraph/seafowl/pull/320>)
- Enable `VACUUM` for Delta tables (<https://github.com/splitgraph/seafowl/pull/322>)

## [0.2.12] - 2023-02-14

- Upgrade to DataFusion 18.0.0 (<https://github.com/splitgraph/seafowl/pull/297>)
- Implement multi-database support for upload endpoint
  (<https://github.com/splitgraph/seafowl/pull/291>)
- Implement multi-database support for GET and POST query endpoints
  (<https://github.com/splitgraph/seafowl/pull/289>)

## [0.2.11] - 2023-01-27

- Upgrade to DataFusion post-16.0 (<https://github.com/splitgraph/seafowl/pull/270>)
- Add an `ssl_cert_file` option to the config (<https://github.com/splitgraph/seafowl/pull/278>)
- Migrate to DataFusion's native DML nodes for logical write statement planning
  (<https://github.com/splitgraph/seafowl/pull/281>)

## [0.2.10] - 2022-12-30

- Add region config parameter for Amazon S3 object store
  (<https://github.com/splitgraph/seafowl/pull/255>)
- Enable querying external Delta tables in Seafowl
  (<https://github.com/splitgraph/seafowl/pull/252>)
- Implement remote table factory (<https://github.com/splitgraph/seafowl/pull/250>)

## [0.2.9] - 2022-12-23

- Add support for pushdown in remote tables:
  - `LIMIT` (<https://github.com/splitgraph/seafowl/pull/221>)
  - `WHERE` (<https://github.com/splitgraph/seafowl/pull/226>,
    <https://github.com/splitgraph/seafowl/pull/235>)
- Factor out remote tables into a separate crate (<https://github.com/splitgraph/seafowl/pull/238>)
- Upgrade to DataFusion 15 (<https://github.com/splitgraph/seafowl/pull/248>)

## [0.2.8] - 2022-11-21

- Implement `table_partitions` system table (<https://github.com/splitgraph/seafowl/pull/214>)
- Add WASI + MessagePack UDF language variant (<https://github.com/splitgraph/seafowl/pull/149>)

## [0.2.7] - 2022-11-17

- Import JSON values as strings in `CREATE EXTERNAL TABLE`
  (<https://github.com/splitgraph/seafowl/pull/208>)
- Add support for SQLite in `CREATE EXTERNAL TABLE`
  (<https://github.com/splitgraph/seafowl/pull/200>)

## [0.2.6] - 2022-11-08

- Update to DataFusion 14 / Arrow 26 (<https://github.com/splitgraph/seafowl/pull/198>)
- Bugfix: `VACUUM` with shared partitions (<https://github.com/splitgraph/seafowl/pull/191>)
- Bugfix: `DELETE` with certain filters that cover a whole partition
  (<https://github.com/splitgraph/seafowl/pull/192>)
- Initial support for other databases in `CREATE EXTERNAL TABLE`
  (<https://github.com/splitgraph/seafowl/pull/190>)
  - More documentation pending. Example:
    `CREATE EXTERNAL TABLE t STORED AS TABLE 'public.t' LOCATION 'postgresql://uname:pw@host:port/dbname'`

## [0.2.5] - 2022-11-02

- Upgrade to DataFusion 13 (784f10bb) / Arrow 25.0.0
  (<https://github.com/splitgraph/seafowl/pull/176>)
- Use ZSTD compression in Parquet files (<https://github.com/splitgraph/seafowl/pull/182>)
- Fix HTTP external tables using pre-signed S3 URLs
  (<https://github.com/splitgraph/seafowl/pull/183>)
- Fix `INSERT INTO .. SELECT FROM` (<https://github.com/splitgraph/seafowl/pull/184>)
- Fix some `OUTER JOIN` issues by using a minimum of 2 `target_partition`s
  (<https://github.com/splitgraph/seafowl/pull/189>)

## [0.2.4] - 2022-10-25

- Add `system.table_versions` table for inspecting table history
  (<https://github.com/splitgraph/seafowl/pull/168>)
- Add SQLite `catalog.read_only` option for compatibility with LiteFS replicas
  (<https://github.com/splitgraph/seafowl/pull/171>)

## [0.2.3] - 2022-10-21

- Add support for time travel queries (`SELECT * FROM table('2022-01-01T00:00:00')`)
  (<https://github.com/splitgraph/seafowl/pull/154>)
- Allow overriding SQLite journal mode (<https://github.com/splitgraph/seafowl/pull/158>)

## [0.2.2] - 2022-10-12

- Allow using SQL types in WASM UDF definitions (<https://github.com/splitgraph/seafowl/pull/147>)

## [0.2.1] - 2022-09-30

- Cached GET API now accepts URL-encoded query text in X-Seafowl-Header
  (<https://github.com/splitgraph/seafowl/pull/122>)
- Add support for `DELETE` statements (<https://github.com/splitgraph/seafowl/pull/121>)
- Add support for `UPDATE` statements (<https://github.com/splitgraph/seafowl/pull/127>)

## [0.2.0] - 2022-09-21

**Breaking**: Previous versions of Seafowl won't be able to read data written by this version.

- Fix storage of nullable / list columns (<https://github.com/splitgraph/seafowl/pull/119>)
- Default columns in `CREATE TABLE` to nullable (<https://github.com/splitgraph/seafowl/pull/119>)

## [0.1.1] - 2022-09-16

- Upgrade to DataFusion 12 (<https://github.com/splitgraph/seafowl/pull/113>)

## [0.1.0] - 2022-09-12

### Fixes

- Use multi-part uploads to fix the memory usage issue when uploading data to S3
  (<https://github.com/splitgraph/seafowl/pull/99>)

<!-- next-url -->

[unreleased]: https://github.com/splitgraph/seafowl/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/splitgraph/seafowl/compare/v0.4.3...v0.5.0
[0.4.3]: https://github.com/splitgraph/seafowl/compare/v0.4.2...v0.4.3
[0.4.2]: https://github.com/splitgraph/seafowl/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/splitgraph/seafowl/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/splitgraph/seafowl/compare/v0.3.4...v0.4.0
[0.3.4]: https://github.com/splitgraph/seafowl/compare/v0.3.3...v0.3.4
[0.3.3]: https://github.com/splitgraph/seafowl/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/splitgraph/seafowl/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/splitgraph/seafowl/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/splitgraph/seafowl/compare/v0.2.12...v0.3.0
[0.2.12]: https://github.com/splitgraph/seafowl/compare/v0.2.11...v0.2.12
[0.2.11]: https://github.com/splitgraph/seafowl/compare/v0.2.10...v0.2.11
[0.2.10]: https://github.com/splitgraph/seafowl/compare/v0.2.10...v0.2.10
[0.2.10]: https://github.com/splitgraph/seafowl/compare/v0.2.9...v0.2.10
[0.2.9]: https://github.com/splitgraph/seafowl/compare/v0.2.8...v0.2.9
[0.2.8]: https://github.com/splitgraph/seafowl/compare/v0.2.7...v0.2.8
[0.2.7]: https://github.com/splitgraph/seafowl/compare/v0.2.6...v0.2.7
[0.2.6]: https://github.com/splitgraph/seafowl/compare/v0.2.5...v0.2.6
[0.2.5]: https://github.com/splitgraph/seafowl/compare/v0.2.4...v0.2.5
[0.2.4]: https://github.com/splitgraph/seafowl/compare/v0.2.3...v0.2.4
[0.2.3]: https://github.com/splitgraph/seafowl/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/splitgraph/seafowl/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/splitgraph/seafowl/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/splitgraph/seafowl/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/splitgraph/seafowl/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/splitgraph/seafowl/compare/v0.1.0-dev.4...v0.1.0
