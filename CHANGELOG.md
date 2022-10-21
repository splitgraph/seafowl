# Change Log

<!-- next-header -->

## [Unreleased] - ReleaseDate

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

[unreleased]: https://github.com/splitgraph/seafowl/compare/v0.2.2...HEAD
[0.2.2]: https://github.com/splitgraph/seafowl/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/splitgraph/seafowl/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/splitgraph/seafowl/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/splitgraph/seafowl/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/splitgraph/seafowl/compare/v0.1.0-dev.4...v0.1.0
