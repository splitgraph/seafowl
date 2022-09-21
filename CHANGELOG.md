# Change Log

<!-- next-header -->

## [Unreleased] - ReleaseDate

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

[unreleased]: https://github.com/splitgraph/seafowl/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/splitgraph/seafowl/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/splitgraph/seafowl/compare/v0.1.0-dev.4...v0.1.0
