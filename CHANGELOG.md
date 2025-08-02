# SolarNetwork Reading Aggregate Validation Tool changes

This document describes changes in each release of the SolarNetwork Reading Aggregate Validation
Tool.

## [Develop](https://github.com/SolarNetwork/reading-aggregate-validator/tree/develop)

### New features

 * The `--node-id` argument is now optional. If not provided all node IDs that match the given
   `--source-id` values will be included.
 * SolarNetwork HTTP rate-limit errors are now handled by pausing a short amount of time
   before retrying the operation.

### Other changes

 * Better native image support.
 * Tidier and more consistent output formatting.

## [1.0.0](https://github.com/SolarNetwork/reading-aggregate-validator/tree/1.0.0)

This is the first public release.
