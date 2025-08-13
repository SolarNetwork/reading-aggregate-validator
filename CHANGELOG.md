# SolarNetwork Reading Aggregate Validation Tool changes

This document describes changes in each release of the SolarNetwork Reading Aggregate Validation
Tool.

## [Develop](https://github.com/SolarNetwork/reading-aggregate-validator/tree/develop)

### New features

 * More detailed reporting on task status: completed, partially completed, and never started states
   are reported instead of the general **No validation problems found** message.

### Fixes

 * Use absolute dates in all API calls, to avoid issues with daylight saving time.
 * Fix reading comparison to use reading statistics, not calendar properties. This resulted in
   false-positives when the two measurement styles differed.
 * Better cope with time ranges that split on large gaps in data.
 * When marking one hour/day stale to compensate for a higher-level aggregate mismatch, find the
   first hour of each day with actual data to mark as stale, otherwise marking an hour without any
   data will have no effect and the higher-level aggregate will not be reprocessed.
 
### Other changes

 * Change the default for `--min-days-offset` from 1 to 5 to not follow current events so closely.


## [1.1.0](https://github.com/SolarNetwork/reading-aggregate-validator/tree/1.1.0)

### New features

 * The `--node-id` argument is now optional. If not provided all node IDs that match the given
   `--source-id` values will be included.
 * SolarNetwork HTTP rate-limit errors are now handled by pausing a short amount of time
   before retrying the operation.
 * Add new `--incremental-mark-stale` option, to mark each stream's invalid time ranges
   immediately after its validation completes (from finding all ranges or reaching the change
   limit or running out of time).

### Other changes

 * Better native image support.
 * Tidier and more consistent output formatting.
 * Improve performance of day level hour-by-hour difference comparisons.


## [1.0.0](https://github.com/SolarNetwork/reading-aggregate-validator/tree/1.0.0)

This is the first public release.
