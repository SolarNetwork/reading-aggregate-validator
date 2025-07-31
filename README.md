# SolarNetwork Reading Aggregate Validation Tool

This project contains a command line (CLI) tool for analyzing SolarNetwork cached aggregate reading
values, comparing them to on-the-fly reading calculations to look for differences. If differences
are found, the tool generates a detailed report and can even submit "reprocessing" requests to
SolarNetwork, to get the differences eliminated.

Normally there should not be any differences between the raw datum stream data and the cached
aggregates SolarNetwork automatically maintains. However, over time anomalies can creep in in
inadvertently, making different views on the same data appear to be inconsistent.

<img alt="Screen shot of tool in action" src="docs/img/sn-reading-aggregate-validator-output@2x.png" width="1011">

# How it works 

The tool works by querying datum streams (unique node + source ID combinations) for a `Difference`
style reading over all available time in the stream. It then compares that value to an _aggregate
rollup_ `Difference` reading for the same time period. The results should be identical. If not, the
tool splits the time range in half and repeats the process on each half, in order to narrow down the
precise time range the difference occurs in. For each split time range with a difference, it keeps
going, splitting the time range in half again, until it reaches the specific **hours** where
differences occur.

Once the specific inconsistent hours have been identified, the tool produces a spreadsheet report
that details what it found:

<img alt="Screen shot of an output report spreadsheet" src="docs/img/sn-reading-aggregate-validator-csv-report@2x.png" width="678">

Additionally, the tool prints out SolarUser API calls that can be used to mark the inconsistent
hours for "reprocessing". Once reprocessing has been completed, the inconsistencies should be
resolved. Alternatively the tool can submit the reprocessing requests directly for you.

# Options

Run the tool with `-h` or `--help` to display all the available options.

# Native Image development

The `JsonUtils` class uses reflection that the normal build process does not pick up, so running the
application with the native image agent can be used to generate an appropriate `reflect-config.json`
file manually.

Run an example use case, like this:

```sh
java -Dspring.aot.enabled=true \
  -agentlib:native-image-agent=config-output-dir=/tmp -jar build/libs/sn-reading-aggregate-validator-1.0.0.jar \
  --token=xyz \
  --secret \
  --node-id=123 \
  --source-id='/*/*/*/GEN/*' \
  --property=wattHours \
  --max-invalid=20 \
  --report-file=/var/tmp/sn-invalid-agg-report.csv
```
