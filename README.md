# SolarNetwork Reading Aggregate Validation Tool

TODO

# Native Image development

The `JsonUtils` class uses reflection that the normal build process does not pick up, so running the
application with the native image agent can be used to generate an appropriate `reflect-config.json` file
manually.

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
