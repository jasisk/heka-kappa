# heka-kappa

A set of lua scripts and a config for heka to monitor kappa instances.

1. Update the config with your info (influxdb host / user / pw / db) in the `HttpInput`.
2. Set your anomaly parameters in the `KappaAggregatorFilter`.
3. Add your slack webhook in the `HttpOutput`.
