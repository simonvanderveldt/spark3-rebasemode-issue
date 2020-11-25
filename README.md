# Examples for (Py)Spark 3 date/datetime rebase issues

Run `./test-timestamps.sh` to test the timestamp cases. They should fail but don't.

Run `./test-dates.sh` to test the date cases. They fail as expected but don't show different values when using different `datetimeRebaseModeInRead` modes.
