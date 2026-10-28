# Windowing

Because streaming data is infinite, systems cannot process entire streams simultaneously. Windowing divides unbounded streams into bounded segments for calculation.

rekuiper supports these built-in window categories:

- **Time Windows**: Partitions streams based on time duration. Supports both processing time and event time.
- **Count Windows**: Partitions streams based on record count.

For complete window syntax and types, refer to [Window Functions](../../sqls/windows.md).

![Tumbling Time Window](/diagrams/windows/tumbling_window.svg)

![Tumbling Count Window](/diagrams/windows/tumbling_count_window.svg)

