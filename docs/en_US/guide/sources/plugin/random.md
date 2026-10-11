# Random Source Connector

In `rekuiper`, the legacy `random` source connector is natively supported as a backward-compatible alias to the built-in [Simulator Source Connector](../builtin/simulator.md).

Streams created with `TYPE = "random"` run directly on the high-performance native generator without requiring legacy Go C-shared dynamic plugins.

## Stream Definition

Define a random mock stream using standard SQL DDL:

```sql
CREATE STREAM mock_stream () WITH (
  TYPE = "random",
  CONF_KEY = "demo"
);
```

If `CONF_KEY` is omitted, the stream defaults to the `default` configuration key.

## Configuration

Configurations can be supplied either under `random/<key>` or `simulator/<key>`:

```yaml
demo:
  data:
    - temperature: 22.5
      humidity: 50.2
    - temperature: 25.1
      humidity: 48.9
  interval: 1000
  loop: true
```

For full configuration properties, see the [Simulator Source Connector](../builtin/simulator.md) guide.
