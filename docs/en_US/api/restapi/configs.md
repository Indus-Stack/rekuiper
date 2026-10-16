# Dynamic Configuration Management

The rekuiper REST API supports dynamic runtime updates to [global configurations](../../configuration/global_configurations.md) without restarting the server daemon.

## Inspect Configurations

Use this endpoint to retrieve the current basic server configuration:

```http
GET http://localhost:9081/configs
```

> Note: `GET http://localhost:9081/config` is also supported as an alias.

Response sample (HTTP 200 OK):

```json
{
  "basic": {
    "debug": false,
    "logLevel": "info",
    "port": 20498,
    "restPort": 9081
  }
}
```

## Reload Basic Configurations

Use this endpoint to update logging and time zone configurations dynamically:

```http
PATCH http://localhost:9081/configs
Content-Type: application/json
```

Request payload:

```json
{
  "debug": true,
  "consoleLog": true,
  "fileLog": true,
  "timezone": "UTC"
}
```

### Supported Dynamically Reloadable Parameters

- `debug`: Enables or disables debug-level logging.
- `consoleLog`: Enables or disables stdout console logging.
- `fileLog`: Enables or disables file-based logging.
- `timezone`: Sets the system timezone for time calculations.

## Shutdown rekuiper

Use this endpoint to stop the rekuiper server process gracefully:

```http
POST http://localhost:9081/stop
```
