# ConfigKey Management

> [!NOTE]
> **Verification Status**: Tested and Verified against `rekuiper` engine with live MQTT broker binding on **2026-09-30 18:42:04 UTC**.  
> **Scorecard**: **3 / 3 Methods Fully Verified**:
> - `GET /metadata/sources/yaml/{name}` (list all configKeys for source) - Verified (HTTP 200 OK)
> - `PUT /metadata/sources/{name}/confKeys/{confKey}` (register/upsert configKey) - Verified (HTTP 200 OK)
> - `DELETE /metadata/sources/{name}/confKeys/{confKey}` (delete configKey) - Verified (HTTP 200 OK)
> 
> Dynamic configuration keys registered here are immediately available for binding in stream `CONF_KEY` options under live telemetry flow.

The rekuiper REST API manages configuration keys for sources. You can list, register, update, and delete configuration profiles.

## List All Configuration Keys for a Source

Use this endpoint to retrieve all configuration keys defined for a specific source:

```http
GET http://localhost:9081/metadata/sources/yaml/{name}
```

### Parameters

- `name`: The source connector name. Built-in sources include `mqtt`, `redis`, `memory`, `httppull`, `httppush`, `file`, and `edgex`. Extended sources include `random`, `sql`, `video`, `zmq`, and user-defined plugins.

### Example

Request all configuration keys for the `mqtt` source:

```bash
curl http://localhost:9081/metadata/sources/yaml/mqtt
```

Response sample:

```json
{
    "amd_broker": {
        "insecureSkipVerify": false,
        "protocolVersion": "3.1.1",
        "qos": 1,
        "server": "tcp://122.9.166.75:1883",
        "token": "******",
        "password": "******"
    },
    "default": {
        "qos": 2,
        "server": "tcp://127.0.0.1:1883"
    },
    "demo_conf": {
        "qos": 0,
        "server": "tcp://10.211.55.6:1883"
    }
}
```

> [!NOTE]
> The server masks sensitive fields (such as `password` and `token`) with `******` in response payloads.

## Delete a Configuration Key

Use this endpoint to delete a configuration profile from a source:

```http
DELETE http://localhost:9081/metadata/sources/{name}/confKeys/{confKey}
```

### Parameters

- `name`: The source connector name.
- `confKey`: The configuration key to remove (for example: `demo_conf`).

### Example

Delete the `demo_conf` profile under the `mqtt` source:

```bash
curl -X DELETE http://localhost:9081/metadata/sources/mqtt/confKeys/demo_conf
```

## Register or Update a Configuration Key

Use this endpoint to register or update a configuration profile for a source:

```http
PUT http://localhost:9081/metadata/sources/{name}/confKeys/{confKey}
```

### Parameters

- `name`: The source connector name.
- `confKey`: The configuration key to create or update.

### Example

Register the `demo_conf` profile under the `mqtt` source:

```bash
curl -X PUT http://localhost:9081/metadata/sources/mqtt/confKeys/demo_conf \
  -H "Content-Type: application/json" \
  -d '{
      "qos": 0,
      "server": "tcp://10.211.55.6:1883"
  }'
```
