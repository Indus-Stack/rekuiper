# REST API Reference

rekuiper provides a comprehensive REST API to manage streams, tables, rules, and system extensions.

By default, the REST API listens on HTTP port `9081`. You can configure the port number using the `restPort` property in `etc/kuiper.yaml`.

::: tip Interactive OpenAPI & Web Management Console
Explore the public [Interactive OpenAPI / Swagger UI](../swagger.md) directly in your browser:
- **Public Swagger Explorer**: Browse and test schemas online via [Interactive OpenAPI / Swagger UI](../swagger.md).
- **Embedded Node OpenAPI 3.0 (`/api-docs`)**: Test endpoints, execute dry-run SQL validations, and inspect JSON contracts on your running node on port `9081`.
- **Web Management Console (`/ui`)**: Monitor fleet throughput, inspect active rule pipelines, and review Dead-Letter Queue (DLQ) poison pills.

See the complete guide and walkthrough in [Management with the Interactive OpenAPI / Swagger UI](../../getting_started/getting_started.md#5-method-c-management-with-the-interactive-openapi-swagger-ui).
:::

## System Information

Use this endpoint to retrieve the version string, host operating system, and process uptime.

```http
GET http://localhost:9081/
```

Example response:

```json
{
  "version": "1.0.1-22-g119ee91",
  "os": "darwin",
  "upTimeSeconds": 14
}
```

## Ping Endpoint

Use this endpoint to verify that the REST server is responsive:

```http
GET http://localhost:9081/ping
```

## Batch Requests

Use this endpoint to execute multiple REST requests sequentially in a single HTTP call:

```http
POST http://localhost:9081/batch/req
```

Request payload:

```json
[
    {
        "method": "POST",
        "path": "/streams",
        "body": "{\"sql\":\"CREATE stream demobatch() WITH (DATASOURCE=\\\"/data1\\\", TYPE=\\\"websocket\\\")\"}"
    },
    {
        "method": "GET",
        "path": "/streams/demobatch"
    }
]
```

Response payload:

```json
[
    {
        "code": 201,
        "response": "Stream demobatch is created."
    },
    {
        "code": 200,
        "response": "{\"Name\":\"demobatch\",\"StreamFields\":null,\"Options\":{\"datasource\":\"/data1\",\"type\":\"websocket\"},\"StreamType\":0,\"Statement\":null}"
    }
]
```

## REST API Sections

- [Authentication](authentication.md)
- [Streams](streams.md)
- [Tables](tables.md)
- [Rules](rules.md)
- [Rule Testing](ruletest.md)
- [Rulesets](ruleset.md)
- [Data Import and Export](data.md)
- [Plugins](plugins.md)
- [Schemas](schemas.md)
- [Services](services.md)
- [User-Defined Functions](udf.md)
- [File Uploads](uploads.md)
- [Configuration Management](configs.md)
- [Configuration Keys](configKey.md)
- [Connections](connection.md)
- [Tracing](trace.md)

## HTTP Standards Compliance & Status Codes

> [!NOTE]
> **Compatibility Note: HTTP Status Codes**
> In legacy eKuiper, querying or deleting non-existent resources (such as `GET /connections/{missing}` or `GET /schemas/{missing}`) returns `400 Bad Request`. `rekuiper` strictly complies with RFC 9110 HTTP semantics by returning `404 Not Found` for missing resources and reserving `400 Bad Request` strictly for malformed syntax or unparseable payloads.
> 
> **Why we chose this difference**: Conflating missing resources with malformed client payloads violates HTTP standards and breaks standard client libraries (which handle 404 with retry or create-if-missing logic), OpenAPI client code generators, caching reverse proxies, and automated REST testing frameworks. Returning `404 Not Found` provides clear, deterministic REST behavior.

