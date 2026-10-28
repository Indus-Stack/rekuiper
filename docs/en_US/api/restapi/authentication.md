# REST API Authentication

rekuiper supports `JWT RSA256` authentication for management REST APIs when enabled.

To enable authentication:
1. Place your RSA public key file in the `etc/mgmt` directory.
2. Sign your JWT tokens using the matching RSA private key.
3. Include the token in HTTP request headers:

```http
Authorization: <JWT_TOKEN>
```

If the token validates successfully, the server processes the request. If validation fails, the server returns HTTP status code `401 Unauthorized`.

## JWT Header

```json
{
  "typ": "JWT",
  "alg": "RS256"
}
```

## JWT Payload Claims

The JWT payload must use the following claim schema:

| Claim | Required | Description |
| :--- | :--- | :--- |
| `iss` | Yes | Issuer. Must match the filename of the public key stored in `etc/mgmt`. |
| `aud` | Yes | Audience. Must be set to `eKuiper`. |
| `exp` | No | Expiration timestamp in Unix epoch seconds. |
| `jti` | No | Unique JWT identifier. |
| `iat` | No | Issuance timestamp in Unix epoch seconds. |
| `nbf` | No | Not-before timestamp in Unix epoch seconds. |
| `sub` | No | Subject identifier. |

Example payload:

```json
{
  "iss": "sample_key.pub",
  "aud": "eKuiper"
}
```

Ensure that the public key file `sample_key.pub` exists in the `etc/mgmt` directory.

## JWT Signature

Sign the token using your RSA private key. rekuiper verifies the signature against the matching public key located in `etc/mgmt`.

## Coexistence with Console Password and Fleet Token

When `basic.authentication: true` is enabled alongside `uiPassword` or `fleetToken`:
- **RS256 JWT Tokens**: Accepted in the `Authorization` header (either raw `<JWT_TOKEN>` or `Bearer <JWT_TOKEN>`).
- **Web Console Password (`uiPassword`)**: Requests carrying `Authorization: Bearer <uiPassword>` are verified using constant-time byte comparison and permitted for Web Console sessions.
- **Fleet Control Plane Token (`fleetToken`)**: Requests to `/fleet/*` or `/api/v1/fleet/*` carrying `Authorization: Bearer <fleetToken>` or `X-Fleet-Token: <fleetToken>` are verified in constant time.

<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 840 190" width="100%" height="auto" role="img" aria-label="REST API Authentication Guard Flow" font-family="-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif">
  <rect x="0" y="0" width="840" height="190" rx="10" fill="#f8fafc" stroke="#e2e8f0" stroke-width="1" />
  <text x="420" y="28" text-anchor="middle" font-size="14" font-weight="700" fill="#0f172a">REST API Authentication Guard (auth_guard)</text>
  <rect x="25" y="55" width="180" height="105" rx="8" fill="#ffffff" stroke="#cbd5e1" stroke-width="1.5" />
  <text x="115" y="82" text-anchor="middle" font-size="12" font-weight="700" fill="#0f172a">Incoming HTTP Request</text>
  <text x="115" y="102" text-anchor="middle" font-size="10" fill="#64748b">Authorization Header</text>
  <text x="115" y="118" text-anchor="middle" font-size="10" fill="#64748b">or X-Fleet-Token</text>
  <text x="115" y="138" text-anchor="middle" font-size="10" fill="#2563eb">Public: /, /ping, /ui, /ui/auth</text>
  <path d="M 205 108 L 255 108" stroke="#64748b" stroke-width="2" />
  <polygon points="255,108 247,104 247,112" fill="#64748b" />
  <rect x="255" y="45" width="310" height="125" rx="8" fill="#ffffff" stroke="#2563eb" stroke-width="1.5" />
  <text x="410" y="70" text-anchor="middle" font-size="12" font-weight="700" fill="#1e40af">Credential Verification Pipeline</text>
  <text x="410" y="92" text-anchor="middle" font-size="11" fill="#334155">1. Fleet Route Check: Constant-Time fleetToken Match</text>
  <text x="410" y="112" text-anchor="middle" font-size="11" fill="#334155">2. Console Bearer Check: Constant-Time uiPassword Match</text>
  <text x="410" y="132" text-anchor="middle" font-size="11" fill="#334155">3. JWT Guard: RS256 Signature &amp; Claims (etc/mgmt/*.pub)</text>
  <text x="410" y="152" text-anchor="middle" font-size="10" fill="#059669">Zero Timing Side-Channels (constant_time_eq)</text>
  <path d="M 565 108 L 615 108" stroke="#64748b" stroke-width="2" />
  <polygon points="615,108 607,104 607,112" fill="#64748b" />
  <rect x="615" y="55" width="200" height="105" rx="8" fill="#ffffff" stroke="#10b981" stroke-width="1.5" />
  <text x="715" y="82" text-anchor="middle" font-size="12" font-weight="700" fill="#047857">Protected Route Handler</text>
  <text x="715" y="104" text-anchor="middle" font-size="11" fill="#334155">Valid: 200 OK / Route Execution</text>
  <text x="715" y="126" text-anchor="middle" font-size="11" fill="#dc2626">Invalid: 401 Unauthorized</text>
</svg>
