# Node Info API

## Endpoint

- Method: `GET`
- URL: `/api/node-info`
- Auth: `Authorization: Bearer <access_token>`

Optional query parameter:

- `include`: comma-separated list of `system`, `network`
  - default: `system,network`
  - examples:
    - `/api/node-info`
    - `/api/node-info?include=system`
    - `/api/node-info?include=network`
    - `/api/node-info?include=system,network`

## Success Response

Status: `200 OK`

```json
{
  "system": {
    "os": "Debian GNU/Linux 12 (bookworm)",
    "kernel": "6.1.0-26-amd64",
    "cpu_usage_percent": 12.5,
    "ram_usage_percent": 34.2,
    "ram_total_mb": 4096,
    "ram_used_mb": 1402
  },
  "network": {
    "interfaces": [
      {
        "interface": "eth0",
        "ip": "192.168.1.10",
        "mac": "aa:bb:cc:dd:ee:01",
        "state": "up"
      },
      {
        "interface": "lo",
        "ip": "127.0.0.1",
        "mac": "00:00:00:00:00:00",
        "state": "up"
      }
    ]
  },
  "timestamp": "2026-06-05T13:52:09.172Z"
}
```

### Fields

`system`:

- `os`: string
- `kernel`: string
- `cpu_usage_percent`: number (0..100)
- `ram_usage_percent`: number (0..100)
- `ram_total_mb`: number (optional)
- `ram_used_mb`: number (optional)

`network.interfaces[]`:

- `interface`: string (e.g. `eth0`, `lo`)
- `ip`: string or `null` (primary IPv4 if available)
- `mac`: string (lowercase MAC, fallback `00:00:00:00:00:00`)
- `state`: string (`up` or `down`)

`timestamp`:

- ISO 8601 UTC timestamp when data snapshot is collected.

## Error Responses

### Invalid token / expired token

Status: `401 Unauthorized`

Framework default body from `flask-jwt-extended` (example):

```json
{
  "msg": "Missing Authorization Header"
}
```

### Invalid include values

Status: `400 Bad Request`

```json
{
  "error": "Invalid include sections",
  "detail": "Allowed values: network,system"
}
```

### Internal error

Status: `500 Internal Server Error`

```json
{
  "error": "Failed to collect node info",
  "detail": "optional debug message"
}
```

## Response Header

- `Content-Type: application/json`
- `X-OpenPLC-Runtime-Version: v4`

## Notes

- Current implementation keeps a lightweight in-memory cache (TTL 1 second) for polling scenarios.
- For Linux environments without `psutil`, `system` falls back to OS and kernel with usage values set to `0.0`, and `network.interfaces` may be empty.
