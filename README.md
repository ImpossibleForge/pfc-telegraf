# pfc-telegraf

> Telegraf output plugin — compresses metrics and logs to [PFC-JSONL](https://github.com/ImpossibleForge/pfc-jsonl) format on local disk or S3.

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://python.org)
[![PFC-JSONL](https://img.shields.io/badge/pfc--jsonl-green.svg)](https://github.com/ImpossibleForge/pfc-jsonl)

Telegraf collects metrics from hundreds of sources. pfc-telegraf receives those metrics via HTTP, buffers them, and compresses them to `.pfc` — up to **90% smaller than raw JSONL**, queryable by timestamp without full decompression.

```
Telegraf (outputs.http) ──► pfc-telegraf :8767 ──► .pfc + .bidx ──► S3
                                                          │
                                              DuckDB / pfc-gateway query
```

---

## Why pfc-telegraf?

| Pain | Solution |
|---|---|
| Metrics stored as raw InfluxDB line protocol or JSON consume too much S3 | PFC compresses to ~9% of original size |
| S3 Athena / S3 Select costs per scan | pfc-gateway queries only the relevant blocks — no full decompression |
| Need cold metrics queryable by time range | `.bidx` block index enables timestamp filtering without loading the whole file |

---

## Installation

```bash
pip install fastapi uvicorn toml

# Linux x64
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-linux-x64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl

# macOS Apple Silicon
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-macos-arm64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl

# macOS Intel
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-macos-x64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl

# Windows (PowerShell)
Invoke-WebRequest https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-windows-x64.exe `
  -OutFile "$env:LOCALAPPDATA\Microsoft\WindowsApps\pfc_jsonl.exe"
```

---

## Quick Start

**1. Start pfc-telegraf:**

```bash
PFC_OUTPUT_DIR=/var/lib/pfc-telegraf python3 pfc_telegraf.py
# Listening on 0.0.0.0:8767
```

**2. Configure Telegraf** (`/etc/telegraf/telegraf.conf`):

```toml
[[outputs.http]]
  url         = "http://localhost:8767/ingest"
  method      = "POST"
  data_format = "influx"   # InfluxDB line protocol (recommended)
  timeout     = "10s"
```

**3. Restart Telegraf:**

```bash
systemctl restart telegraf
```

Metrics flow: Telegraf → pfc-telegraf → `.pfc` files → query with DuckDB or pfc-gateway.

---

## Configuration

```toml
# config/pfc_telegraf.toml

[server]
host    = "0.0.0.0"
port    = 8767
api_key = ""              # Optional authentication

[buffer]
output_dir = "/var/lib/pfc-telegraf"
prefix     = "telegraf"   # Output filename prefix: telegraf_20260425T100000.pfc
rotate_mb  = 64           # Flush when buffer reaches 64 MB
rotate_sec = 3600         # Flush at least every hour

[pfc]
binary = "/usr/local/bin/pfc_jsonl"

[s3]
enabled = false
bucket  = "my-metrics-archive"
prefix  = "telegraf/"
region  = "us-east-1"
```

Start with config:
```bash
python3 pfc_telegraf.py --config config/pfc_telegraf.toml
```

**Environment variable overrides:**

| Variable | Description |
|---|---|
| `PFC_OUTPUT_DIR` | Output directory for `.pfc` files |
| `PFC_API_KEY` | API key for authentication |
| `PFC_BINARY` | Path to `pfc_jsonl` binary |
| `PFC_S3_BUCKET` | S3 bucket (enables S3 upload) |

---

## Supported Input Formats

pfc-telegraf accepts two Telegraf output formats:

### InfluxDB Line Protocol (`data_format = "influx"`) — recommended

```
cpu,host=server01,region=us-east usage_user=1.5,usage_idle=98.5 1700000000000000000
mem,host=server01 used=2048i,total=8192i 1700000000000000000
```

Stored as flat JSONL:
```json
{"timestamp": "2023-11-14T22:13:20Z", "measurement": "cpu", "host": "server01", "region": "us-east", "usage_user": 1.5, "usage_idle": 98.5}
```

### Telegraf JSON (`data_format = "json"`)

```json
[{"fields": {"usage_user": 1.5}, "name": "cpu", "tags": {"host": "server01"}, "timestamp": 1700000000}]
```

---

## HTTP API

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Health check (no auth required) |
| `/ingest` | POST | Receive metrics (line protocol or JSON) |
| `/ingest/status` | GET | Buffer status and statistics |
| `/ingest/flush` | POST | Force immediate compression + write |

**Authentication** (if `api_key` is set):
```bash
curl -H "x-api-key: your-key" http://localhost:8767/ingest/status
# or
curl -H "Authorization: Bearer your-key" http://localhost:8767/ingest/status
```

---

## Querying the Output

### DuckDB (local)

```sql
INSTALL pfc FROM community;
LOAD pfc;

-- Count all metrics
SELECT COUNT(*) FROM pfc_scan('/var/lib/pfc-telegraf/telegraf_20260425T100000.pfc');

-- Filter by measurement
SELECT * FROM pfc_scan('telegraf_20260425T100000.pfc')
WHERE json_extract_string(line, '$.measurement') = 'cpu'
LIMIT 10;

-- Time range query
SELECT json_extract_string(line, '$.host') AS host,
       AVG(json_extract(line, '$.usage_user')::FLOAT) AS avg_cpu
FROM pfc_scan('telegraf_20260425T100000.pfc')
WHERE json_extract_string(line, '$.timestamp') >= '2026-04-25T10:00:00Z'
GROUP BY host;
```

### pfc-gateway (HTTP)

```bash
curl -X POST http://localhost:8765/query \
  -H "x-api-key: your-key" \
  -H "Content-Type: application/json" \
  -d '{
    "file": "/var/lib/pfc-telegraf/telegraf_20260425T100000.pfc",
    "from_ts": "2026-04-25T10:00:00Z",
    "to_ts":   "2026-04-25T11:00:00Z"
  }'
```

---

## Part of the PFC Ecosystem

**[→ View all PFC tools & integrations](https://github.com/ImpossibleForge/pfc-jsonl#ecosystem)**

| Direct integration | Why |
|---|---|
| [pfc-gateway](https://github.com/ImpossibleForge/pfc-gateway) | Query the archives pfc-telegraf creates — HTTP REST, no DuckDB required |
| [pfc-vector](https://github.com/ImpossibleForge/pfc-vector) | Alternative ingest — high-performance Rust daemon for the same HTTP metrics pipeline |

---

## Disclaimer

pfc-telegraf is an independent open-source project and is not affiliated with, endorsed by, or associated with Telegraf, InfluxData, Inc., or any related projects.

## License

pfc-telegraf (this repository) is released under the MIT License — see [LICENSE](LICENSE).

The PFC-JSONL binary (pfc_jsonl) is proprietary software — free for personal and open-source use. Commercial use requires a license: info@impossibleforge.com
