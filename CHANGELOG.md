# Changelog — pfc-telegraf

## [0.1.0] — 2026-04-25

### Added
- HTTP ingest server (port 8767, configurable)
- InfluxDB line protocol parser — native Telegraf format support
- Telegraf JSON output format support
- Buffer management with size-based (MB) and time-based rotation
- Compression via `pfc_jsonl` binary → `.pfc` + `.bidx` + `.idx`
- Optional S3 upload after compression
- `GET /health`, `GET /ingest/status`, `POST /ingest`, `POST /ingest/flush` endpoints
- API key authentication (x-api-key header or Bearer token)
- TOML configuration file support with environment variable overrides
- Watchdog for automatic time-based rotation
- Graceful shutdown (SIGTERM/SIGINT) with buffer flush
- Example configs: `config/pfc_telegraf.toml`, `config/telegraf.conf`
