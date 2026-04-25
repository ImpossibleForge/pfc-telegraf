#!/usr/bin/env python3
"""
pfc-telegraf v0.1.0
Telegraf output plugin — receives Telegraf metrics/logs via HTTP and compresses to PFC format.

Accepts:
  POST /ingest   InfluxDB line protocol (Content-Type: text/plain)
                 Telegraf JSON output   (Content-Type: application/json)
  GET  /health
  GET  /ingest/status
  POST /ingest/flush

Configure Telegraf:
  [[outputs.http]]
    url         = "http://localhost:8767/ingest"
    method      = "POST"
    data_format = "influx"          # line protocol (recommended)
    # data_format = "json"          # Telegraf JSON (also supported)
"""

import asyncio
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import toml
import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [pfc-telegraf] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("pfc-telegraf")

VERSION = "0.1.0"

# ─── Config ───────────────────────────────────────────────────────────────────

DEFAULT_CONFIG = {
    "server": {
        "host": "0.0.0.0",
        "port": 8767,
        "api_key": "",
    },
    "buffer": {
        "rotate_mb": 64,
        "rotate_sec": 3600,
        "output_dir": "/tmp/pfc-telegraf",
        "prefix": "telegraf",
    },
    "pfc": {
        "binary": "/usr/local/bin/pfc_jsonl",
    },
    "s3": {
        "enabled": False,
        "bucket": "",
        "prefix": "telegraf/",
        "region": "us-east-1",
    },
}


def deep_merge(base: dict, override: dict) -> dict:
    result = base.copy()
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def load_config(path: Optional[str] = None) -> dict:
    cfg = DEFAULT_CONFIG.copy()
    if path and Path(path).exists():
        with open(path) as f:
            user_cfg = toml.load(f)
        cfg = deep_merge(cfg, user_cfg)
    # Env overrides
    if v := os.environ.get("PFC_OUTPUT_DIR"):
        cfg["buffer"]["output_dir"] = v
    if v := os.environ.get("PFC_API_KEY"):
        cfg["server"]["api_key"] = v
    if v := os.environ.get("PFC_BINARY"):
        cfg["pfc"]["binary"] = v
    if v := os.environ.get("PFC_S3_BUCKET"):
        cfg["s3"]["bucket"] = v
        cfg["s3"]["enabled"] = True
    return cfg


# ─── InfluxDB Line Protocol Parser ────────────────────────────────────────────

def _parse_field_value(raw: str):
    """Parse a single field value from InfluxDB line protocol."""
    raw = raw.strip()
    if not raw:
        return None
    # Integer: ends with 'i'
    if raw.endswith("i") and raw[:-1].lstrip("-").isdigit():
        return int(raw[:-1])
    # Float
    try:
        return float(raw)
    except ValueError:
        pass
    # Boolean
    if raw.lower() in ("true", "t", "True"):
        return True
    if raw.lower() in ("false", "f", "False"):
        return False
    # String: remove surrounding quotes
    if raw.startswith('"') and raw.endswith('"'):
        return raw[1:-1].replace('\\"', '"')
    return raw


def _split_csv(s: str) -> list[str]:
    """Split comma-separated values respecting quoted strings."""
    parts, current, in_quotes = [], [], False
    for ch in s:
        if ch == '"':
            in_quotes = not in_quotes
            current.append(ch)
        elif ch == "," and not in_quotes:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current))
    return parts


def _split_sections(line: str) -> list[str]:
    """Split line protocol line on unescaped spaces, respecting quoted strings."""
    parts, current, in_quotes = [], [], False
    for ch in line:
        if ch == '"':
            in_quotes = not in_quotes
            current.append(ch)
        elif ch == " " and not in_quotes:
            if current:
                parts.append("".join(current))
                current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current))
    return parts


def parse_line_protocol(line: str) -> Optional[dict]:
    """
    Parse one InfluxDB line protocol line to a flat dict.
    Returns None for comment lines, empty lines, or parse errors.

    Format: measurement[,tag=val...] field=val[,field=val...] [nanosec_timestamp]
    """
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    # Split on unescaped spaces (respects quoted string values)
    parts = _split_sections(line)
    if len(parts) < 2:
        return None

    meas_tags_raw = parts[0]
    fields_raw = parts[1]
    ts_raw = parts[2] if len(parts) >= 3 else None

    # Measurement + tags
    meas_tag_parts = meas_tags_raw.split(",", 1)
    measurement = meas_tag_parts[0].replace("\\,", ",")
    row: dict = {"measurement": measurement}

    if len(meas_tag_parts) > 1:
        for tag_pair in _split_csv(meas_tag_parts[1]):
            if "=" in tag_pair:
                k, v = tag_pair.split("=", 1)
                row[k.strip()] = v.strip()

    # Fields
    for field_pair in _split_csv(fields_raw):
        if "=" in field_pair:
            k, v = field_pair.split("=", 1)
            parsed = _parse_field_value(v)
            if parsed is not None:
                row[k.strip()] = parsed

    # Timestamp: nanoseconds → ISO8601 UTC
    if ts_raw and ts_raw.lstrip("-").isdigit():
        ns = int(ts_raw)
        sec = ns / 1_000_000_000
        dt = datetime.fromtimestamp(sec, tz=timezone.utc)
        row["timestamp"] = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        row["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return row


def parse_telegraf_json(data) -> list[dict]:
    """
    Parse Telegraf JSON output format to flat JSONL rows.

    Telegraf JSON format:
      [{"fields": {...}, "name": "cpu", "tags": {...}, "timestamp": 1234567890}]
    """
    rows = []
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        return rows

    for item in data:
        if not isinstance(item, dict):
            continue
        row = {}
        # Measurement name
        if "name" in item:
            row["measurement"] = item["name"]
        # Tags (flat)
        for k, v in item.get("tags", {}).items():
            row[k] = v
        # Fields (flat)
        for k, v in item.get("fields", {}).items():
            row[k] = v
        # Timestamp
        ts = item.get("timestamp")
        if ts is not None:
            try:
                dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
                row["timestamp"] = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                row["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            row["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        rows.append(row)

    return rows


# ─── Buffer & Compression ─────────────────────────────────────────────────────

class PfcBuffer:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.buf_cfg = cfg["buffer"]
        self.pfc_bin = cfg["pfc"]["binary"]
        self.output_dir = Path(self.buf_cfg["output_dir"])
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self._rows: list[str] = []          # JSONL lines
        self._bytes: int = 0
        self._last_flush = time.monotonic()
        self._lock = asyncio.Lock()
        self._total_accepted = 0
        self._total_flushed = 0
        self._total_files = 0

    @property
    def rotate_bytes(self) -> int:
        return int(self.buf_cfg["rotate_mb"] * 1024 * 1024)

    @property
    def rotate_sec(self) -> int:
        return int(self.buf_cfg["rotate_sec"])

    async def add(self, rows: list[dict]) -> int:
        """Add rows to buffer. Returns accepted count."""
        lines = []
        for r in rows:
            try:
                lines.append(json.dumps(r, ensure_ascii=False))
            except Exception:
                continue

        async with self._lock:
            self._rows.extend(lines)
            added_bytes = sum(len(l.encode()) + 1 for l in lines)
            self._bytes += added_bytes
            self._total_accepted += len(lines)

        if self._bytes >= self.rotate_bytes:
            await self.flush(reason="size")

        return len(lines)

    async def flush(self, reason: str = "manual") -> dict:
        async with self._lock:
            if not self._rows:
                return {"flushed": False, "reason": reason, "rows": 0}

            rows_snapshot = self._rows[:]
            bytes_snapshot = self._bytes
            self._rows = []
            self._bytes = 0
            self._last_flush = time.monotonic()

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        prefix = self.buf_cfg["prefix"]
        out_path = self.output_dir / f"{prefix}_{ts}.pfc"

        result = await asyncio.get_event_loop().run_in_executor(
            None, self._compress, rows_snapshot, out_path
        )

        if result:
            self._total_flushed += len(rows_snapshot)
            self._total_files += 1
            log.info(f"Flushed {len(rows_snapshot)} rows → {out_path.name} ({reason})")

            if self.cfg["s3"]["enabled"]:
                await asyncio.get_event_loop().run_in_executor(
                    None, self._upload_s3, out_path
                )

            return {"flushed": True, "file": str(out_path), "rows": len(rows_snapshot), "reason": reason}
        else:
            # Restore on failure
            async with self._lock:
                self._rows = rows_snapshot + self._rows
                self._bytes += bytes_snapshot
            return {"flushed": False, "error": "compress failed", "rows": len(rows_snapshot)}

    def _compress(self, rows: list[str], out_path: Path) -> bool:
        """Write JSONL to temp file and compress with pfc_jsonl."""
        tmp = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
            ) as f:
                tmp = f.name
                for line in rows:
                    f.write(line + "\n")

            result = subprocess.run(
                [self.pfc_bin, "compress", tmp, str(out_path)],
                capture_output=True, text=True, timeout=120
            )
            if result.returncode != 0:
                log.error(f"pfc_jsonl compress failed: {result.stderr[:200]}")
                return False
            return True

        except FileNotFoundError:
            log.error(f"pfc_jsonl binary not found: {self.pfc_bin}")
            return False
        except subprocess.TimeoutExpired:
            log.error("pfc_jsonl compress timed out")
            return False
        except Exception as e:
            log.error(f"Compress error: {e}")
            return False
        finally:
            if tmp:
                try:
                    Path(tmp).unlink(missing_ok=True)
                except Exception:
                    pass

    def _upload_s3(self, path: Path):
        """Upload to S3 using AWS CLI."""
        s3_cfg = self.cfg["s3"]
        bucket = s3_cfg["bucket"]
        prefix = s3_cfg.get("prefix", "telegraf/")
        key = f"{prefix}{path.name}"
        try:
            result = subprocess.run(
                ["aws", "s3", "cp", str(path), f"s3://{bucket}/{key}",
                 "--region", s3_cfg.get("region", "us-east-1")],
                capture_output=True, text=True, timeout=120
            )
            if result.returncode == 0:
                log.info(f"S3 upload OK: s3://{bucket}/{key}")
            else:
                log.error(f"S3 upload failed: {result.stderr[:200]}")
        except Exception as e:
            log.error(f"S3 upload error: {e}")

    def status(self) -> dict:
        return {
            "version": VERSION,
            "buffered_rows": len(self._rows),
            "buffered_bytes": self._bytes,
            "rotate_mb": self.buf_cfg["rotate_mb"],
            "rotate_sec": self.rotate_sec,
            "total_accepted": self._total_accepted,
            "total_flushed": self._total_flushed,
            "total_files": self._total_files,
            "output_dir": str(self.output_dir),
            "s3_enabled": self.cfg["s3"]["enabled"],
        }


# ─── Watchdog ─────────────────────────────────────────────────────────────────

async def watchdog(buf: PfcBuffer):
    while True:
        await asyncio.sleep(60)
        elapsed = time.monotonic() - buf._last_flush
        if elapsed >= buf.rotate_sec and buf._rows:
            log.info(f"Watchdog: rotating after {elapsed:.0f}s")
            await buf.flush(reason="time")


# ─── FastAPI App ──────────────────────────────────────────────────────────────

def create_app(cfg: dict, buf: PfcBuffer) -> FastAPI:
    api_key = cfg["server"].get("api_key", "")

    def check_auth(request: Request) -> bool:
        if not api_key:
            return True
        return (
            request.headers.get("x-api-key") == api_key
            or request.headers.get("Authorization") == f"Bearer {api_key}"
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        task = asyncio.create_task(watchdog(buf))
        log.info(f"pfc-telegraf {VERSION} ready — output_dir={buf.output_dir}")
        yield
        task.cancel()

    app = FastAPI(title="pfc-telegraf", version=VERSION, lifespan=lifespan)

    @app.get("/health")
    async def health():
        return {"status": "ok", "version": VERSION}

    @app.get("/")
    async def root():
        return {"status": "ok", "version": VERSION, "binary": cfg["pfc"]["binary"]}

    @app.get("/ingest/status")
    async def ingest_status(request: Request):
        if not check_auth(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        return buf.status()

    @app.post("/ingest")
    async def ingest(request: Request):
        if not check_auth(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)

        content_type = request.headers.get("content-type", "").lower()
        body = await request.body()

        if not body:
            return JSONResponse({"accepted": 0, "warning": "empty body"})

        rows: list[dict] = []

        # InfluxDB line protocol
        if "text/plain" in content_type or "application/influx" in content_type:
            text = body.decode("utf-8", errors="replace")
            for line in text.splitlines():
                row = parse_line_protocol(line)
                if row:
                    rows.append(row)

        # Telegraf JSON format or generic JSON
        elif "application/json" in content_type or "json" in content_type:
            try:
                data = json.loads(body)
            except json.JSONDecodeError as e:
                return JSONResponse({"error": f"invalid JSON: {e}"}, status_code=400)

            if isinstance(data, list):
                # Check if it's Telegraf JSON format (has "fields" key)
                if data and isinstance(data[0], dict) and "fields" in data[0]:
                    rows = parse_telegraf_json(data)
                else:
                    # Generic JSON array — use as-is
                    rows = [r for r in data if isinstance(r, dict)]
            elif isinstance(data, dict):
                if "fields" in data:
                    rows = parse_telegraf_json(data)
                else:
                    rows = [data]

        else:
            # Try line protocol as fallback
            try:
                text = body.decode("utf-8", errors="replace")
                for line in text.splitlines():
                    row = parse_line_protocol(line)
                    if row:
                        rows.append(row)
            except Exception:
                return JSONResponse({"error": "unsupported content type"}, status_code=415)

        if not rows:
            return JSONResponse({"accepted": 0, "warning": "no valid rows parsed"})

        accepted = await buf.add(rows)
        return {"accepted": accepted}

    @app.post("/ingest/flush")
    async def ingest_flush(request: Request):
        if not check_auth(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        return await buf.flush(reason="manual")

    return app


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description=f"pfc-telegraf {VERSION}")
    parser.add_argument("--config", "-c", help="Path to TOML config file")
    parser.add_argument("--version", action="version", version=f"pfc-telegraf {VERSION}")
    args = parser.parse_args()

    cfg = load_config(args.config)
    buf = PfcBuffer(cfg)
    app = create_app(cfg, buf)

    host = cfg["server"]["host"]
    port = cfg["server"]["port"]
    log.info(f"pfc-telegraf {VERSION} — listening on {host}:{port}")

    def shutdown(sig, frame):
        log.info("Shutdown signal received — flushing buffer...")
        asyncio.run(buf.flush(reason="shutdown"))
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    uvicorn.run(app, host=host, port=port, log_level="warning")


if __name__ == "__main__":
    main()
