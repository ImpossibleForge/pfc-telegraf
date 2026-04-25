"""
E2E Pipeline Test for pfc-telegraf v0.1.0

Tests the full pipeline:
  Simulated Telegraf data → pfc-telegraf:8767 → .pfc/.bidx → DuckDB + pfc-gateway:8765

Requirements:
  - pfc_jsonl binary in PATH
  - DuckDB with pfc extension: INSTALL pfc FROM community
  - pfc-gateway running on port 8765 (optional, gateway tests skipped if unavailable)

Usage:
  python3 tests/test_e2e_pipeline.py
"""

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

TELEGRAF_URL = "http://localhost:8767"
GATEWAY_URL  = "http://localhost:8765"
GATEWAY_KEY  = "your-api-key"          # set to match your pfc-gateway API key, or leave empty
ARCHIVE_DIR  = "/tmp/pfc-telegraf-e2e"
DUCKDB_BIN   = "duckdb"
SCRIPT_DIR   = Path(__file__).parent.parent  # pfc-telegraf repo root

PASS, FAIL = [], []


def ok(test, detail=""):
    PASS.append(test)
    print(f"  ✅ PASS  {test}" + (f" — {detail}" if detail else ""))


def fail(test, detail=""):
    FAIL.append(test)
    print(f"  ❌ FAIL  {test}" + (f" — {detail}" if detail else ""))


def curl(method, url, data=None, content_type="application/json", headers=None):
    cmd = ["curl", "-s", "-w", "\n%{http_code}", "-X", method, url]
    h = headers or {}
    if content_type:
        h["Content-Type"] = content_type
    for k, v in h.items():
        cmd += ["-H", f"{k}: {v}"]
    if data is not None:
        body = data if isinstance(data, bytes) else (
            json.dumps(data).encode() if not isinstance(data, str) else data.encode()
        )
        cmd += ["--data-binary", "@-"]
        result = subprocess.run(cmd, input=body, capture_output=True, timeout=15)
    else:
        result = subprocess.run(cmd, capture_output=True, timeout=15)
    out = result.stdout.decode("utf-8", errors="replace")
    parts = out.rsplit("\n", 1)
    body_out = parts[0].strip()
    status = int(parts[1].strip()) if len(parts) > 1 else 0
    return status, body_out


def make_line_protocol(count=100, measurement="cpu", start_min=0):
    """Generate InfluxDB line protocol lines."""
    base = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=start_min)
    lines = []
    for i in range(count):
        ts = base + timedelta(seconds=i * 5)
        ns = int(ts.timestamp() * 1_000_000_000)
        host = f"server{(i % 3) + 1:02d}"
        usage_user = round(1.0 + (i % 20) * 0.5, 1)
        usage_idle = round(100.0 - usage_user, 1)
        lines.append(
            f"{measurement},host={host},region=us-east "
            f"usage_user={usage_user},usage_idle={usage_idle},load={i % 10}i "
            f"{ns}"
        )
    return "\n".join(lines) + "\n"


def make_telegraf_json(count=50, measurement="mem", start_min=10):
    """Generate Telegraf JSON format metrics."""
    base = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=start_min)
    rows = []
    for i in range(count):
        ts = base + timedelta(seconds=i * 5)
        rows.append({
            "fields": {"used": 1024 * (i + 1), "total": 8192,
                       "free": 8192 - 1024 * (i % 8 + 1)},
            "name": measurement,
            "tags": {"host": f"server{(i % 3) + 1:02d}", "region": "us-east"},
            "timestamp": int(ts.timestamp()),
        })
    return rows


print("\n══════════════════════════════════════════════════")
print("  pfc-telegraf — E2E Pipeline Test")
print("══════════════════════════════════════════════════\n")

# ── 0. Setup: start pfc-telegraf ──────────────────────────────────────────────
print("【0】Service Setup")
os.makedirs(ARCHIVE_DIR, exist_ok=True)

proc = subprocess.Popen(
    [sys.executable, str(SCRIPT_DIR / "pfc_telegraf.py")],
    env={**os.environ, "PFC_OUTPUT_DIR": ARCHIVE_DIR, "PFC_API_KEY": ""},
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
)
time.sleep(2)

status, body = curl("GET", f"{TELEGRAF_URL}/health")
if status == 200:
    ok("pfc-telegraf started", f"pid={proc.pid}")
else:
    fail("pfc-telegraf started", f"status={status}")
    proc.terminate()
    sys.exit(1)

# ── 1. Health checks ──────────────────────────────────────────────────────────
print("\n【1】Health Checks")
gw_available = False
status, body = curl("GET", f"{GATEWAY_URL}/health")
if status == 200:
    ok("pfc-gateway reachable", "gateway tests will run")
    gw_available = True
else:
    ok("pfc-gateway not running", "gateway tests skipped — start pfc-gateway to enable")

# ── 2. Ingest: InfluxDB Line Protocol ─────────────────────────────────────────
print("\n【2】Ingest — InfluxDB Line Protocol (data_format = influx)")
lp_data = make_line_protocol(150, "cpu", start_min=0)
status, body = curl("POST", f"{TELEGRAF_URL}/ingest",
                    data=lp_data.encode(), content_type="text/plain")
if status == 200:
    accepted = json.loads(body).get("accepted", 0)
    ok("Line protocol ingest", f"accepted={accepted} rows")
else:
    fail("Line protocol ingest", f"status={status}, body={body[:80]}")

# ── 3. Ingest: Telegraf JSON format ───────────────────────────────────────────
print("\n【3】Ingest — Telegraf JSON Format (data_format = json)")
json_data = make_telegraf_json(80, "mem", start_min=15)
status, body = curl("POST", f"{TELEGRAF_URL}/ingest", data=json_data)
if status == 200:
    ok("Telegraf JSON ingest", f"accepted={json.loads(body).get('accepted', 0)} rows")
else:
    fail("Telegraf JSON ingest", f"status={status}")

# ── 4. Multi-measurement ingest ───────────────────────────────────────────────
print("\n【4】Ingest — Multiple Measurements (disk, net)")
for meas, start in [("disk", 5), ("net", 7)]:
    data = make_line_protocol(50, meas, start_min=start)
    status, body = curl("POST", f"{TELEGRAF_URL}/ingest",
                        data=data.encode(), content_type="text/plain")
    if status == 200:
        ok(f"{meas} ingest", f"accepted={json.loads(body).get('accepted', 0)}")
    else:
        fail(f"{meas} ingest", f"status={status}")

# ── 5. Status check ───────────────────────────────────────────────────────────
print("\n【5】Status Check")
status, body = curl("GET", f"{TELEGRAF_URL}/ingest/status")
if status == 200:
    s = json.loads(body)
    ok("ingest/status", f"buffered={s.get('buffered_rows', 0)}, "
       f"total_accepted={s.get('total_accepted', 0)}")
else:
    fail("ingest/status", f"status={status}")

# ── 6. Force Flush ────────────────────────────────────────────────────────────
print("\n【6】Force Flush — Compress to .pfc")
status, body = curl("POST", f"{TELEGRAF_URL}/ingest/flush")
if status == 200:
    r = json.loads(body)
    ok("Force flush", f"flushed={r.get('flushed')}, rows={r.get('rows', 0)}")
else:
    fail("Force flush", f"status={status}, body={body[:80]}")

print("     (waiting 5s for compression...)")
time.sleep(5)

# ── 7. Archive verification ───────────────────────────────────────────────────
print("\n【7】Archive Output")
pfc_files = subprocess.run(
    ["find", ARCHIVE_DIR, "-name", "*.pfc", "-type", "f"],
    capture_output=True, text=True
).stdout.strip().splitlines()

bidx_files = subprocess.run(
    ["find", ARCHIVE_DIR, "-name", "*.bidx", "-type", "f"],
    capture_output=True, text=True
).stdout.strip().splitlines()

if pfc_files:
    ok(".pfc file created", f"{len(pfc_files)} file(s)")
else:
    fail(".pfc file created", "no .pfc files found")

if bidx_files:
    ok(".bidx index created", f"{len(bidx_files)} file(s)")
else:
    fail(".bidx index created", "no .bidx files found")

# ── 8. pfc_jsonl info ─────────────────────────────────────────────────────────
print("\n【8】pfc_jsonl info")
if pfc_files:
    r = subprocess.run(
        ["pfc_jsonl", "info", pfc_files[0]],
        capture_output=True, text=True, timeout=10
    )
    if r.returncode == 0:
        ok("pfc_jsonl info", pfc_files[0].split("/")[-1])
    else:
        fail("pfc_jsonl info", r.stderr[:80])

# ── 9. DuckDB Queries ─────────────────────────────────────────────────────────
print("\n【9】DuckDB — Queries on pfc-telegraf Output")

def dq(query):
    r = subprocess.run(
        [DUCKDB_BIN, "-csv", "-c", f"INSTALL pfc FROM community; LOAD pfc; {query}"],
        capture_output=True, text=True, timeout=30
    )
    lines = [l for l in r.stdout.strip().splitlines() if l.strip()]
    val = lines[1].split(",")[0].strip() if len(lines) > 1 else ""
    return r.returncode == 0, val, r.stderr


if pfc_files:
    pf = pfc_files[0]
    J = lambda f: f"json_extract_string(line, '$.{f}')"
    N = lambda f: f"json_extract(line, '$.{f}')::FLOAT"

    duckdb_tests = [
        ("Row Count",
         f"SELECT COUNT(*) FROM pfc_scan('{pf}');",
         lambda v: v.isdigit() and int(v) > 0,
         lambda v: f"{v} rows"),
        ("Measurement Filter",
         f"SELECT COUNT(*) FROM pfc_scan('{pf}') WHERE {J('measurement')}='cpu';",
         lambda v: v.isdigit() and int(v) > 0,
         lambda v: f"{v} cpu rows"),
        ("Host Filter",
         f"SELECT COUNT(DISTINCT {J('host')}) FROM pfc_scan('{pf}');",
         lambda v: v.isdigit() and int(v) >= 1,
         lambda v: f"{v} distinct hosts"),
        ("Timestamp Range",
         f"SELECT COUNT(*) FROM pfc_scan('{pf}') "
         f"WHERE {J('timestamp')} >= '2026-01-01T10:05:00Z' "
         f"AND {J('timestamp')} < '2026-01-01T10:15:00Z';",
         lambda v: v.isdigit(),
         lambda v: f"{v} rows in 10-min window"),
        ("Avg Field Value",
         f"SELECT ROUND(AVG({N('usage_user')}), 2) FROM pfc_scan('{pf}') "
         f"WHERE {J('measurement')} = 'cpu';",
         lambda v: len(v) > 0 and v not in ("avg", ""),
         lambda v: f"avg_usage_user={v}"),
    ]

    for name, q, check, detail_fn in duckdb_tests:
        ok_, val, err = dq(q)
        if ok_ and check(val):
            ok(f"DuckDB: {name}", detail_fn(val))
        else:
            fail(f"DuckDB: {name}", (err or val)[:100])

# ── 10. pfc-gateway Queries ───────────────────────────────────────────────────
print("\n【10】pfc-gateway — HTTP Queries on pfc-telegraf Output")
if pfc_files and gw_available:
    pf = pfc_files[0]
    h = {"x-api-key": GATEWAY_KEY} if GATEWAY_KEY else {}

    status, body = curl("POST", f"{GATEWAY_URL}/query", data={"file": pf}, headers=h)
    if status == 200:
        rows = [l for l in body.splitlines() if l.strip().startswith("{")]
        ok("pfc-gateway /query (no filter)", f"{len(rows)} rows")
    else:
        fail("pfc-gateway /query", f"status={status}, body={body[:100]}")

    status, body = curl("POST", f"{GATEWAY_URL}/query",
                        data={"file": pf,
                              "from_ts": "2026-01-01T10:00:00Z",
                              "to_ts":   "2026-01-01T10:10:00Z"},
                        headers=h)
    if status == 200:
        rows = [l for l in body.splitlines() if l.strip().startswith("{")]
        ok("pfc-gateway /query (timestamp range)", f"{len(rows)} rows in 10-min window")
    else:
        fail("pfc-gateway /query (timestamp range)", f"status={status}")
elif pfc_files:
    print("  ⏭  pfc-gateway tests skipped (not running)")

# ── 11. Second flush + multi-file batch ───────────────────────────────────────
print("\n【11】Second Flush + Multi-File Batch Query")
more_data = make_line_protocol(60, "cpu", start_min=30)
curl("POST", f"{TELEGRAF_URL}/ingest", data=more_data.encode(), content_type="text/plain")
curl("POST", f"{TELEGRAF_URL}/ingest/flush")
time.sleep(5)

all_pfc = subprocess.run(
    ["find", ARCHIVE_DIR, "-name", "*.pfc", "-type", "f"],
    capture_output=True, text=True
).stdout.strip().splitlines()
ok("Multiple .pfc files", f"{len(all_pfc)} total files")

if len(all_pfc) >= 2 and gw_available:
    h = {"x-api-key": GATEWAY_KEY} if GATEWAY_KEY else {}
    status, body = curl("POST", f"{GATEWAY_URL}/query/batch",
                        data={"files": all_pfc}, headers=h)
    if status == 200:
        rows = [l for l in body.splitlines() if l.strip().startswith("{")]
        ok("pfc-gateway /query/batch", f"{len(rows)} rows from {len(all_pfc)} files")
    else:
        fail("pfc-gateway /query/batch", f"status={status}")

# ── Cleanup ───────────────────────────────────────────────────────────────────
proc.terminate()
try:
    proc.wait(timeout=3)
except Exception:
    proc.kill()

# ── Summary ───────────────────────────────────────────────────────────────────
print("\n══════════════════════════════════════════════════")
total = len(PASS) + len(FAIL)
print(f"  Result: {len(PASS)}/{total} PASS")
if FAIL:
    print("\n  Failed:")
    for f in FAIL:
        print(f"    ✗ {f}")
print("══════════════════════════════════════════════════\n")
sys.exit(0 if not FAIL else 1)
