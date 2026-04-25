"""
E2E Pipeline Test for pfc-telegraf v0.1.0
Tests the full pipeline:
  Telegraf-simulated data → pfc-telegraf:8767 → .pfc/.bidx → DuckDB + pfc-gateway:8765
Run on server: python3 tests/test_e2e_pipeline.py
"""

import json
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta

TELEGRAF_URL = "http://localhost:8767"
GATEWAY_URL  = "http://localhost:8765"
GATEWAY_KEY  = "testkey"
ARCHIVE_DIR  = "/root/pfc-telegraf-e2e"
DUCKDB_BIN   = "duckdb"

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
    base = datetime(2026, 4, 25, 10, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=start_min)
    lines = []
    for i in range(count):
        ts = base + timedelta(seconds=i * 5)
        ns = int(ts.timestamp() * 1_000_000_000)
        host = f"server{(i % 3) + 1:02d}"
        usage_user = round(1.0 + (i % 20) * 0.5, 1)
        usage_idle = round(100.0 - usage_user, 1)
        lines.append(
            f"{measurement},host={host},region=eu-central "
            f"usage_user={usage_user},usage_idle={usage_idle},load={i % 10}i "
            f"{ns}"
        )
    return "\n".join(lines) + "\n"

def make_telegraf_json(count=50, measurement="mem", start_min=10):
    """Generate Telegraf JSON format metrics."""
    base = datetime(2026, 4, 25, 10, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=start_min)
    rows = []
    for i in range(count):
        ts = base + timedelta(seconds=i * 5)
        rows.append({
            "fields": {"used": 1024 * (i + 1), "total": 8192, "free": 8192 - 1024 * (i % 8 + 1)},
            "name": measurement,
            "tags": {"host": f"server{(i % 3) + 1:02d}", "region": "eu-central"},
            "timestamp": int(ts.timestamp()),
        })
    return rows

print("\n══════════════════════════════════════════════════")
print("  pfc-telegraf — E2E Pipeline Test")
print("══════════════════════════════════════════════════\n")

# ── 0. Setup: start pfc-telegraf ──────────────────────────────────────────────
print("【0】Service Setup")
import os, signal
os.makedirs(ARCHIVE_DIR, exist_ok=True)

proc = subprocess.Popen(
    ["python3", "/root/pfc-telegraf/pfc_telegraf.py"],
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
status, body = curl("GET", f"{GATEWAY_URL}/", headers={"x-api-key": GATEWAY_KEY})
if status == 200:
    ok("pfc-gateway reachable", json.loads(body).get("version", "?"))
else:
    fail("pfc-gateway reachable", f"status={status} (start pfc-gateway first)")

# ── 2. Ingest: InfluxDB Line Protocol ────────────────────────────────────────
print("\n【2】Ingest — InfluxDB Line Protocol (Telegraf outputs.http data_format=influx)")
lp_data = make_line_protocol(150, "cpu", start_min=0)
status, body = curl("POST", f"{TELEGRAF_URL}/ingest", data=lp_data.encode(), content_type="text/plain")
if status == 200:
    accepted = json.loads(body).get("accepted", 0)
    ok("Line Protocol ingest", f"accepted={accepted} rows")
else:
    fail("Line Protocol ingest", f"status={status}, body={body[:80]}")

# ── 3. Ingest: Telegraf JSON format ──────────────────────────────────────────
print("\n【3】Ingest — Telegraf JSON Format (data_format=json)")
json_data = make_telegraf_json(80, "mem", start_min=15)
status, body = curl("POST", f"{TELEGRAF_URL}/ingest", data=json_data)
if status == 200:
    accepted = json.loads(body).get("accepted", 0)
    ok("Telegraf JSON ingest", f"accepted={accepted} rows")
else:
    fail("Telegraf JSON ingest", f"status={status}")

# ── 4. Multi-measurement ingest ───────────────────────────────────────────────
print("\n【4】Ingest — Multiple Measurements (disk, net)")
disk_data = make_line_protocol(50, "disk", start_min=5)
net_data  = make_line_protocol(50, "net", start_min=7)
for meas, data in [("disk", disk_data), ("net", net_data)]:
    status, body = curl("POST", f"{TELEGRAF_URL}/ingest", data=data.encode(), content_type="text/plain")
    if status == 200:
        ok(f"{meas} ingest", f"accepted={json.loads(body).get('accepted',0)}")
    else:
        fail(f"{meas} ingest", f"status={status}")

# ── 5. Status check ───────────────────────────────────────────────────────────
print("\n【5】Status vor Flush")
status, body = curl("GET", f"{TELEGRAF_URL}/ingest/status")
if status == 200:
    s = json.loads(body)
    ok("ingest/status", f"buffered={s.get('buffered_rows',0)}, total_accepted={s.get('total_accepted',0)}")
else:
    fail("ingest/status", f"status={status}")

# ── 6. Force Flush ────────────────────────────────────────────────────────────
print("\n【6】Force Flush → Compress to .pfc")
status, body = curl("POST", f"{TELEGRAF_URL}/ingest/flush")
if status == 200:
    r = json.loads(body)
    ok("Force flush", f"flushed={r.get('flushed')}, rows={r.get('rows',0)}")
else:
    fail("Force flush", f"status={status}, body={body[:80]}")

print("     (warte 5s auf Komprimierung...)")
time.sleep(5)

# ── 7. Archive verification ───────────────────────────────────────────────────
print("\n【7】Archive Output prüfen")
result = subprocess.run(["ls", "-la", ARCHIVE_DIR], capture_output=True, text=True)
pfc_lines = [l for l in result.stdout.splitlines() if ".pfc" in l]
for l in pfc_lines:
    print(f"     {l.strip()}")

pfc_files = subprocess.run(
    ["find", ARCHIVE_DIR, "-name", "*.pfc", "-type", "f"],
    capture_output=True, text=True
).stdout.strip().splitlines()

if pfc_files:
    ok(".pfc file created", f"{len(pfc_files)} file(s)")
    bidx = subprocess.run(
        ["find", ARCHIVE_DIR, "-name", "*.bidx"], capture_output=True, text=True
    ).stdout.strip().splitlines()
    ok(".bidx index created", f"{len(bidx)} file(s)") if bidx else fail(".bidx missing")
else:
    fail("No .pfc files found")

# ── 8. pfc_jsonl info ─────────────────────────────────────────────────────────
print("\n【8】pfc_jsonl info auf Output")
if pfc_files:
    r = subprocess.run(["pfc_jsonl", "info", pfc_files[0]], capture_output=True, text=True, timeout=10)
    if r.returncode == 0:
        for l in r.stdout.splitlines()[:6]:
            print(f"     {l}")
        ok("pfc_jsonl info", pfc_files[0].split("/")[-1])
    else:
        fail("pfc_jsonl info", r.stderr[:80])

# ── 9. DuckDB Queries ─────────────────────────────────────────────────────────
print("\n【9】DuckDB — Queries auf pfc-telegraf Output")
FN = "read_pfc_jsonl"

def dq(query):
    r = subprocess.run([DUCKDB_BIN, "-csv", "-c", f"LOAD pfc; {query}"],
                       capture_output=True, text=True, timeout=30)
    lines = [l for l in r.stdout.strip().splitlines() if l.strip()]
    val = lines[1].split(",")[0].strip() if len(lines) > 1 else ""
    return r.returncode == 0, val, r.stderr

if pfc_files:
    pf = pfc_files[0]
    J = lambda f: f"json_extract_string(line, '$.{f}')"
    N = lambda f: f"json_extract(line, '$.{f}')::FLOAT"

    tests = [
        ("Row Count",
         f"SELECT COUNT(*) FROM {FN}('{pf}');",
         lambda v: int(v) > 0,
         lambda v: f"{v} rows"),
        ("Measurement Filter (cpu)",
         f"SELECT COUNT(*) FROM {FN}('{pf}') WHERE {J('measurement')}='cpu';",
         lambda v: int(v) > 0,
         lambda v: f"{v} cpu rows"),
        ("Host Filter",
         f"SELECT COUNT(DISTINCT {J('host')}) FROM {FN}('{pf}');",
         lambda v: int(v) >= 1,
         lambda v: f"{v} distinct hosts"),
        ("Timestamp Range",
         f"SELECT COUNT(*) FROM {FN}('{pf}') WHERE {J('timestamp')}>='2026-04-25T10:05:00Z' AND {J('timestamp')}<'2026-04-25T10:15:00Z';",
         lambda v: v.isdigit(),
         lambda v: f"{v} rows in 10-min window"),
        ("Avg Usage",
         f"SELECT ROUND(AVG({N('usage_user')}),2) FROM {FN}('{pf}') WHERE {J('measurement')}='cpu';",
         lambda v: len(v) > 0 and v != 'avg',
         lambda v: f"avg_usage_user={v}"),
    ]

    for name, q, check, detail_fn in tests:
        ok_, val, err = dq(q)
        if ok_ and check(val):
            ok(f"DuckDB: {name}", detail_fn(val))
        else:
            fail(f"DuckDB: {name}", (err or val)[:100])

# ── 10. pfc-gateway Queries ───────────────────────────────────────────────────
print("\n【10】pfc-gateway — HTTP Queries auf pfc-telegraf Output")
if pfc_files:
    pf = pfc_files[0]

    # No-filter query
    status, body = curl("POST", f"{GATEWAY_URL}/query",
                        data={"file": pf},
                        headers={"x-api-key": GATEWAY_KEY})
    if status == 200:
        rows = [l for l in body.splitlines() if l.strip().startswith("{")]
        ok("pfc-gateway /query (no filter)", f"{len(rows)} rows")
    else:
        fail("pfc-gateway /query", f"status={status}, body={body[:100]}")

    # Timestamp range
    status, body = curl("POST", f"{GATEWAY_URL}/query",
                        data={"file": pf,
                              "from_ts": "2026-04-25T10:00:00Z",
                              "to_ts":   "2026-04-25T10:10:00Z"},
                        headers={"x-api-key": GATEWAY_KEY})
    if status == 200:
        rows = [l for l in body.splitlines() if l.strip().startswith("{")]
        ok("pfc-gateway /query (ts range)", f"{len(rows)} rows in 10-min window")
    else:
        fail("pfc-gateway /query (ts range)", f"status={status}, body={body[:100]}")

# ── 11. Second flush → multi-file batch ──────────────────────────────────────
print("\n【11】Second flush + batch query")
more_data = make_line_protocol(60, "cpu", start_min=30)
curl("POST", f"{TELEGRAF_URL}/ingest", data=more_data.encode(), content_type="text/plain")
curl("POST", f"{TELEGRAF_URL}/ingest/flush")
time.sleep(5)

all_pfc = subprocess.run(
    ["find", ARCHIVE_DIR, "-name", "*.pfc", "-type", "f"],
    capture_output=True, text=True
).stdout.strip().splitlines()
ok("Multiple .pfc files", f"{len(all_pfc)} total files")

if len(all_pfc) >= 2:
    status, body = curl("POST", f"{GATEWAY_URL}/query/batch",
                        data={"files": all_pfc},
                        headers={"x-api-key": GATEWAY_KEY})
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
print(f"  Ergebnis: {len(PASS)}/{total} PASS")
if FAIL:
    print("\n  Fehlgeschlagen:")
    for f in FAIL:
        print(f"    ✗ {f}")
print("══════════════════════════════════════════════════\n")
sys.exit(0 if not FAIL else 1)
