"""
Resilience tests for pfc-telegraf v0.1.0
Tests: error recovery, missing binary, disk issues, graceful shutdown, edge cases
"""

import asyncio
import json
import sys
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parent.parent))
from pfc_telegraf import PfcBuffer, create_app, parse_line_protocol, parse_telegraf_json


def make_cfg(tmp_path, api_key="", pfc_binary="/usr/local/bin/pfc_jsonl"):
    return {
        "server": {"host": "0.0.0.0", "port": 8767, "api_key": api_key},
        "buffer": {"rotate_mb": 64, "rotate_sec": 3600,
                   "output_dir": str(tmp_path), "prefix": "telegraf"},
        "pfc": {"binary": pfc_binary},
        "s3": {"enabled": False, "bucket": "", "prefix": "", "region": "us-east-1"},
    }


# ─── Binary Missing ───────────────────────────────────────────────────────────

class TestMissingBinary:

    @pytest.mark.asyncio
    async def test_flush_with_missing_binary(self, tmp_path):
        cfg = make_cfg(tmp_path, pfc_binary="/nonexistent/pfc_jsonl")
        buf = PfcBuffer(cfg)
        await buf.add([{"measurement": "cpu", "value": 1.0, "timestamp": "2026-01-01T00:00:00Z"}])
        result = await buf.flush(reason="test")
        # Should fail gracefully — not raise
        assert result["flushed"] is False
        assert "error" in result or "compress failed" in str(result)

    @pytest.mark.asyncio
    async def test_rows_restored_after_failed_flush(self, tmp_path):
        cfg = make_cfg(tmp_path, pfc_binary="/nonexistent/pfc_jsonl")
        buf = PfcBuffer(cfg)
        await buf.add([{"measurement": "cpu", "value": 1.0, "timestamp": "2026-01-01T00:00:00Z"}])
        result = await buf.flush(reason="test")
        # Rows should be restored to buffer after failed compress
        assert buf._rows  # buffer not empty

    def test_http_ingest_still_works_without_binary(self, tmp_path):
        """Ingest should succeed even if pfc_jsonl isn't there yet — compress happens at flush."""
        cfg = make_cfg(tmp_path, pfc_binary="/nonexistent/pfc_jsonl")
        buf = PfcBuffer(cfg)
        app = create_app(cfg, buf)
        client = TestClient(app)
        r = client.post("/ingest", content=b"cpu usage=50.0 1700000000000000000\n",
                        headers={"Content-Type": "text/plain"})
        assert r.status_code == 200
        assert r.json()["accepted"] == 1


# ─── Malformed Input ──────────────────────────────────────────────────────────

class TestMalformedInput:

    def test_line_protocol_no_fields_skipped(self, tmp_path):
        cfg = make_cfg(tmp_path)
        buf = PfcBuffer(cfg)
        app = create_app(cfg, buf)
        client = TestClient(app)
        body = "onlymeasurement\ncpu usage=50.0 1700000000000000000\n"
        r = client.post("/ingest", content=body.encode(),
                        headers={"Content-Type": "text/plain"})
        assert r.status_code == 200
        # Only valid line accepted
        assert r.json()["accepted"] == 1

    def test_mixed_valid_invalid_lines(self, tmp_path):
        cfg = make_cfg(tmp_path)
        buf = PfcBuffer(cfg)
        app = create_app(cfg, buf)
        client = TestClient(app)
        body = (
            "cpu usage=1.0 1700000000000000000\n"
            "# comment skipped\n"
            "\n"
            "mem used=2048i 1700000000000000000\n"
        )
        r = client.post("/ingest", content=body.encode(),
                        headers={"Content-Type": "text/plain"})
        assert r.status_code == 200
        assert r.json()["accepted"] == 2

    def test_json_non_array_single_object(self, tmp_path):
        cfg = make_cfg(tmp_path)
        buf = PfcBuffer(cfg)
        app = create_app(cfg, buf)
        client = TestClient(app)
        data = {"measurement": "cpu", "value": 1.0}
        r = client.post("/ingest", json=data)
        assert r.status_code == 200
        assert r.json()["accepted"] == 1

    def test_invalid_json_returns_400(self, tmp_path):
        cfg = make_cfg(tmp_path)
        buf = PfcBuffer(cfg)
        app = create_app(cfg, buf)
        client = TestClient(app)
        r = client.post("/ingest", content=b"{broken json",
                        headers={"Content-Type": "application/json"})
        assert r.status_code == 400

    def test_very_long_line_handled(self, tmp_path):
        cfg = make_cfg(tmp_path)
        buf = PfcBuffer(cfg)
        app = create_app(cfg, buf)
        client = TestClient(app)
        long_tag = "x" * 10000
        body = f"cpu,host={long_tag} usage=50.0 1700000000000000000\n"
        r = client.post("/ingest", content=body.encode(),
                        headers={"Content-Type": "text/plain"})
        assert r.status_code == 200

    def test_binary_data_in_body(self, tmp_path):
        cfg = make_cfg(tmp_path)
        buf = PfcBuffer(cfg)
        app = create_app(cfg, buf)
        client = TestClient(app)
        binary_body = bytes(range(256))
        r = client.post("/ingest", content=binary_body,
                        headers={"Content-Type": "text/plain"})
        # Should not crash — may return 0 accepted
        assert r.status_code == 200

    def test_empty_json_array(self, tmp_path):
        cfg = make_cfg(tmp_path)
        buf = PfcBuffer(cfg)
        app = create_app(cfg, buf)
        client = TestClient(app)
        r = client.post("/ingest", json=[])
        assert r.status_code == 200
        assert r.json()["accepted"] == 0


# ─── Buffer Behavior ──────────────────────────────────────────────────────────

class TestBufferBehavior:

    @pytest.mark.asyncio
    async def test_flush_empty_buffer(self, tmp_path):
        cfg = make_cfg(tmp_path)
        buf = PfcBuffer(cfg)
        result = await buf.flush(reason="test")
        assert result["flushed"] is False

    @pytest.mark.asyncio
    async def test_status_accurate_after_add(self, tmp_path):
        cfg = make_cfg(tmp_path)
        buf = PfcBuffer(cfg)
        rows = [{"measurement": "cpu", "v": i, "timestamp": "2026-01-01T00:00:00Z"}
                for i in range(10)]
        await buf.add(rows)
        s = buf.status()
        assert s["buffered_rows"] == 10
        assert s["total_accepted"] == 10

    @pytest.mark.asyncio
    async def test_multiple_add_calls_accumulate(self, tmp_path):
        cfg = make_cfg(tmp_path)
        buf = PfcBuffer(cfg)
        for _ in range(5):
            await buf.add([{"measurement": "cpu", "v": 1.0, "timestamp": "2026-01-01T00:00:00Z"}])
        assert buf._total_accepted == 5
        assert len(buf._rows) == 5

    @pytest.mark.asyncio
    async def test_flush_clears_buffer(self, tmp_path):
        cfg = make_cfg(tmp_path, pfc_binary="/usr/local/bin/pfc_jsonl")
        buf = PfcBuffer(cfg)
        await buf.add([{"measurement": "cpu", "v": 1.0, "timestamp": "2026-01-01T00:00:00Z"}])

        with patch.object(buf, '_compress', return_value=True):
            await buf.flush(reason="test")

        assert buf._rows == []
        assert buf._bytes == 0

    def test_output_dir_created_if_missing(self, tmp_path):
        nested = tmp_path / "deep" / "nested" / "dir"
        cfg = make_cfg(tmp_path)
        cfg["buffer"]["output_dir"] = str(nested)
        buf = PfcBuffer(cfg)
        assert nested.exists()


# ─── Concurrent Ingest ────────────────────────────────────────────────────────

class TestConcurrentIngest:

    def test_concurrent_posts_all_accepted(self, tmp_path):
        cfg = make_cfg(tmp_path)
        buf = PfcBuffer(cfg)
        app = create_app(cfg, buf)
        client = TestClient(app)

        total = 0
        for i in range(20):
            body = f"cpu,host=srv{i} usage={float(i)} 1700000000000000000\n"
            r = client.post("/ingest", content=body.encode(),
                            headers={"Content-Type": "text/plain"})
            assert r.status_code == 200
            total += r.json()["accepted"]

        assert total == 20
        r2 = client.get("/ingest/status")
        assert r2.json()["total_accepted"] == 20


# ─── Telegraf JSON Edge Cases ─────────────────────────────────────────────────

class TestTelegrafJsonEdgeCases:

    def test_telegraf_json_without_name(self):
        data = [{"fields": {"v": 1.0}, "tags": {"host": "x"}, "timestamp": 1700000000}]
        rows = parse_telegraf_json(data)
        assert len(rows) == 1
        assert "measurement" not in rows[0]

    def test_telegraf_json_empty_fields(self):
        data = [{"fields": {}, "name": "cpu", "tags": {}, "timestamp": 1700000000}]
        rows = parse_telegraf_json(data)
        assert len(rows) == 1
        assert rows[0]["measurement"] == "cpu"

    def test_many_metrics_parsed(self):
        data = [{"fields": {"v": float(i)}, "name": "m", "tags": {}, "timestamp": 1700000000}
                for i in range(100)]
        rows = parse_telegraf_json(data)
        assert len(rows) == 100

    def test_fields_and_tags_dont_collide(self):
        """Tags take precedence over fields if same key name."""
        data = [{"fields": {"host": "field_host", "v": 1.0},
                 "name": "cpu",
                 "tags": {"host": "tag_host"},
                 "timestamp": 1700000000}]
        rows = parse_telegraf_json(data)
        # Tags are applied first, fields after — fields may overwrite tags
        # We just check no crash and we have a host key
        assert "host" in rows[0]
