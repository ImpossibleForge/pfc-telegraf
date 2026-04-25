"""
Unit tests for pfc-telegraf v0.1.0
Tests: line protocol parser, Telegraf JSON parser, buffer logic, HTTP endpoints
"""

import asyncio
import json
import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parent.parent))
from pfc_telegraf import (
    PfcBuffer,
    VERSION,
    create_app,
    deep_merge,
    load_config,
    parse_line_protocol,
    parse_telegraf_json,
)

# ─── Line Protocol Parser ─────────────────────────────────────────────────────

class TestLineProtocolParser:

    def test_simple_measurement_with_fields(self):
        row = parse_line_protocol("cpu usage_idle=98.0,usage_user=1.5 1700000000000000000")
        assert row["measurement"] == "cpu"
        assert row["usage_idle"] == 98.0
        assert row["usage_user"] == 1.5
        assert "2023" in row["timestamp"] or "2024" in row["timestamp"]

    def test_measurement_with_tags(self):
        row = parse_line_protocol("cpu,host=server01,region=us-east usage_idle=98.0 1700000000000000000")
        assert row["measurement"] == "cpu"
        assert row["host"] == "server01"
        assert row["region"] == "us-east"
        assert row["usage_idle"] == 98.0

    def test_integer_field(self):
        row = parse_line_protocol("disk,path=/ free=442695610368i 1700000000000000000")
        assert row["free"] == 442695610368
        assert isinstance(row["free"], int)

    def test_float_field(self):
        row = parse_line_protocol("temp sensor=23.5 1700000000000000000")
        assert row["sensor"] == 23.5
        assert isinstance(row["sensor"], float)

    def test_boolean_field_true(self):
        row = parse_line_protocol("health active=true 1700000000000000000")
        assert row["active"] is True

    def test_boolean_field_false(self):
        row = parse_line_protocol("health active=false 1700000000000000000")
        assert row["active"] is False

    def test_string_field(self):
        row = parse_line_protocol('event message="hello world" 1700000000000000000')
        assert row["message"] == "hello world"

    def test_no_timestamp_adds_current(self):
        row = parse_line_protocol("cpu usage=50.0")
        assert "timestamp" in row
        assert "T" in row["timestamp"]

    def test_nanosecond_timestamp_converted(self):
        # 2023-11-14T22:13:20Z in nanoseconds
        ns = 1700000000 * 1_000_000_000
        row = parse_line_protocol(f"cpu usage=50.0 {ns}")
        assert row["timestamp"] == "2023-11-14T22:13:20Z"

    def test_multiple_tags_and_fields(self):
        row = parse_line_protocol(
            "net,host=web01,interface=eth0 bytes_recv=1024i,bytes_sent=2048i,drop_in=0i 1700000000000000000"
        )
        assert row["host"] == "web01"
        assert row["interface"] == "eth0"
        assert row["bytes_recv"] == 1024
        assert row["bytes_sent"] == 2048
        assert row["drop_in"] == 0

    def test_comment_line_returns_none(self):
        assert parse_line_protocol("# this is a comment") is None

    def test_empty_line_returns_none(self):
        assert parse_line_protocol("") is None
        assert parse_line_protocol("   ") is None

    def test_malformed_no_fields_returns_none(self):
        assert parse_line_protocol("justameasurement") is None

    def test_negative_float(self):
        row = parse_line_protocol("temp value=-10.5 1700000000000000000")
        assert row["value"] == -10.5

    def test_negative_integer(self):
        row = parse_line_protocol("delta value=-42i 1700000000000000000")
        assert row["value"] == -42

    def test_measurement_preserved(self):
        row = parse_line_protocol("my_measurement field=1.0 1700000000000000000")
        assert row["measurement"] == "my_measurement"


# ─── Telegraf JSON Parser ─────────────────────────────────────────────────────

class TestTelegramJsonParser:

    def test_standard_telegraf_json_format(self):
        data = [{"fields": {"usage_user": 1.5, "usage_idle": 98.5},
                 "name": "cpu",
                 "tags": {"host": "server01"},
                 "timestamp": 1700000000}]
        rows = parse_telegraf_json(data)
        assert len(rows) == 1
        assert rows[0]["measurement"] == "cpu"
        assert rows[0]["host"] == "server01"
        assert rows[0]["usage_user"] == 1.5
        assert rows[0]["usage_idle"] == 98.5
        assert rows[0]["timestamp"] == "2023-11-14T22:13:20Z"

    def test_multiple_metrics(self):
        data = [
            {"fields": {"value": 1.0}, "name": "m1", "tags": {}, "timestamp": 1700000000},
            {"fields": {"value": 2.0}, "name": "m2", "tags": {}, "timestamp": 1700000001},
        ]
        rows = parse_telegraf_json(data)
        assert len(rows) == 2
        assert rows[0]["measurement"] == "m1"
        assert rows[1]["measurement"] == "m2"

    def test_missing_timestamp_adds_current(self):
        data = [{"fields": {"val": 1.0}, "name": "test", "tags": {}}]
        rows = parse_telegraf_json(data)
        assert len(rows) == 1
        assert "timestamp" in rows[0]

    def test_multiple_tags_flattened(self):
        data = [{"fields": {"v": 1},
                 "name": "net",
                 "tags": {"host": "web01", "interface": "eth0"},
                 "timestamp": 1700000000}]
        rows = parse_telegraf_json(data)
        assert rows[0]["host"] == "web01"
        assert rows[0]["interface"] == "eth0"

    def test_empty_tags(self):
        data = [{"fields": {"val": 42.0}, "name": "cpu", "tags": {}, "timestamp": 1700000000}]
        rows = parse_telegraf_json(data)
        assert rows[0]["val"] == 42.0
        assert "measurement" in rows[0]

    def test_single_dict_not_list(self):
        data = {"fields": {"v": 1.0}, "name": "cpu", "tags": {}, "timestamp": 1700000000}
        rows = parse_telegraf_json(data)
        assert len(rows) == 1

    def test_invalid_type_returns_empty(self):
        rows = parse_telegraf_json("not a list or dict")
        assert rows == []

    def test_non_dict_items_skipped(self):
        data = [{"fields": {"v": 1.0}, "name": "cpu", "tags": {}, "timestamp": 1700000000}, "bad", 42]
        rows = parse_telegraf_json(data)
        assert len(rows) == 1


# ─── Config ───────────────────────────────────────────────────────────────────

class TestConfig:

    def test_default_config(self):
        cfg = load_config(None)
        assert cfg["server"]["port"] == 8767
        assert cfg["buffer"]["rotate_mb"] == 64
        assert cfg["s3"]["enabled"] is False

    def test_env_override_output_dir(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PFC_OUTPUT_DIR", str(tmp_path))
        cfg = load_config(None)
        assert cfg["buffer"]["output_dir"] == str(tmp_path)

    def test_env_override_api_key(self, monkeypatch):
        monkeypatch.setenv("PFC_API_KEY", "mysecret")
        cfg = load_config(None)
        assert cfg["server"]["api_key"] == "mysecret"

    def test_deep_merge(self):
        base = {"a": {"x": 1, "y": 2}, "b": 3}
        override = {"a": {"x": 10, "z": 99}}
        merged = deep_merge(base, override)
        assert merged["a"]["x"] == 10
        assert merged["a"]["y"] == 2
        assert merged["a"]["z"] == 99
        assert merged["b"] == 3


# ─── HTTP Endpoints ───────────────────────────────────────────────────────────

@pytest.fixture
def test_cfg(tmp_path):
    return {
        "server": {"host": "0.0.0.0", "port": 8767, "api_key": ""},
        "buffer": {"rotate_mb": 64, "rotate_sec": 3600,
                   "output_dir": str(tmp_path), "prefix": "telegraf"},
        "pfc": {"binary": "/usr/local/bin/pfc_jsonl"},
        "s3": {"enabled": False, "bucket": "", "prefix": "telegraf/", "region": "us-east-1"},
    }


@pytest.fixture
def client(test_cfg):
    buf = PfcBuffer(test_cfg)
    app = create_app(test_cfg, buf)
    return TestClient(app)


class TestHttpEndpoints:

    def test_health(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"
        assert r.json()["version"] == VERSION

    def test_root(self, client):
        r = client.get("/")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_ingest_line_protocol(self, client):
        body = "cpu,host=srv01 usage_idle=98.0,usage_user=1.5 1700000000000000000\n"
        r = client.post("/ingest", content=body,
                        headers={"Content-Type": "text/plain; charset=utf-8"})
        assert r.status_code == 200
        assert r.json()["accepted"] == 1

    def test_ingest_multiple_lines(self, client):
        body = (
            "cpu,host=srv01 usage=50.0 1700000000000000000\n"
            "mem,host=srv01 used=1024i 1700000000000001000\n"
            "disk,host=srv01 free=100i 1700000000000002000\n"
        )
        r = client.post("/ingest", content=body,
                        headers={"Content-Type": "text/plain"})
        assert r.status_code == 200
        assert r.json()["accepted"] == 3

    def test_ingest_telegraf_json(self, client):
        data = [{"fields": {"usage_user": 1.5}, "name": "cpu",
                 "tags": {"host": "srv01"}, "timestamp": 1700000000}]
        r = client.post("/ingest", json=data)
        assert r.status_code == 200
        assert r.json()["accepted"] == 1

    def test_ingest_status(self, client):
        r = client.get("/ingest/status")
        assert r.status_code == 200
        d = r.json()
        assert "buffered_rows" in d
        assert "total_accepted" in d

    def test_ingest_empty_body(self, client):
        r = client.post("/ingest", content=b"",
                        headers={"Content-Type": "text/plain"})
        assert r.status_code == 200
        assert r.json()["accepted"] == 0

    def test_ingest_comment_only(self, client):
        r = client.post("/ingest", content=b"# comment\n",
                        headers={"Content-Type": "text/plain"})
        assert r.status_code == 200

    def test_ingest_invalid_json(self, client):
        r = client.post("/ingest", content=b"not json",
                        headers={"Content-Type": "application/json"})
        assert r.status_code == 400

    def test_status_reflects_accepted_count(self, client):
        body = "cpu usage=50.0 1700000000000000000\nmem used=1024i 1700000000000000000\n"
        client.post("/ingest", content=body, headers={"Content-Type": "text/plain"})
        r = client.get("/ingest/status")
        assert r.json()["total_accepted"] >= 2


# ─── Auth ─────────────────────────────────────────────────────────────────────

@pytest.fixture
def auth_client(tmp_path):
    cfg = {
        "server": {"host": "0.0.0.0", "port": 8767, "api_key": "secret123"},
        "buffer": {"rotate_mb": 64, "rotate_sec": 3600,
                   "output_dir": str(tmp_path), "prefix": "telegraf"},
        "pfc": {"binary": "/usr/local/bin/pfc_jsonl"},
        "s3": {"enabled": False, "bucket": "", "prefix": "", "region": "us-east-1"},
    }
    buf = PfcBuffer(cfg)
    app = create_app(cfg, buf)
    return TestClient(app)


class TestAuth:

    def test_health_no_auth_required(self, auth_client):
        r = auth_client.get("/health")
        assert r.status_code == 200

    def test_ingest_requires_auth(self, auth_client):
        r = auth_client.post("/ingest", content=b"cpu v=1.0\n",
                             headers={"Content-Type": "text/plain"})
        assert r.status_code == 401

    def test_ingest_with_api_key_header(self, auth_client):
        r = auth_client.post("/ingest", content=b"cpu v=1.0 1700000000000000000\n",
                             headers={"Content-Type": "text/plain", "x-api-key": "secret123"})
        assert r.status_code == 200

    def test_ingest_with_bearer_token(self, auth_client):
        r = auth_client.post("/ingest", content=b"cpu v=1.0 1700000000000000000\n",
                             headers={"Content-Type": "text/plain",
                                      "Authorization": "Bearer secret123"})
        assert r.status_code == 200

    def test_status_requires_auth(self, auth_client):
        r = auth_client.get("/ingest/status")
        assert r.status_code == 401

    def test_status_with_auth(self, auth_client):
        r = auth_client.get("/ingest/status", headers={"x-api-key": "secret123"})
        assert r.status_code == 200
