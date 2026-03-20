"""Tests for YAML-based server configuration loading."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from cascadq.config import ServerConfig, load_server_config


class TestLoadServerConfig:
    def test_full_config(self, tmp_path: Path) -> None:
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("""\
server:
  host: "127.0.0.1"
  port: 9090
  log_level: debug

storage:
  backend: s3
  prefix: "prod/"
  bucket: my-bucket
  endpoint_url: https://example.com
  region: us-east-1
  hedge_after_seconds: 1.5

broker:
  heartbeat_timeout_seconds: 10.0
  compaction_interval_seconds: 120.0
""")
        cfg = load_server_config(cfg_file)

        assert cfg.server.host == "127.0.0.1"
        assert cfg.server.port == 9090
        assert cfg.server.log_level == "debug"
        assert cfg.storage.backend == "s3"
        assert cfg.storage.prefix == "prod/"
        assert cfg.storage.bucket == "my-bucket"
        assert cfg.storage.endpoint_url == "https://example.com"
        assert cfg.storage.region == "us-east-1"
        assert cfg.storage.hedge_after_seconds == 1.5
        assert cfg.broker.heartbeat_timeout_seconds == 10.0
        assert cfg.broker.compaction_interval_seconds == 120.0

    def test_partial_config_fills_defaults(self, tmp_path: Path) -> None:
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("storage:\n  backend: s3\n  bucket: b\n")

        cfg = load_server_config(cfg_file)

        assert cfg.server.host == "0.0.0.0"
        assert cfg.server.port == 8000
        assert cfg.server.log_level == "info"
        assert cfg.storage.backend == "s3"
        assert cfg.storage.bucket == "b"
        assert cfg.storage.region == "auto"
        assert cfg.broker.heartbeat_timeout_seconds == 30.0

    def test_empty_file_returns_defaults(self, tmp_path: Path) -> None:
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("")

        cfg = load_server_config(cfg_file)

        assert cfg == ServerConfig()

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            load_server_config(tmp_path / "nope.yaml")

    def test_invalid_log_level_raises(self, tmp_path: Path) -> None:
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("server:\n  log_level: trace\n")

        with pytest.raises(ValidationError, match="log_level"):
            load_server_config(cfg_file)

    def test_invalid_backend_raises(self, tmp_path: Path) -> None:
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("storage:\n  backend: redis\n")

        with pytest.raises(ValidationError, match="backend"):
            load_server_config(cfg_file)

    def test_unknown_top_level_key_raises(self, tmp_path: Path) -> None:
        """Extra keys in the YAML should be rejected."""
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("bogus:\n  key: value\n")

        with pytest.raises(ValidationError):
            load_server_config(cfg_file)
