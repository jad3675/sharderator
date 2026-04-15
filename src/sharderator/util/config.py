"""Settings persistence using YAML + keyring for secrets."""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from pathlib import Path

import keyring
import yaml

CONFIG_DIR = Path.home() / ".sharderator"
CONFIG_FILE = CONFIG_DIR / "config.yaml"
KEYRING_SERVICE = "sharderator"


@dataclass
class ConnectionConfig:
    cloud_id: str = ""
    hosts: list[str] = field(default_factory=list)
    username: str = ""
    use_api_key: bool = True
    verify_certs: bool = True


@dataclass
class OperationConfig:
    target_shard_count: int = 1
    working_tier: str = "data_warm,data_hot,data_content"
    snapshot_repo: str = ""
    recovery_timeout_minutes: int = 30
    delete_old_snapshots: bool = False
    dry_run: bool = True
    merge_granularity: str = "monthly"
    disk_safety_margin: float = 0.30
    restore_batch_size: int = 3
    reindex_requests_per_second: float = 5000.0
    allow_yellow_cluster: bool = True
    ignore_circuit_breakers: bool = False
    circuit_breaker_wait_minutes: int = 10
    batch_backpressure_timeout_minutes: int = 15


@dataclass
class AppConfig:
    connection: ConnectionConfig = field(default_factory=ConnectionConfig)
    operation: OperationConfig = field(default_factory=OperationConfig)

    def save(self) -> None:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        data = {
            "connection": dataclasses.asdict(self.connection),
            "operation": dataclasses.asdict(self.operation),
        }
        with open(CONFIG_FILE, "w") as f:
            yaml.dump(data, f, default_flow_style=False)

    @classmethod
    def load(cls) -> AppConfig:
        cfg = cls()
        if CONFIG_FILE.exists():
            with open(CONFIG_FILE) as f:
                data = yaml.safe_load(f) or {}
            conn = data.get("connection", {})
            valid_conn = {f.name for f in dataclasses.fields(ConnectionConfig)}
            cfg.connection = ConnectionConfig(**{k: v for k, v in conn.items() if k in valid_conn})

            op = data.get("operation", {})
            valid_op = {f.name for f in dataclasses.fields(OperationConfig)}
            cfg.operation = OperationConfig(**{k: v for k, v in op.items() if k in valid_op})
        return cfg

    @staticmethod
    def store_secret(key: str, value: str) -> None:
        keyring.set_password(KEYRING_SERVICE, key, value)

    @staticmethod
    def get_secret(key: str) -> str:
        return keyring.get_password(KEYRING_SERVICE, key) or ""
