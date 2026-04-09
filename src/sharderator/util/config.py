"""Settings persistence using YAML + keyring for secrets."""

from __future__ import annotations

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


@dataclass
class AppConfig:
    connection: ConnectionConfig = field(default_factory=ConnectionConfig)
    operation: OperationConfig = field(default_factory=OperationConfig)

    def save(self) -> None:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        data = {
            "connection": {
                "cloud_id": self.connection.cloud_id,
                "hosts": self.connection.hosts,
                "username": self.connection.username,
                "use_api_key": self.connection.use_api_key,
                "verify_certs": self.connection.verify_certs,
            },
            "operation": {
                "target_shard_count": self.operation.target_shard_count,
                "working_tier": self.operation.working_tier,
                "snapshot_repo": self.operation.snapshot_repo,
                "recovery_timeout_minutes": self.operation.recovery_timeout_minutes,
                "delete_old_snapshots": self.operation.delete_old_snapshots,
                "dry_run": self.operation.dry_run,
                "merge_granularity": self.operation.merge_granularity,
                "disk_safety_margin": self.operation.disk_safety_margin,
                "restore_batch_size": self.operation.restore_batch_size,
                "reindex_requests_per_second": self.operation.reindex_requests_per_second,
                "allow_yellow_cluster": self.operation.allow_yellow_cluster,
                "ignore_circuit_breakers": self.operation.ignore_circuit_breakers,
            },
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
            cfg.connection = ConnectionConfig(
                cloud_id=conn.get("cloud_id", ""),
                hosts=conn.get("hosts", []),
                username=conn.get("username", ""),
                use_api_key=conn.get("use_api_key", True),
                verify_certs=conn.get("verify_certs", True),
            )
            op = data.get("operation", {})
            cfg.operation = OperationConfig(
                target_shard_count=op.get("target_shard_count", 1),
                working_tier=op.get("working_tier", "data_warm,data_hot,data_content"),
                snapshot_repo=op.get("snapshot_repo", ""),
                recovery_timeout_minutes=op.get("recovery_timeout_minutes", 30),
                delete_old_snapshots=op.get("delete_old_snapshots", False),
                dry_run=op.get("dry_run", True),
                merge_granularity=op.get("merge_granularity", "monthly"),
                disk_safety_margin=op.get("disk_safety_margin", 0.30),
                restore_batch_size=op.get("restore_batch_size", 3),
                reindex_requests_per_second=op.get("reindex_requests_per_second", 5000.0),
                allow_yellow_cluster=op.get("allow_yellow_cluster", True),
                ignore_circuit_breakers=op.get("ignore_circuit_breakers", False),
            )
        return cfg

    @staticmethod
    def store_secret(key: str, value: str) -> None:
        keyring.set_password(KEYRING_SERVICE, key, value)

    @staticmethod
    def get_secret(key: str) -> str:
        return keyring.get_password(KEYRING_SERVICE, key) or ""
