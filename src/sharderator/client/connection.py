"""Elasticsearch client wrapper with connection management."""

from __future__ import annotations

from elasticsearch import Elasticsearch

from sharderator.util.config import AppConfig, ConnectionConfig
from sharderator.util.logging import get_logger

log = get_logger(__name__)

MIN_ES_VERSION = (8, 7, 0)


class ClusterConnection:
    """Manages the Elasticsearch client lifecycle and cluster metadata."""

    def __init__(self) -> None:
        self._client: Elasticsearch | None = None
        self.cluster_name: str = ""
        self.cluster_health: str = ""
        self.es_version: tuple[int, ...] = (0, 0, 0)
        self.frozen_shard_limit: int = 3000
        self.frozen_shard_count: int = 0
        self.snapshot_repos: list[str] = []

    @property
    def client(self) -> Elasticsearch:
        if self._client is None:
            raise RuntimeError("Not connected")
        return self._client

    @property
    def connected(self) -> bool:
        return self._client is not None

    def connect(self, config: ConnectionConfig) -> None:
        """Establish connection to the Elasticsearch cluster."""
        kwargs: dict = {"request_timeout": 60}

        if config.cloud_id:
            kwargs["cloud_id"] = config.cloud_id
        elif config.hosts:
            kwargs["hosts"] = config.hosts

        if config.use_api_key:
            api_key = AppConfig.get_secret("api_key")
            if api_key:
                kwargs["api_key"] = api_key
        else:
            password = AppConfig.get_secret("password")
            if config.username and password:
                kwargs["basic_auth"] = (config.username, password)

        kwargs["verify_certs"] = config.verify_certs
        self._client = Elasticsearch(**kwargs)
        self._discover_cluster()

    def _discover_cluster(self) -> None:
        """Populate cluster metadata after connecting."""
        info = self.client.info()
        version_str = info["version"]["number"]
        self.es_version = tuple(int(p) for p in version_str.split(".")[:3])
        self.cluster_name = info["cluster_name"]

        if self.es_version < MIN_ES_VERSION:
            self.disconnect()
            raise RuntimeError(
                f"Elasticsearch {version_str} is below minimum "
                f"{'.'.join(str(v) for v in MIN_ES_VERSION)}"
            )

        health = self.client.cluster.health()
        self.cluster_health = health["status"]

        # Discover frozen shard limit
        settings = self.client.cluster.get_settings(include_defaults=True)
        defaults = settings.get("defaults", {})
        self.frozen_shard_limit = int(
            defaults.get("cluster", {})
            .get("max_shards_per_node", {})
            .get("frozen", 3000)
        )

        self._count_frozen_shards()

        # Discover snapshot repos
        repos = self.client.snapshot.get_repository()
        self.snapshot_repos = list(repos.keys())

        log.info(
            "connected",
            cluster=self.cluster_name,
            version=version_str,
            health=self.cluster_health,
            frozen_shards=self.frozen_shard_count,
            frozen_limit=self.frozen_shard_limit,
            repos=self.snapshot_repos,
        )

    def _count_frozen_shards(self) -> None:
        """Count ALL shards (primaries + replicas) on frozen-tier indices.

        Fix 2.2: The cluster.max_shards_per_node.frozen limit counts all shards,
        not just primaries. We must count replicas too.
        """
        shards = self.client.cat.shards(format="json", h="index,node,prirep")
        frozen_count = sum(
            1
            for s in shards
            if isinstance(s, dict)
            and (
                s.get("index", "").startswith("partial-")
                or s.get("index", "").startswith(".partial-")
            )
        )
        self.frozen_shard_count = frozen_count

    def disconnect(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
            self.cluster_name = ""
            self.cluster_health = ""
