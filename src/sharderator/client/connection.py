"""Elasticsearch client wrapper with connection management."""

from __future__ import annotations

from collections import defaultdict

from elasticsearch import Elasticsearch

from sharderator.models.frozen_topology import FrozenNodeCapacity, FrozenTopology
from sharderator.util.config import AppConfig, ConnectionConfig
from sharderator.util.logging import get_logger

log = get_logger(__name__)

MIN_ES_VERSION = (8, 7, 0)

_FROZEN_CAPABLE_ROLES = {"data_frozen", "data"}


class ClusterConnection:
    """Manages the Elasticsearch client lifecycle and cluster metadata."""

    def __init__(self) -> None:
        self._client: Elasticsearch | None = None
        self.cluster_name: str = ""
        self.cluster_health: str = ""
        self.es_version: tuple[int, ...] = (0, 0, 0)
        self.frozen_topology: FrozenTopology = FrozenTopology()
        self.snapshot_repos: list[str] = []

    # Convenience properties — most call sites just need the headline numbers
    @property
    def frozen_shard_count(self) -> int:
        return self.frozen_topology.cluster_shard_count

    @property
    def frozen_shard_limit(self) -> int:
        return self.frozen_topology.per_node_limit

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

        # Discover frozen tier topology (per-node shard capacity)
        self.frozen_topology = self._discover_frozen_topology()

        # Discover snapshot repos
        repos = self.client.snapshot.get_repository()
        self.snapshot_repos = list(repos.keys())

        hot = self.frozen_topology.hot_node
        log.info(
            "connected",
            cluster=self.cluster_name,
            version=version_str,
            health=self.cluster_health,
            frozen_shards=self.frozen_topology.cluster_shard_count,
            frozen_nodes=len(self.frozen_topology.nodes),
            per_node_limit=self.frozen_topology.per_node_limit,
            peak_node=hot.node_name if hot else "none",
            peak_utilization=f"{self.frozen_topology.max_node_utilization_pct:.1f}%",
            repos=self.snapshot_repos,
        )

    def _discover_frozen_topology(self) -> FrozenTopology:
        """Build per-node frozen shard capacity model."""
        # 1. Read per-node limit from cluster settings
        per_node_limit = 3000
        try:
            settings = self.client.cluster.get_settings(include_defaults=True)
            for scope in ("transient", "persistent", "defaults"):
                scope_data = settings.get(scope, {})
                msn = scope_data.get("cluster", {}).get("max_shards_per_node", {})
                if isinstance(msn, dict):
                    val = msn.get("frozen")
                    if val is not None:
                        per_node_limit = int(val)
                        break
                elif isinstance(msn, str) and msn:
                    per_node_limit = int(msn)
                    break
        except Exception:
            pass

        # 2. Discover frozen-capable nodes
        frozen_nodes: dict[str, FrozenNodeCapacity] = {}
        try:
            nodes_info = self.client.nodes.info(metric="os")
            for node_id, node_data in nodes_info.get("nodes", {}).items():
                roles = node_data.get("roles", [])
                if _FROZEN_CAPABLE_ROLES.intersection(roles):
                    frozen_nodes[node_data.get("name", node_id)] = FrozenNodeCapacity(
                        node_id=node_id,
                        node_name=node_data.get("name", node_id),
                        roles=roles,
                        shard_count=0,
                        shard_limit=per_node_limit,
                    )
        except Exception as e:
            log.warning("frozen_topology_nodes_info_failed", error=str(e))
            # Fall back to counting shards without node topology
            return self._frozen_topology_fallback(per_node_limit)

        if not frozen_nodes:
            # No frozen-capable nodes found — fall back
            return self._frozen_topology_fallback(per_node_limit)

        # 3. Count frozen shards per node
        try:
            shards = self.client.cat.shards(format="json", h="index,node,prirep,state")
            for s in shards:
                if not isinstance(s, dict):
                    continue
                name = s.get("index", "")
                if not (name.startswith("partial-") or name.startswith(".partial-")):
                    continue
                state = s.get("state", "")
                if state == "UNASSIGNED":
                    continue
                node_name = s.get("node", "")
                if node_name in frozen_nodes:
                    frozen_nodes[node_name].shard_count += 1
                elif node_name:
                    # Shard on a node not in our frozen-capable list — could be
                    # a node with legacy roles or one that left between API calls
                    log.debug("frozen_shard_on_unknown_node", node=node_name, index=name)
        except Exception as e:
            log.warning("frozen_topology_shard_count_failed", error=str(e))

        return FrozenTopology(
            nodes=list(frozen_nodes.values()),
            per_node_limit=per_node_limit,
        )

    def _frozen_topology_fallback(self, per_node_limit: int) -> FrozenTopology:
        """Degraded mode: count cluster-wide frozen shards without per-node breakdown."""
        total = 0
        try:
            shards = self.client.cat.shards(format="json", h="index,node,prirep")
            total = sum(
                1 for s in shards
                if isinstance(s, dict)
                and (s.get("index", "").startswith("partial-")
                     or s.get("index", "").startswith(".partial-"))
            )
        except Exception:
            pass
        log.warning("frozen_topology_degraded", total_shards=total, per_node_limit=per_node_limit)
        return FrozenTopology.single_node_fallback(total, per_node_limit)

    def disconnect(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
            self.cluster_name = ""
            self.cluster_health = ""
