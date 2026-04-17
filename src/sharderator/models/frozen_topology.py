"""Frozen tier topology — per-node shard capacity model."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class FrozenNodeCapacity:
    """Shard capacity for a single frozen-tier node."""
    node_id: str
    node_name: str
    roles: list[str] = field(default_factory=list)
    shard_count: int = 0
    shard_limit: int = 3000

    @property
    def utilization_pct(self) -> float:
        if self.shard_limit <= 0:
            return 0.0
        return self.shard_count / self.shard_limit * 100


@dataclass
class FrozenTopology:
    """Frozen tier topology with per-node capacity tracking.

    The binding constraint is the per-node limit, not the cluster total.
    `cluster.max_shards_per_node.frozen` is a per-node cap — dividing
    cluster-wide totals by it is only correct for single-node clusters.
    """
    nodes: list[FrozenNodeCapacity] = field(default_factory=list)
    per_node_limit: int = 3000

    @property
    def cluster_shard_count(self) -> int:
        return sum(n.shard_count for n in self.nodes)

    @property
    def cluster_shard_capacity(self) -> int:
        return len(self.nodes) * self.per_node_limit

    @property
    def max_node_utilization_pct(self) -> float:
        if not self.nodes:
            return 0.0
        return max(n.utilization_pct for n in self.nodes)

    @property
    def cluster_utilization_pct(self) -> float:
        cap = self.cluster_shard_capacity
        if cap <= 0:
            return 0.0
        return self.cluster_shard_count / cap * 100

    @property
    def hot_node(self) -> FrozenNodeCapacity | None:
        if not self.nodes:
            return None
        return max(self.nodes, key=lambda n: n.utilization_pct)

    @property
    def empty_nodes(self) -> int:
        return sum(1 for n in self.nodes if n.shard_count == 0)

    @property
    def is_hotspot(self) -> bool:
        """True if peak node is 20+ points above cluster average."""
        if len(self.nodes) <= 1:
            return False
        return self.max_node_utilization_pct - self.cluster_utilization_pct >= 20

    def to_dict(self) -> dict:
        return {
            "per_node_limit": self.per_node_limit,
            "cluster_capacity": self.cluster_shard_capacity,
            "cluster_shard_count": self.cluster_shard_count,
            "node_count": len(self.nodes),
            "max_node_utilization_pct": round(self.max_node_utilization_pct, 1),
            "cluster_utilization_pct": round(self.cluster_utilization_pct, 1),
            "hot_node": self.hot_node.node_name if self.hot_node else None,
            "empty_nodes": self.empty_nodes,
            "is_hotspot": self.is_hotspot,
            "nodes": [
                {
                    "name": n.node_name,
                    "shards": n.shard_count,
                    "limit": n.shard_limit,
                    "utilization_pct": round(n.utilization_pct, 1),
                }
                for n in sorted(self.nodes, key=lambda x: x.utilization_pct, reverse=True)
            ],
        }

    @classmethod
    def single_node_fallback(cls, shard_count: int, per_node_limit: int = 3000) -> "FrozenTopology":
        """Create a synthetic single-node topology for degraded mode."""
        node = FrozenNodeCapacity(
            node_id="unknown",
            node_name="cluster-total",
            roles=["data_frozen"],
            shard_count=shard_count,
            shard_limit=per_node_limit,
        )
        return cls(nodes=[node], per_node_limit=per_node_limit)
