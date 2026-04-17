"""Frozen tier health analysis — categorize indices and identify optimization opportunities."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

from sharderator.engine.merge_analyzer import INDEX_PATTERN, _date_to_bucket, propose_merges
from sharderator.models.frozen_topology import FrozenTopology
from sharderator.models.index_info import IndexInfo


@dataclass
class PatternSummary:
    """Shard consumption summary for a single base pattern."""
    base_pattern: str
    index_count: int = 0
    total_shards: int = 0
    total_size_bytes: int = 0
    total_docs: int = 0
    shards_after_monthly_merge: int = 0
    shards_after_quarterly_merge: int = 0
    shards_after_yearly_merge: int = 0
    monthly_groups: int = 0
    quarterly_groups: int = 0
    yearly_groups: int = 0

    @property
    def total_size_mb(self) -> float:
        return self.total_size_bytes / (1024 * 1024)

    @property
    def monthly_savings(self) -> int:
        return self.total_shards - self.shards_after_monthly_merge

    @property
    def quarterly_savings(self) -> int:
        return self.total_shards - self.shards_after_quarterly_merge

    @property
    def yearly_savings(self) -> int:
        return self.total_shards - self.shards_after_yearly_merge


@dataclass
class FrozenAnalysis:
    """Complete frozen tier health analysis."""
    total_indices: int = 0
    total_shards: int = 0
    topology: FrozenTopology = field(default_factory=FrozenTopology)

    # Categorized indices
    over_sharded: list[IndexInfo] = field(default_factory=list)
    mergeable_patterns: list[PatternSummary] = field(default_factory=list)
    already_optimal: int = 0
    already_sharderated: int = 0
    already_merged: int = 0
    unrecognized: list[IndexInfo] = field(default_factory=list)

    # Aggregate savings
    shrink_savings: int = 0
    merge_monthly_savings: int = 0
    merge_quarterly_savings: int = 0
    merge_yearly_savings: int = 0

    @property
    def peak_node_utilization_pct(self) -> float:
        return self.topology.max_node_utilization_pct

    @property
    def cluster_utilization_pct(self) -> float:
        return self.topology.cluster_utilization_pct

    @property
    def frozen_limit(self) -> int:
        """Per-node limit — convenience for renderers."""
        return self.topology.per_node_limit

    @property
    def cluster_capacity(self) -> int:
        return self.topology.cluster_shard_capacity

    def budget_status(self) -> str:
        pct = self.peak_node_utilization_pct
        if self.topology.is_hotspot:
            return "HOTSPOT"
        if pct >= 100:
            return "OVER LIMIT"
        elif pct >= 90:
            return "CRITICAL"
        elif pct >= 75:
            return "WARNING"
        return "OK"

    def to_dict(self) -> dict:
        hot = self.topology.hot_node
        return {
            "total_indices": self.total_indices,
            "total_shards": self.total_shards,
            "peak_node_utilization_pct": round(self.peak_node_utilization_pct, 1),
            "cluster_utilization_pct": round(self.cluster_utilization_pct, 1),
            "budget_status": self.budget_status(),
            "topology": self.topology.to_dict(),
            "over_sharded_count": len(self.over_sharded),
            "over_sharded_shards": sum(i.shard_count for i in self.over_sharded),
            "over_sharded": [
                {"name": i.name, "shards": i.shard_count, "target": i.target_shard_count,
                 "savings": i.shard_reduction, "size_mb": round(i.store_size_mb, 1)}
                for i in self.over_sharded
            ],
            "mergeable_patterns": [
                {
                    "pattern": p.base_pattern,
                    "indices": p.index_count,
                    "shards": p.total_shards,
                    "size_mb": round(p.total_size_mb, 1),
                    "monthly_groups": p.monthly_groups,
                    "shards_after_monthly": p.shards_after_monthly_merge,
                    "monthly_savings": p.monthly_savings,
                    "quarterly_groups": p.quarterly_groups,
                    "shards_after_quarterly": p.shards_after_quarterly_merge,
                    "quarterly_savings": p.quarterly_savings,
                }
                for p in self.mergeable_patterns
            ],
            "already_optimal": self.already_optimal,
            "already_sharderated": self.already_sharderated,
            "already_merged": self.already_merged,
            "unrecognized": len(self.unrecognized),
            "shrink_savings": self.shrink_savings,
            "merge_monthly_savings": self.merge_monthly_savings,
            "merge_quarterly_savings": self.merge_quarterly_savings,
        }

    def format_text(self) -> str:
        """Format as a human-readable text report."""
        lines: list[str] = []
        topo = self.topology
        hot = topo.hot_node
        status = self.budget_status()

        # Per-node peak bar
        peak_pct = self.peak_node_utilization_pct
        bar_width = 40
        filled = min(int(peak_pct / 100 * bar_width), bar_width)
        bar = "█" * filled + "░" * (bar_width - filled)
        lines.append(f"Frozen Tier Budget")
        lines.append(f"  Per-node peak:  [{bar}] {peak_pct:.1f}%  {status}")
        if hot:
            lines.append(f"                  {hot.shard_count:,} / {hot.shard_limit:,} shards on {hot.node_name}")
        lines.append("")
        lines.append(
            f"  Cluster-wide:   {topo.cluster_shard_count:,} / {topo.cluster_shard_capacity:,} shards "
            f"across {len(topo.nodes)} frozen node(s) ({self.cluster_utilization_pct:.1f}%)"
        )

        # Per-node breakdown
        if len(topo.nodes) > 1:
            lines.append("")
            lines.append("  Per-node breakdown:")
            for n in sorted(topo.nodes, key=lambda x: x.utilization_pct, reverse=True):
                marker = "  ← hot" if hot and n.node_name == hot.node_name else ""
                lines.append(
                    f"    {n.node_name:<30} {n.shard_count:>6} / {n.shard_limit:>6}   {n.utilization_pct:>5.1f}%{marker}"
                )
        lines.append("")

        # Over-sharded singles
        if self.over_sharded:
            lines.append(f"Over-Sharded Indices ({len(self.over_sharded)} indices, {self.shrink_savings} shards reclaimable via shrink):")
            for i in self.over_sharded[:15]:
                lines.append(f"  {i.name:<60} {i.shard_count:>4} → {i.target_shard_count} ({i.shard_reduction} saved)")
            if len(self.over_sharded) > 15:
                lines.append(f"  ... and {len(self.over_sharded) - 15} more")
            lines.append("")

        # Mergeable patterns
        if self.mergeable_patterns:
            lines.append(f"Mergeable Patterns ({len(self.mergeable_patterns)} patterns, {self.merge_monthly_savings} shards reclaimable via monthly merge):")
            lines.append(f"  {'Pattern':<45} {'Indices':>8} {'Shards':>8} {'→Monthly':>10} {'Saved':>8}")
            lines.append(f"  {'-'*45} {'-'*8} {'-'*8} {'-'*10} {'-'*8}")
            for p in self.mergeable_patterns[:20]:
                lines.append(
                    f"  {p.base_pattern:<45} {p.index_count:>8} {p.total_shards:>8} "
                    f"{p.shards_after_monthly_merge:>10} {p.monthly_savings:>8}"
                )
            if len(self.mergeable_patterns) > 20:
                lines.append(f"  ... and {len(self.mergeable_patterns) - 20} more")
            lines.append("")

        # Summary
        lines.append("Summary:")
        lines.append(f"  Already optimal (1 shard):  {self.already_optimal}")
        lines.append(f"  Already sharderated:        {self.already_sharderated}")
        lines.append(f"  Already merged:             {self.already_merged}")
        if self.unrecognized:
            lines.append(f"  Unrecognized naming:        {len(self.unrecognized)}")
        lines.append("")

        # Recommendations
        total_reclaimable = self.shrink_savings + self.merge_monthly_savings
        if total_reclaimable > 0:
            lines.append(f"Potential: {total_reclaimable:,} shards reclaimable")
            lines.append(f"  → Shrink mode:  {self.shrink_savings:,} shards from {len(self.over_sharded)} indices")
            lines.append(f"  → Merge monthly: {self.merge_monthly_savings:,} shards from {len(self.mergeable_patterns)} patterns")
            after = self.total_shards - total_reclaimable
            cap = self.cluster_capacity or 1
            after_pct = after / cap * 100
            lines.append(f"  → Budget after:  {after:,} / {cap:,} cluster capacity ({after_pct:.1f}%)")
        else:
            lines.append("Frozen tier is already optimally sharded.")

        return "\n".join(lines)

    def format_html(self, cluster_name: str = "", timestamp: str = "") -> str:
        """Format as a self-contained HTML report suitable for printing to PDF."""
        import time as _time
        ts = timestamp or _time.strftime("%Y-%m-%d %H:%M:%S")
        topo = self.topology
        hot = topo.hot_node
        status = self.budget_status()
        status_colors = {"OK": "#43a047", "WARNING": "#ffc107", "CRITICAL": "#ff9800",
                         "OVER LIMIT": "#e53935", "HOTSPOT": "#ff5722"}
        status_color = status_colors.get(status, "#888")
        peak_pct = min(self.peak_node_utilization_pct, 100)
        cluster_pct = min(self.cluster_utilization_pct, 100)
        total_reclaimable = self.shrink_savings + self.merge_monthly_savings

        # Per-node table rows
        node_rows = ""
        for n in sorted(topo.nodes, key=lambda x: x.utilization_pct, reverse=True):
            is_hot = hot and n.node_name == hot.node_name and len(topo.nodes) > 1
            bg = "background:#fff3e0;" if is_hot else ""
            marker = " ← hot" if is_hot else ""
            node_rows += (
                f"<tr style='{bg}'><td>{n.node_name}{marker}</td>"
                f"<td class='r'>{n.shard_count:,}</td><td class='r'>{n.shard_limit:,}</td>"
                f"<td class='r'>{n.utilization_pct:.1f}%</td></tr>\n"
            )

        rows_over = ""
        for i in self.over_sharded:
            savings = i.shard_count - i.target_shard_count
            rows_over += (
                f"<tr><td>{i.name}</td><td class='r'>{i.shard_count}</td>"
                f"<td class='r'>{i.target_shard_count}</td><td class='r'>{savings}</td>"
                f"<td class='r'>{i.store_size_mb:.1f}</td></tr>\n"
            )

        rows_merge = ""
        for p in self.mergeable_patterns:
            rows_merge += (
                f"<tr><td>{p.base_pattern}</td><td class='r'>{p.index_count}</td>"
                f"<td class='r'>{p.total_shards}</td>"
                f"<td class='r'>{p.shards_after_monthly_merge}</td>"
                f"<td class='r'>{p.monthly_savings}</td>"
                f"<td class='r'>{p.total_size_mb:.1f}</td></tr>\n"
            )

        after = self.total_shards - total_reclaimable
        cap = self.cluster_capacity or 1
        after_pct = after / cap * 100

        hot_label = f"{hot.shard_count:,} / {hot.shard_limit:,} on {hot.node_name}" if hot else ""

        return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>Sharderator — Frozen Tier Analysis</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         max-width: 960px; margin: 20px auto; color: #333; font-size: 14px; }}
  h1 {{ color: #1a237e; border-bottom: 2px solid #1a237e; padding-bottom: 8px; }}
  h2 {{ color: #283593; margin-top: 24px; }}
  .meta {{ color: #666; font-size: 12px; margin-bottom: 16px; }}
  .budget-bar {{ background: #e0e0e0; border-radius: 6px; height: 32px; position: relative;
                 margin: 8px 0; overflow: hidden; }}
  .budget-fill {{ height: 100%; border-radius: 6px; min-width: 2px; }}
  .budget-text {{ position: absolute; top: 0; left: 0; right: 0; height: 32px;
                  line-height: 32px; text-align: center; font-weight: bold; color: #333; }}
  .budget-small {{ height: 22px; }}
  .budget-small .budget-text {{ height: 22px; line-height: 22px; font-size: 12px; }}
  .cards {{ display: flex; gap: 12px; margin: 16px 0; }}
  .card {{ flex: 1; border: 1px solid #ccc; border-radius: 6px; padding: 12px; text-align: center; }}
  .card h3 {{ margin: 0 0 4px 0; font-size: 13px; color: #555; }}
  .card .value {{ font-size: 18px; font-weight: bold; color: #1a237e; }}
  .card .sub {{ font-size: 11px; color: #888; }}
  table {{ width: 100%; border-collapse: collapse; margin: 8px 0; font-size: 13px; }}
  th {{ background: #f5f5f5; text-align: left; padding: 6px 8px; border-bottom: 2px solid #ddd; }}
  td {{ padding: 4px 8px; border-bottom: 1px solid #eee; }}
  tr:nth-child(even) {{ background: #fafafa; }}
  .r {{ text-align: right; }}
  .rec {{ background: #e8f5e9; border: 1px solid #a5d6a7; border-radius: 6px;
          padding: 12px 16px; margin-top: 16px; }}
  .rec b {{ color: #2e7d32; }}
  @media print {{ body {{ font-size: 11px; }} .budget-bar {{ print-color-adjust: exact; -webkit-print-color-adjust: exact; }} }}
</style></head><body>
<h1>Sharderator — Frozen Tier Analysis</h1>
<div class="meta">Cluster: {cluster_name or 'N/A'} &nbsp;|&nbsp; Generated: {ts} &nbsp;|&nbsp; {len(topo.nodes)} frozen node(s)</div>

<h2>Per-Node Peak — {status}</h2>
<div class="budget-bar">
  <div class="budget-fill" style="background:{status_color};width:{peak_pct:.1f}%"></div>
  <div class="budget-text">{hot_label} ({self.peak_node_utilization_pct:.1f}%)</div>
</div>

<div class="budget-bar budget-small">
  <div class="budget-fill" style="background:#78909c;width:{cluster_pct:.1f}%"></div>
  <div class="budget-text">Cluster: {topo.cluster_shard_count:,} / {topo.cluster_shard_capacity:,} ({self.cluster_utilization_pct:.1f}%)</div>
</div>

{"<h2>Per-Node Breakdown</h2><table><tr><th>Node</th><th class='r'>Shards</th><th class='r'>Limit</th><th class='r'>Utilization</th></tr>" + node_rows + "</table>" if len(topo.nodes) > 1 else ""}

<div class="cards">
  <div class="card"><h3>Over-Sharded</h3><div class="value">{len(self.over_sharded)}</div><div class="sub">{self.shrink_savings:,} shards reclaimable</div></div>
  <div class="card"><h3>Mergeable</h3><div class="value">{len(self.mergeable_patterns)}</div><div class="sub">{self.merge_monthly_savings:,} shards reclaimable</div></div>
  <div class="card"><h3>Processed</h3><div class="value">{self.already_sharderated + self.already_merged}</div><div class="sub">{self.already_sharderated} sharderated, {self.already_merged} merged</div></div>
</div>
<div class="meta">Of {self.total_indices:,} total indices, {self.already_optimal} are already at 1 shard.</div>

<h2>Over-Sharded Indices ({len(self.over_sharded)})</h2>
<table>
<tr><th>Index Name</th><th class="r">Shards</th><th class="r">Target</th><th class="r">Savings</th><th class="r">Size (MB)</th></tr>
{rows_over if rows_over else '<tr><td colspan="5" style="color:#888">None</td></tr>'}
</table>

<h2>Mergeable Patterns ({len(self.mergeable_patterns)})</h2>
<table>
<tr><th>Base Pattern</th><th class="r">Indices</th><th class="r">Shards</th><th class="r">→ Monthly</th><th class="r">Savings</th><th class="r">Size (MB)</th></tr>
{rows_merge if rows_merge else '<tr><td colspan="6" style="color:#888">None</td></tr>'}
</table>

<div class="rec">
  <b>{total_reclaimable:,} shards reclaimable</b><br>
  Shrink: {self.shrink_savings:,} from {len(self.over_sharded)} indices &nbsp;|&nbsp;
  Merge (monthly): {self.merge_monthly_savings:,} from {len(self.mergeable_patterns)} patterns<br>
  Budget after: {after:,} / {cap:,} cluster capacity ({after_pct:.1f}%)
</div>

<div class="meta" style="margin-top:24px">Generated by Sharderator</div>
</body></html>"""


def analyze_frozen_tier(
    indices: list[IndexInfo],
    topology: FrozenTopology,
    min_shards_for_shrink: int = 2,
) -> FrozenAnalysis:
    """Analyze frozen indices and categorize optimization opportunities."""
    analysis = FrozenAnalysis(
        total_indices=len(indices),
        total_shards=sum(i.shard_count for i in indices),
        topology=topology,
    )

    # Categorize each index
    for idx in indices:
        name = idx.name

        # Already processed by Sharderator?
        if name.endswith("-sharderated"):
            analysis.already_sharderated += 1
        elif name.endswith("-merged"):
            analysis.already_merged += 1

        # Over-sharded? (candidate for shrink)
        if idx.shard_count >= min_shards_for_shrink and idx.shard_count > idx.target_shard_count:
            analysis.over_sharded.append(idx)

        # Already optimal?
        elif idx.shard_count <= 1:
            analysis.already_optimal += 1

        # Check if name matches the date pattern (mergeable)
        m = INDEX_PATTERN.match(name)
        if not m:
            analysis.unrecognized.append(idx)

    # Sort over-sharded by shard count descending (biggest offenders first)
    analysis.over_sharded.sort(key=lambda i: i.shard_count, reverse=True)
    analysis.shrink_savings = sum(i.shard_reduction for i in analysis.over_sharded)

    # Analyze merge potential by pattern
    # Group indices by base pattern to build PatternSummary
    pattern_indices: dict[str, list[IndexInfo]] = {}
    for idx in indices:
        m = INDEX_PATTERN.match(idx.name)
        if m:
            base = m.group(1)
            pattern_indices.setdefault(base, []).append(idx)

    for base, members in sorted(pattern_indices.items()):
        if len(members) < 2:
            continue  # Need at least 2 to merge

        monthly_groups = propose_merges(members, "monthly")
        quarterly_groups = propose_merges(members, "quarterly")
        yearly_groups = propose_merges(members, "yearly")

        # Only include if there's actual merge potential
        monthly_savings = sum(g.shard_reduction for g in monthly_groups)
        if monthly_savings == 0 and not quarterly_groups:
            continue

        summary = PatternSummary(
            base_pattern=base,
            index_count=len(members),
            total_shards=sum(i.shard_count for i in members),
            total_size_bytes=sum(i.store_size_bytes for i in members),
            total_docs=sum(i.doc_count for i in members),
            monthly_groups=len(monthly_groups),
            shards_after_monthly_merge=sum(i.shard_count for i in members) - monthly_savings,
            quarterly_groups=len(quarterly_groups),
            shards_after_quarterly_merge=sum(i.shard_count for i in members) - sum(g.shard_reduction for g in quarterly_groups),
            yearly_groups=len(yearly_groups),
            shards_after_yearly_merge=sum(i.shard_count for i in members) - sum(g.shard_reduction for g in yearly_groups),
        )
        analysis.mergeable_patterns.append(summary)

    # Sort patterns by monthly savings descending
    analysis.mergeable_patterns.sort(key=lambda p: p.monthly_savings, reverse=True)
    analysis.merge_monthly_savings = sum(p.monthly_savings for p in analysis.mergeable_patterns)
    analysis.merge_quarterly_savings = sum(p.quarterly_savings for p in analysis.mergeable_patterns)
    analysis.merge_yearly_savings = sum(p.yearly_savings for p in analysis.mergeable_patterns)

    return analysis
