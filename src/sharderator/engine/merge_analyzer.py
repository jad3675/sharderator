"""Analyze frozen indices and propose merge groups for v2 multi-index consolidation."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sharderator.models.index_info import IndexInfo

if TYPE_CHECKING:
    from sharderator.models.job import JobRecord

# Pattern to extract base name and date from common index naming conventions.
# Handles: .ds-metrics-system.cpu-default-2026.02.14-000804
#           partial-.ds-metrics-system.cpu-default-2026.02.14-000804
INDEX_PATTERN = re.compile(
    r"^(?:\.?partial-)?(?:\.ds-)?"  # optional prefixes
    r"(.+?)"  # base name (non-greedy)
    r"-(\d{4}\.\d{2}\.\d{2})"  # date YYYY.MM.DD
    r"(?:-\d+)?$"  # optional generation number
)


@dataclass
class MergeGroup:
    """A set of indices that should be merged into one."""

    base_pattern: str
    time_bucket: str  # e.g. "2026.02" for monthly
    source_indices: list[IndexInfo] = field(default_factory=list)
    merged_index_name: str = ""
    total_docs: int = 0
    total_size_bytes: int = 0
    mapping_conflicts: list[str] = field(default_factory=list)

    @property
    def source_shard_count(self) -> int:
        return sum(i.shard_count for i in self.source_indices)

    @property
    def shard_reduction(self) -> int:
        return self.source_shard_count - 1

    @property
    def total_size_mb(self) -> float:
        return self.total_size_bytes / (1024 * 1024)

    @classmethod
    def from_job_record(cls, job: "JobRecord") -> "MergeGroup":
        """Reconstruct a MergeGroup from a persisted merge job record.

        Used by the GUI tracker resume to restart merge jobs without
        requiring the operator to re-select the group in the Merge tab.
        """
        base_pattern, time_bucket = _parse_merged_name(job.index_name)

        # Reconstruct source IndexInfo from enriched_metadata
        source_indices: list[IndexInfo] = []
        if job.enriched_metadata:
            for meta in job.enriched_metadata:
                source_indices.append(IndexInfo(**meta))
        else:
            # Fallback: minimal IndexInfo from source names only.
            # The orchestrator will re-enrich if needed.
            for idx_name in job.source_frozen_indices:
                source_indices.append(IndexInfo(
                    name=idx_name, shard_count=1,
                    doc_count=0, store_size_bytes=0,
                ))

        total_size = sum(i.store_size_bytes for i in source_indices)

        return cls(
            base_pattern=base_pattern,
            time_bucket=time_bucket,
            source_indices=source_indices,
            merged_index_name=job.index_name,
            total_docs=job.expected_doc_count,
            total_size_bytes=total_size,
        )


def propose_merges(
    indices: list[IndexInfo],
    granularity: str = "monthly",
    min_group_size: int = 2,
) -> list[MergeGroup]:
    """Group frozen indices by base pattern + time bucket and propose merges."""
    buckets: dict[tuple[str, str], list[IndexInfo]] = {}

    for idx in indices:
        m = INDEX_PATTERN.match(idx.name)
        if not m:
            continue
        base = m.group(1)
        date_str = m.group(2)

        bucket = _date_to_bucket(date_str, granularity)
        key = (base, bucket)
        buckets.setdefault(key, []).append(idx)

    groups = []
    for (base, bucket), members in sorted(buckets.items()):
        if len(members) < min_group_size:
            continue
        g = MergeGroup(
            base_pattern=base,
            time_bucket=bucket,
            source_indices=sorted(members, key=lambda i: i.name),
            total_docs=sum(i.doc_count for i in members),
            total_size_bytes=sum(i.store_size_bytes for i in members),
        )
        g.merged_index_name = f"partial-.ds-{base}-{bucket}-merged"
        groups.append(g)

    # Sort by shard reduction descending — biggest wins first
    groups.sort(key=lambda g: g.shard_reduction, reverse=True)
    return groups


def union_mappings(mappings: list[dict]) -> tuple[dict, list[str]]:
    """Merge multiple index mappings into a superset.

    Returns (merged_mapping, conflicts) where conflicts is a list of
    human-readable strings describing field type conflicts. First-seen-wins
    for resolution — the conflicts list gives the operator visibility into
    what was silently resolved.
    """
    result: dict = {"properties": {}}
    conflicts: list[str] = []

    for m in mappings:
        props = m.get("properties", {})
        for field_name, field_def in props.items():
            if field_name not in result["properties"]:
                result["properties"][field_name] = field_def
            else:
                # Check for type conflicts
                existing_type = result["properties"][field_name].get("type")
                new_type = field_def.get("type")
                if existing_type and new_type and existing_type != new_type:
                    conflicts.append(
                        f"'{field_name}': {existing_type} kept, {new_type} dropped"
                    )
                    # Keep the first definition

    # Preserve top-level mapping keys like _routing, _source, dynamic, etc.
    for m in mappings:
        for key, val in m.items():
            if key != "properties" and key not in result:
                result[key] = val

    return result, conflicts


def _date_to_bucket(date_str: str, granularity: str) -> str:
    """Convert '2026.02.14' to a time bucket string."""
    parts = date_str.split(".")
    year, month = parts[0], parts[1]
    if granularity == "monthly":
        return f"{year}.{month}"
    elif granularity == "quarterly":
        q = (int(month) - 1) // 3 + 1
        return f"{year}.Q{q}"
    elif granularity == "yearly":
        return year
    return f"{year}.{month}"


def _parse_merged_name(name: str) -> tuple[str, str]:
    """Extract base_pattern and time_bucket from a merged index name.

    Handles both naming formats:
      partial-.ds-metrics-system.cpu-default-2026.02-merged  (final frozen mount)
      sharderator-merged-metrics-system.cpu-default-2026.02  (intermediate working name)

    The time bucket is the last hyphen-delimited segment. This works for
    monthly (2026.02), quarterly (2026.Q1), and yearly (2026) buckets.
    """
    stripped = name

    # Strip the final frozen mount format
    if stripped.endswith("-merged"):
        stripped = stripped[: -len("-merged")]
        for prefix in ("partial-.ds-", ".partial-.ds-", "partial-", ".partial-"):
            if stripped.startswith(prefix):
                stripped = stripped[len(prefix):]
                break

    # Strip the intermediate working name format
    elif stripped.startswith("sharderator-merged-"):
        stripped = stripped[len("sharderator-merged-"):]

    # The time bucket is the last segment
    parts = stripped.rsplit("-", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return stripped, "unknown"


@dataclass
class MergeGroupValidation:
    """Validation results for a merge group."""

    group: MergeGroup
    missing_snapshots: list[str] = field(default_factory=list)
    mapping_conflicts: list[str] = field(default_factory=list)
    is_viable: bool = True


def validate_merge_group(
    client,
    group: MergeGroup,
) -> MergeGroupValidation:
    """Quick validation of a merge group before execution.

    Checks snapshot metadata accessibility and samples mapping drift
    between first and last index in the group.
    """
    result = MergeGroupValidation(group=group)

    for idx in group.source_indices:
        try:
            settings = client.indices.get_settings(index=idx.name)
            snap_meta = (
                settings[idx.name]["settings"]["index"]
                .get("store", {})
                .get("snapshot", {})
            )
            if not snap_meta.get("snapshot_name"):
                result.missing_snapshots.append(idx.name)
                result.is_viable = False
        except Exception:
            result.missing_snapshots.append(idx.name)
            result.is_viable = False

    # Sample mapping drift between first and last index
    if len(group.source_indices) >= 2:
        try:
            first_name = group.source_indices[0].name
            last_name = group.source_indices[-1].name
            first_mapping = client.indices.get_mapping(index=first_name)
            last_mapping = client.indices.get_mapping(index=last_name)
            first_props = set(
                first_mapping[first_name]["mappings"]
                .get("properties", {})
                .keys()
            )
            last_props = set(
                last_mapping[last_name]["mappings"]
                .get("properties", {})
                .keys()
            )
            diff = first_props.symmetric_difference(last_props)
            if diff:
                result.mapping_conflicts.append(
                    f"Field drift between first/last: {', '.join(sorted(diff)[:5])}"
                    + (f" (+{len(diff) - 5} more)" if len(diff) > 5 else "")
                )

            # Also check for type conflicts between first and last
            sampled = [
                first_mapping[first_name]["mappings"],
                last_mapping[last_name]["mappings"],
            ]
            _, type_conflicts = union_mappings(sampled)
            for c in type_conflicts:
                result.mapping_conflicts.append(f"Type conflict: {c}")
        except Exception:
            pass

    return result
