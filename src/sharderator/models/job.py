"""Job state tracking for the consolidation pipeline."""

from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field
from typing import Any


class JobState(enum.Enum):
    PENDING = "PENDING"
    ANALYZING = "ANALYZING"
    RESTORING = "RESTORING"
    AWAITING_RECOVERY = "AWAITING_RECOVERY"
    SHRINKING = "SHRINKING"
    AWAITING_SHRINK = "AWAITING_SHRINK"
    MERGING = "MERGING"
    AWAITING_MERGE = "AWAITING_MERGE"
    FORCE_MERGING = "FORCE_MERGING"
    SNAPSHOTTING = "SNAPSHOTTING"
    AWAITING_SNAPSHOT = "AWAITING_SNAPSHOT"
    REMOUNTING = "REMOUNTING"
    VERIFYING = "VERIFYING"
    SWAPPING = "SWAPPING"
    CLEANING_UP = "CLEANING_UP"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class JobType(enum.Enum):
    SHRINK = "SHRINK"
    MERGE = "MERGE"


# Valid forward transitions
TRANSITIONS: dict[JobState, list[JobState]] = {
    JobState.PENDING: [JobState.ANALYZING, JobState.FAILED],
    JobState.ANALYZING: [JobState.RESTORING, JobState.FAILED],
    JobState.RESTORING: [JobState.AWAITING_RECOVERY, JobState.FAILED],
    JobState.AWAITING_RECOVERY: [JobState.SHRINKING, JobState.MERGING, JobState.FAILED],
    JobState.SHRINKING: [JobState.AWAITING_SHRINK, JobState.FAILED],
    JobState.AWAITING_SHRINK: [JobState.FORCE_MERGING, JobState.FAILED],
    JobState.MERGING: [JobState.AWAITING_MERGE, JobState.FAILED],
    JobState.AWAITING_MERGE: [JobState.FORCE_MERGING, JobState.FAILED],
    JobState.FORCE_MERGING: [JobState.SNAPSHOTTING, JobState.FAILED],
    JobState.SNAPSHOTTING: [JobState.AWAITING_SNAPSHOT, JobState.FAILED],
    JobState.AWAITING_SNAPSHOT: [JobState.REMOUNTING, JobState.FAILED],
    JobState.REMOUNTING: [JobState.VERIFYING, JobState.FAILED],
    JobState.VERIFYING: [JobState.SWAPPING, JobState.CLEANING_UP, JobState.FAILED],
    JobState.SWAPPING: [JobState.CLEANING_UP, JobState.FAILED],
    JobState.CLEANING_UP: [JobState.COMPLETED, JobState.FAILED],
    JobState.COMPLETED: [],
    JobState.FAILED: [],
}


@dataclass
class JobRecord:
    """Tracks the state of a single index consolidation operation."""

    index_name: str
    job_type: JobType = JobType.SHRINK
    state: JobState = JobState.PENDING
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    error: str = ""
    restore_index: str = ""
    shrunk_index: str = ""
    new_snapshot_name: str = ""
    new_frozen_index: str = ""
    original_snapshot_name: str = ""
    repository_name: str = ""
    expected_doc_count: int = 0
    # For merge jobs
    restore_indices: list[str] = field(default_factory=list)
    merge_task_id: str = ""
    source_frozen_indices: list[str] = field(default_factory=list)
    enriched_metadata: list[dict[str, Any]] = field(default_factory=list)
    history: list[dict[str, Any]] = field(default_factory=list)

    def transition(self, new_state: JobState, error: str = "") -> None:
        allowed = TRANSITIONS.get(self.state, [])
        if new_state not in allowed:
            raise ValueError(
                f"Invalid transition: {self.state.value} -> {new_state.value}"
            )
        self.history.append(
            {
                "from": self.state.value,
                "to": new_state.value,
                "timestamp": time.time(),
                "error": error,
            }
        )
        self.state = new_state
        self.updated_at = time.time()
        if error:
            self.error = error

    def to_dict(self) -> dict[str, Any]:
        return {
            "index_name": self.index_name,
            "job_type": self.job_type.value,
            "state": self.state.value,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "error": self.error,
            "restore_index": self.restore_index,
            "shrunk_index": self.shrunk_index,
            "new_snapshot_name": self.new_snapshot_name,
            "new_frozen_index": self.new_frozen_index,
            "original_snapshot_name": self.original_snapshot_name,
            "repository_name": self.repository_name,
            "expected_doc_count": self.expected_doc_count,
            "restore_indices": self.restore_indices,
            "merge_task_id": self.merge_task_id,
            "source_frozen_indices": self.source_frozen_indices,
            "enriched_metadata": self.enriched_metadata,
            "history": self.history,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> JobRecord:
        jt_raw = data.get("job_type", "SHRINK")
        try:
            jt = JobType(jt_raw)
        except ValueError:
            jt = JobType.SHRINK
        return cls(
            index_name=data["index_name"],
            job_type=jt,
            state=JobState(data["state"]),
            created_at=data.get("created_at", 0),
            updated_at=data.get("updated_at", 0),
            error=data.get("error", ""),
            restore_index=data.get("restore_index", ""),
            shrunk_index=data.get("shrunk_index", ""),
            new_snapshot_name=data.get("new_snapshot_name", ""),
            new_frozen_index=data.get("new_frozen_index", ""),
            original_snapshot_name=data.get("original_snapshot_name", ""),
            repository_name=data.get("repository_name", ""),
            expected_doc_count=data.get("expected_doc_count", 0),
            restore_indices=data.get("restore_indices", []),
            merge_task_id=data.get("merge_task_id", ""),
            source_frozen_indices=data.get("source_frozen_indices", []),
            enriched_metadata=data.get("enriched_metadata", []),
            history=data.get("history", []),
        )
