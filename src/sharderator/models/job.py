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
    AWAITING_REINDEX = "AWAITING_REINDEX"
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
    JobState.SHRINKING: [JobState.AWAITING_SHRINK, JobState.AWAITING_REINDEX, JobState.FAILED],
    JobState.AWAITING_SHRINK: [JobState.FORCE_MERGING, JobState.FAILED],
    JobState.AWAITING_REINDEX: [JobState.FORCE_MERGING, JobState.FAILED],
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
        import dataclasses
        d = dataclasses.asdict(self)
        d["state"] = self.state.value
        d["job_type"] = self.job_type.value
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> JobRecord:
        import dataclasses
        data = dict(data)
        data["state"] = JobState(data.get("state", "PENDING"))
        raw_jt = data.get("job_type", "SHRINK")
        try:
            data["job_type"] = JobType(raw_jt)
        except ValueError:
            data["job_type"] = JobType.SHRINK
        # Drop unknown keys from older/newer tracker docs
        valid = {f.name for f in dataclasses.fields(cls)}
        data = {k: v for k, v in data.items() if k in valid}
        return cls(**data)
