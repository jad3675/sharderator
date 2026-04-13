"""Pipeline event interface — decoupled from Qt."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class PipelineEvents(Protocol):
    """Callback interface for pipeline progress reporting.

    Implementations: QtPipelineEvents (GUI), CliEvents (CLI).
    The engine modules call these methods; they never import PyQt6.
    """

    def on_progress(self, index_name: str, pct: float) -> None: ...
    def on_state_changed(self, index_name: str, state: str) -> None: ...
    def on_log(self, message: str) -> None: ...
    def on_completed(self, index_name: str) -> None: ...
    def on_failed(self, index_name: str, error: str) -> None: ...


class NullEvents:
    """No-op implementation for testing or silent operation."""

    def on_progress(self, index_name: str, pct: float) -> None:
        pass

    def on_state_changed(self, index_name: str, state: str) -> None:
        pass

    def on_log(self, message: str) -> None:
        pass

    def on_completed(self, index_name: str) -> None:
        pass

    def on_failed(self, index_name: str, error: str) -> None:
        pass
