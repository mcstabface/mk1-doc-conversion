from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AuditRunSummary:
    run_id: int
    source_root: str
    status: str
    started_utc: int | None
    finished_utc: int | None


@dataclass(frozen=True)
class TruthOverrideSummary:
    source_artifact_id: int
    active_artifact_type: str
    active_artifact_path: str
    active_artifact_hash: str
    redacted_artifact_id: int
    created_utc: int