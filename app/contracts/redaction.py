from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RedactionPlanRequest:
    run_id: int
    profile: str
    ruleset_version: str
    ruleset_hash: str
    artifact_ids: list[int]


@dataclass(frozen=True)
class RedactionPlanSummary:
    plan_id: int
    run_id: int
    profile: str
    ruleset_version: str
    ruleset_hash: str
    artifacts_selected: int
    artifacts_with_suggestions: int
    suggestions_created: int
    category_counts: dict
    status: str


@dataclass(frozen=True)
class RedactionApprovalRequest:
    plan_id: int
    approval_flags: dict


@dataclass(frozen=True)
class RedactionApprovalSummary:
    approval_id: int
    plan_id: int
    approved_utc: int
    status: str


@dataclass(frozen=True)
class RedactionPreviewRequest:
    source_artifact_id: int
    profile: str
    ruleset_version: str
    ruleset_hash: str
    plan_id: int
    approval_id: int


@dataclass(frozen=True)
class RedactionPreviewSummary:
    source_artifact_id: int
    plan_id: int
    approval_id: int
    status: str
    document: dict


@dataclass(frozen=True)
class RedactionCommitRequest:
    source_artifact_id: int
    profile: str
    ruleset_version: str
    ruleset_hash: str
    plan_id: int
    approval_id: int
    artifact_output_path: Path


@dataclass(frozen=True)
class RedactionCommitSummary:
    source_artifact_id: int
    redacted_artifact_id: int
    artifact_path: str
    artifact_hash: str
    status: str