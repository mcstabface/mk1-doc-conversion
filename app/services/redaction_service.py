from __future__ import annotations

import hashlib
from pathlib import Path

from experts.redaction.redaction_plan_expert import RedactionPlanExpert
from experts.redaction.redaction_approval_record_expert import RedactionApprovalRecordExpert
from experts.redaction.redaction_preview_expert import RedactionPreviewExpert
from experts.redaction.redaction_commit_expert import RedactionCommitExpert

from app.contracts.redaction import (
    RedactionPlanRequest,
    RedactionPlanSummary,
    RedactionApprovalRequest,
    RedactionApprovalSummary,
    RedactionPreviewRequest,
    RedactionPreviewSummary,
    RedactionCommitRequest,
    RedactionCommitSummary,
)
from app.repositories.redaction_repository import RedactionRepository


class RedactionService:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.repo = RedactionRepository(str(db_path))
        self.plan_expert = RedactionPlanExpert(str(db_path))
        self.approval_expert = RedactionApprovalRecordExpert(str(db_path))
        self.preview_expert = RedactionPreviewExpert(str(db_path))
        self.commit_expert = RedactionCommitExpert(str(db_path))

    def list_source_artifacts_for_run(self, run_id: int) -> list[dict]:
        candidate_artifacts = self.repo.list_redaction_candidate_artifacts_for_run(run_id)
        if candidate_artifacts:
            return candidate_artifacts

        return self.repo.list_source_artifacts_for_run(run_id)

    def create_plan(self, request: RedactionPlanRequest) -> RedactionPlanSummary:
        result = self.plan_expert.run(
            {
                "run_id": request.run_id,
                "profile": request.profile,
                "ruleset_version": request.ruleset_version,
                "ruleset_hash": request.ruleset_hash,
                "artifact_ids": request.artifact_ids,
            }
        )["redaction_plan"]

        return RedactionPlanSummary(
            plan_id=result["plan_id"],
            run_id=result["run_id"],
            profile=result["profile"],
            ruleset_version=result["ruleset_version"],
            ruleset_hash=result["ruleset_hash"],
            suggestions_created=result["suggestions_created"],
            category_counts=result["category_counts"],
            status=result["status"],
        )

    def record_approval(self, request: RedactionApprovalRequest) -> RedactionApprovalSummary:
        result = self.approval_expert.run(
            {
                "plan_id": request.plan_id,
                "approval_flags": request.approval_flags,
            }
        )["redaction_approval_record"]

        return RedactionApprovalSummary(
            approval_id=result["approval_id"],
            plan_id=result["plan_id"],
            approved_utc=result["approved_utc"],
            status=result["status"],
        )

    def get_preview(self, request: RedactionPreviewRequest) -> RedactionPreviewSummary:
        result = self.preview_expert.run(
            {
                "source_artifact_id": request.source_artifact_id,
                "profile": request.profile,
                "ruleset_version": request.ruleset_version,
                "ruleset_hash": request.ruleset_hash,
                "plan_id": request.plan_id,
                "approval_id": request.approval_id,
            }
        )["redaction_preview"]

        return RedactionPreviewSummary(
            source_artifact_id=result["source_artifact_id"],
            plan_id=result["plan_id"],
            approval_id=result["approval_id"],
            status=result["status"],
            document=result["document"],
        )

    def commit(self, request: RedactionCommitRequest) -> RedactionCommitSummary:
        request.artifact_output_path.parent.mkdir(parents=True, exist_ok=True)

        result = self.commit_expert.run(
            {
                "source_artifact_id": request.source_artifact_id,
                "profile": request.profile,
                "ruleset_version": request.ruleset_version,
                "ruleset_hash": request.ruleset_hash,
                "plan_id": request.plan_id,
                "approval_id": request.approval_id,
                "artifact_output_path": str(request.artifact_output_path),
            }
        )["redaction_commit"]

        return RedactionCommitSummary(
            source_artifact_id=result["source_artifact_id"],
            redacted_artifact_id=result["redacted_artifact_id"],
            artifact_path=result["artifact_path"],
            artifact_hash=result["artifact_hash"],
            status=result["status"],
        )

    def list_runs(self, limit: int = 50) -> list[dict]:
        candidate_runs = self.repo.list_redaction_candidate_runs(limit=limit)
        if candidate_runs:
            return candidate_runs

        return self.repo.list_runs(limit=limit)

    def list_plan_history_for_source_artifact(
        self,
        source_artifact_id: int,
        limit: int = 20,
    ) -> list[dict]:
        return self.repo.list_plan_history_for_source_artifact(
            source_artifact_id=source_artifact_id,
            limit=limit,
        )

    def get_truth_override_state(self, source_artifact_id: int) -> dict | None:
        return self.repo.get_truth_override_for_source(source_artifact_id)

    def build_default_output_path(
        self,
        source_artifact_id: int,
        profile: str,
        plan_id: int,
    ) -> Path:
        filename = f"artifact_{source_artifact_id}.plan_{plan_id}.{profile}.redacted.json"
        return self.db_path.parent.parent / "redacted" / filename