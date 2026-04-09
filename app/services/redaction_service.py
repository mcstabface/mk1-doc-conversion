from __future__ import annotations

import json
from pathlib import Path

from experts.redaction.redaction_plan_expert import RedactionPlanExpert
from experts.redaction.redaction_approval_record_expert import RedactionApprovalRecordExpert
from experts.redaction.redaction_preview_expert import RedactionPreviewExpert
from experts.redaction.redaction_commit_expert import RedactionCommitExpert
from experts.llm_search.search_context_chunk_expert import SearchContextChunkExpert
from experts.llm_search.embedding_chunk_expert import EmbeddingChunkExpert
from mk1_io.artifact_writer import write_validated_artifact

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
            artifacts_selected=result["artifacts_selected"],
            artifacts_with_suggestions=result["artifacts_with_suggestions"],
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

    def commit_batch(
        self,
        *,
        artifact_ids: list[int],
        profile: str,
        ruleset_version: str,
        ruleset_hash: str,
        plan_id: int,
        approval_id: int,
        output_root: Path,
    ) -> list[dict]:
        results: list[dict] = []

        for artifact_id in artifact_ids:
            try:
                output_path = (
                    output_root.resolve()
                    / f"artifact_{artifact_id}.plan_{plan_id}.{profile}.redacted.json"
                )

                commit = self.commit(
                    RedactionCommitRequest(
                        source_artifact_id=artifact_id,
                        profile=profile,
                        ruleset_version=ruleset_version,
                        ruleset_hash=ruleset_hash,
                        plan_id=plan_id,
                        approval_id=approval_id,
                        artifact_output_path=output_path,
                    )
                )

                results.append(
                    {
                        "source_artifact_id": artifact_id,
                        "status": commit.status,
                        "redacted_artifact_id": commit.redacted_artifact_id,
                        "artifact_path": commit.artifact_path,
                        "error": "",
                    }
                )
            except Exception as exc:
                results.append(
                    {
                        "source_artifact_id": artifact_id,
                        "status": "FAILED",
                        "redacted_artifact_id": "",
                        "artifact_path": "",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )

        return results

    def rechunk_search_context_artifact(
        self,
        *,
        artifact_path: Path,
        output_path: Path,
    ) -> dict:
        artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
        result = SearchContextChunkExpert().run(
            {"search_context_document": artifact}
        )["search_context_chunks"]

        output_path.parent.mkdir(parents=True, exist_ok=True)
        write_validated_artifact(output_path, result)

        return {
            "status": "COMPLETE",
            "source_artifact_path": str(artifact_path),
            "chunk_artifact_path": str(output_path),
            "chunk_count": len(result["chunks"]),
            "chunk_source_mode": result.get("chunking", {}).get("chunk_source_mode"),
            "redaction_present": bool(result.get("redaction")),
        }

    def generate_embeddings_for_chunk_artifact(
        self,
        *,
        chunk_artifact_path: Path,
        output_dir: Path,
        embedding_model: str = "nomic-embed-text",
        endpoint: str = "http://localhost:11434/api/embeddings",
        batch_size: int = 64,
    ) -> dict:
        output_dir.mkdir(parents=True, exist_ok=True)

        return EmbeddingChunkExpert().run(
            {
                "chunk_artifact_path": str(chunk_artifact_path),
                "output_dir": str(output_dir),
                "embedding_model": embedding_model,
                "endpoint": endpoint,
                "batch_size": batch_size,
            }
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

    def list_artifact_ids_with_suggestions_for_plan(self, plan_id: int) -> list[int]:
        return self.repo.list_artifact_ids_with_suggestions_for_plan(plan_id)

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