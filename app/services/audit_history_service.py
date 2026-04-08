from __future__ import annotations

from pathlib import Path

from app.repositories.run_repository import RunRepository
from app.repositories.redaction_repository import RedactionRepository


class AuditHistoryService:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.run_repo = RunRepository(str(db_path))
        self.redaction_repo = RedactionRepository(str(db_path))

    def list_recent_runs(self, limit: int = 25) -> list[dict]:
        return self.run_repo.list_recent_runs(limit=limit)

    def list_runs_for_redaction(self, limit: int = 50) -> list[dict]:
        return self.redaction_repo.list_runs(limit=limit)

    def list_source_artifacts_for_run(self, run_id: int) -> list[dict]:
        return self.redaction_repo.list_source_artifacts_for_run(run_id)

    def get_truth_override_for_source(self, source_artifact_id: int) -> dict | None:
        return self.redaction_repo.get_truth_override_for_source(source_artifact_id)