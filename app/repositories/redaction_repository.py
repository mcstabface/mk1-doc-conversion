from __future__ import annotations

import sqlite3
from typing import Any


class RedactionRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def list_runs(self, limit: int = 50) -> list[dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT
                    run_id,
                    source_root,
                    status,
                    started_utc,
                    finished_utc
                FROM runs
                ORDER BY run_id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def list_source_artifacts_for_run(self, run_id: int) -> list[dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT
                    artifact_id,
                    logical_path,
                    physical_path,
                    source_type,
                    sha256,
                    size_bytes
                FROM source_artifacts
                WHERE run_id = ?
                ORDER BY logical_path ASC
                """,
                (run_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_truth_override_for_source(self, source_artifact_id: int) -> dict[str, Any] | None:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT
                    source_artifact_id,
                    active_artifact_type,
                    active_artifact_path,
                    active_artifact_hash,
                    redacted_artifact_id,
                    created_utc
                FROM artifact_truth_overrides
                WHERE source_artifact_id = ?
                """,
                (source_artifact_id,),
            ).fetchone()
            return dict(row) if row else None