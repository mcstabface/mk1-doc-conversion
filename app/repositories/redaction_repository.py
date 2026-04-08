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
                WHERE first_seen_run_id = ? OR last_seen_run_id = ?
                ORDER BY logical_path ASC
                """,
                (run_id, run_id),
            ).fetchall()
            return [dict(r) for r in rows]

    def list_redaction_candidate_runs(self, limit: int = 50) -> list[dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT DISTINCT
                    r.run_id,
                    r.source_root,
                    r.status,
                    r.started_utc,
                    r.finished_utc,
                    r.notes
                FROM runs r
                JOIN source_artifacts s
                    ON s.first_seen_run_id = r.run_id
                    OR s.last_seen_run_id = r.run_id
                LEFT JOIN artifact_truth_overrides o
                    ON o.source_artifact_id = s.artifact_id
                    AND o.active_artifact_type = 'search_context_document'
                LEFT JOIN search_context_registry scr
                    ON scr.source_path = s.physical_path
                    AND scr.source_hash = s.sha256
                    AND scr.artifact_type = 'search_context_document'
                WHERE
                    r.status IN ('SUCCESS', 'FAILED', 'CONVERSION_RUN_COMPLETE')
                    AND COALESCE(o.active_artifact_path, scr.artifact_path) IS NOT NULL
                ORDER BY r.run_id DESC
                LIMIT ?
                """,
                (limit,),
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

    def list_redaction_candidate_artifacts_for_run(self, run_id: int) -> list[dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT
                    candidates.artifact_id,
                    candidates.logical_path,
                    candidates.physical_path,
                    candidates.source_type,
                    candidates.sha256,
                    candidates.size_bytes,
                    candidates.active_truth_artifact_path
                FROM (
                    SELECT DISTINCT
                        s.artifact_id,
                        s.logical_path,
                        s.physical_path,
                        s.source_type,
                        s.sha256,
                        s.size_bytes,
                        COALESCE(
                            o.active_artifact_path,
                            scr_exact.artifact_path,
                            scr_path.artifact_path
                        ) AS active_truth_artifact_path
                    FROM source_artifacts s
                    LEFT JOIN artifact_truth_overrides o
                        ON o.source_artifact_id = s.artifact_id
                        AND o.active_artifact_type = 'search_context_document'
                    LEFT JOIN search_context_registry scr_exact
                        ON scr_exact.source_path = s.physical_path
                        AND scr_exact.source_hash = s.sha256
                        AND scr_exact.artifact_type = 'search_context_document'
                    LEFT JOIN search_context_registry scr_path
                        ON scr_path.source_path = s.physical_path
                        AND scr_path.artifact_type = 'search_context_document'
                    WHERE
                        (s.first_seen_run_id = ? OR s.last_seen_run_id = ?)
                ) candidates
                WHERE candidates.active_truth_artifact_path IS NOT NULL
                ORDER BY candidates.logical_path ASC
                """,
                (run_id, run_id),
            ).fetchall()
            return [dict(r) for r in rows]