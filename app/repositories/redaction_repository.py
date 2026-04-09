from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


class RedactionRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def _resolve_search_context_artifact_path_from_disk(
        self,
        *,
        physical_path: str,
        source_hash: str,
    ) -> str | None:
        search_context_dir = Path(self.db_path).resolve().parent.parent / "search_context"
        if not search_context_dir.exists():
            return None

        exact_match: str | None = None
        path_match: str | None = None

        for artifact_path in sorted(search_context_dir.glob("*.json")):
            try:
                with open(artifact_path, "r", encoding="utf-8") as f:
                    artifact = json.load(f)
            except Exception:
                continue

            if artifact.get("artifact_type") != "search_context_document":
                continue

            artifact_source_path = (
                artifact.get("source_path")
                or artifact.get("source", {}).get("source_path")
            )
            artifact_source_hash = (
                artifact.get("document_hash")
                or artifact.get("source_hash")
                or artifact.get("source", {}).get("source_hash")
            )

            if artifact_source_path == physical_path and artifact_source_hash == source_hash:
                exact_match = str(artifact_path)
                break

            if artifact_source_path == physical_path and path_match is None:
                path_match = str(artifact_path)

        return exact_match or path_match

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
                ORDER BY s.logical_path ASC
                """,
                (run_id, run_id),
            ).fetchall()

        candidates: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            if not item.get("active_truth_artifact_path"):
                item["active_truth_artifact_path"] = self._resolve_search_context_artifact_path_from_disk(
                    physical_path=item["physical_path"],
                    source_hash=item["sha256"],
                )

            if item.get("active_truth_artifact_path"):
                candidates.append(item)

        return candidates

    def list_plan_history_for_source_artifact(
        self,
        source_artifact_id: int,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT
                    p.plan_id,
                    p.run_id,
                    p.profile,
                    p.ruleset_version,
                    p.ruleset_hash,
                    p.status AS plan_status,
                    p.created_utc AS plan_created_utc,
                    COUNT(s.suggestion_id) AS suggestions_created,
                    MAX(a.approval_id) AS approval_id,
                    MAX(a.approved_utc) AS approved_utc,
                    MAX(r.redacted_artifact_id) AS redacted_artifact_id,
                    MAX(r.artifact_path) AS redacted_artifact_path,
                    MAX(r.created_utc) AS committed_utc
                FROM redaction_plan_runs p
                JOIN redaction_plan_suggestions s
                    ON s.plan_id = p.plan_id
                LEFT JOIN redaction_approvals a
                    ON a.plan_id = p.plan_id
                LEFT JOIN redacted_artifacts r
                    ON r.plan_id = p.plan_id
                    AND r.source_artifact_id = s.artifact_id
                WHERE s.artifact_id = ?
                GROUP BY
                    p.plan_id,
                    p.run_id,
                    p.profile,
                    p.ruleset_version,
                    p.ruleset_hash,
                    p.status,
                    p.created_utc
                ORDER BY p.plan_id DESC
                LIMIT ?
                """,
                (source_artifact_id, limit),
            ).fetchall()

            history: list[dict[str, Any]] = []
            for row in rows:
                row_dict = dict(row)

                category_rows = conn.execute(
                    """
                    SELECT
                        category,
                        COUNT(*) AS count
                    FROM redaction_plan_suggestions
                    WHERE plan_id = ?
                      AND artifact_id = ?
                    GROUP BY category
                    ORDER BY category
                    """,
                    (row_dict["plan_id"], source_artifact_id),
                ).fetchall()

                row_dict["category_counts"] = {
                    category_row["category"]: category_row["count"]
                    for category_row in category_rows
                }

                history.append(row_dict)

            return history

    def list_artifact_ids_with_suggestions_for_plan(self, plan_id: int) -> list[int]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT artifact_id
                FROM redaction_plan_suggestions
                WHERE plan_id = ?
                ORDER BY artifact_id ASC
                """,
                (plan_id,),
            ).fetchall()

            return [int(row[0]) for row in rows]