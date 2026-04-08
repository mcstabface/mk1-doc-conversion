from __future__ import annotations

import hashlib
import sqlite3
import time
from pathlib import Path
from typing import Dict, Any

from experts.base_expert import BaseExpert
from mk1_io.artifact_writer import write_validated_artifact


class RedactionCommitExpert(BaseExpert):

    def __init__(self, db_path: str):
        self.db_path = db_path

    def run(self, payload: Dict[str, Any]) -> Dict[str, Any]:

        required_fields = [
            "source_artifact_id",
            "profile",
            "ruleset_version",
            "ruleset_hash",
            "plan_id",
            "approval_id",
            "artifact_output_path",
        ]

        missing = [
            f for f in required_fields
            if f not in payload
        ]

        if missing:
            raise ValueError(
                f"Missing required payload fields: {missing}"
            )

        source_artifact_id = payload["source_artifact_id"]
        profile = payload["profile"]
        ruleset_version = payload["ruleset_version"]
        ruleset_hash = payload["ruleset_hash"]
        plan_id = payload["plan_id"]
        approval_id = payload["approval_id"]

        artifact_output_path = Path(
            payload["artifact_output_path"]
        ).resolve()

        now_utc = int(time.time())

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        try:
            cursor = conn.cursor()

            conn.execute("BEGIN")

            approval_row = cursor.execute(
                """
                SELECT approval_id, plan_id
                FROM redaction_approvals
                WHERE approval_id = ?
                """,
                (approval_id,),
            ).fetchone()

            if not approval_row:
                raise RuntimeError(
                    f"No redaction approval found for approval_id={approval_id}."
                )

            if approval_row["plan_id"] != plan_id:
                raise RuntimeError(
                    f"Approval {approval_id} does not belong to plan_id={plan_id}."
                )

            approval_exists = cursor.execute(
                """
                SELECT plan_id
                FROM redaction_plan_runs
                WHERE plan_id = ?
                AND status = 'PLANNED'
                """,
                (plan_id,),
            ).fetchone()

            if not approval_exists:
                raise RuntimeError(
                    "Commit blocked: plan approval not verified."
                )

            planned_source = cursor.execute(
                """
                SELECT 1
                FROM redaction_plan_suggestions
                WHERE plan_id = ?
                  AND artifact_id = ?
                LIMIT 1
                """,
                (plan_id, source_artifact_id),
            ).fetchone()

            if not planned_source:
                raise RuntimeError(
                    f"Source artifact {source_artifact_id} is not part of plan_id={plan_id}."
                )

            redacted_document = self._build_redacted_document(
                conn,
                source_artifact_id=source_artifact_id,
                plan_id=plan_id,
                profile=profile,
                ruleset_version=ruleset_version,
                ruleset_hash=ruleset_hash,
                approval_id=approval_id,
                now_utc=now_utc,
            )

            write_validated_artifact(
                artifact_output_path,
                redacted_document,
            )

            artifact_bytes = artifact_output_path.read_bytes()

            artifact_hash = hashlib.sha256(
                artifact_bytes
            ).hexdigest()

            cursor.execute(
                """
                INSERT INTO redacted_artifacts (
                    source_artifact_id,
                    plan_id,
                    approval_id,
                    profile,
                    ruleset_version,
                    ruleset_hash,
                    artifact_path,
                    artifact_hash,
                    artifact_type,
                    created_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_artifact_id,
                    plan_id,
                    approval_id,
                    profile,
                    ruleset_version,
                    ruleset_hash,
                    str(artifact_output_path),
                    artifact_hash,
                    "search_context_document",
                    now_utc,
                ),
            )

            redacted_artifact_id = cursor.lastrowid

            existing = cursor.execute(
                """
                SELECT redacted_artifact_id
                FROM artifact_truth_overrides
                WHERE source_artifact_id = ?
                """,
                (source_artifact_id,),
            ).fetchone()

            if existing:
                raise RuntimeError(
                    f"Commit blocked: active redaction already exists for source_artifact_id={source_artifact_id}."
                )

            cursor.execute(
                """
                INSERT OR REPLACE INTO artifact_truth_overrides (
                    source_artifact_id,
                    active_artifact_type,
                    active_artifact_path,
                    active_artifact_hash,
                    redacted_artifact_id,
                    created_utc
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    source_artifact_id,
                    "search_context_document",
                    str(artifact_output_path),
                    artifact_hash,
                    redacted_artifact_id,
                    now_utc,
                ),
            )

            conn.commit()

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        return {
            "redaction_commit": {
                "status": "COMPLETE",
                "source_artifact_id": source_artifact_id,
                "redacted_artifact_id": redacted_artifact_id,
                "artifact_path": str(artifact_output_path),
                "artifact_hash": artifact_hash,
            }
        }

    def _build_redacted_document(
        self,
        conn,
        *,
        source_artifact_id: int,
        plan_id: int,
        profile: str,
        ruleset_version: str,
        ruleset_hash: str,
        approval_id: int,
        now_utc: int,
    ) -> dict:

        cursor = conn.cursor()

        source_row = cursor.execute(
            """
            SELECT
                s.physical_path,
                s.logical_path,
                s.sha256
            FROM source_artifacts s
            WHERE s.artifact_id = ?
            """,
            (source_artifact_id,),
        ).fetchone()

        if not source_row:
            raise RuntimeError(
                f"Source artifact not found: {source_artifact_id}"
            )

        override_row = cursor.execute(
            """
            SELECT active_artifact_path
            FROM artifact_truth_overrides
            WHERE source_artifact_id = ?
            """,
            (source_artifact_id,),
        ).fetchone()

        if override_row:
            artifact_path = override_row["active_artifact_path"]
        else:
            registry_row = cursor.execute(
                """
                SELECT artifact_path
                FROM search_context_registry
                WHERE
                    source_path = ?
                    AND source_hash = ?
                    AND artifact_type = 'search_context_document'
                """,
                (
                    source_row["physical_path"],
                    source_row["sha256"],
                ),
            ).fetchone()

            if not registry_row:
                registry_row = cursor.execute(
                    """
                    SELECT artifact_path
                    FROM search_context_registry
                    WHERE
                        source_path = ?
                        AND artifact_type = 'search_context_document'
                    """,
                    (
                        source_row["physical_path"],
                    ),
                ).fetchone()

            if not registry_row:
                raise RuntimeError(
                    "No active search_context_document artifact found."
                )

            artifact_path = registry_row["artifact_path"]

        import json
        from pathlib import Path

        path = Path(artifact_path).resolve()

        with open(path, "r", encoding="utf-8") as f:
            artifact = json.load(f)

        text = artifact.get("text_content")

        if not isinstance(text, str):
            raise RuntimeError(
                "Artifact missing valid text_content."
            )

        suggestions = cursor.execute(
            """
            SELECT
                original_text,
                replacement_text
            FROM redaction_plan_suggestions
            WHERE plan_id = ?
            ORDER BY suggestion_id ASC
            """,
            (plan_id,),
        ).fetchall()

        if not suggestions:
            raise RuntimeError(
                f"No redaction suggestions found for plan_id={plan_id}."
            )

        applied_count = 0

        for row in suggestions:
            original = row["original_text"]
            replacement = row["replacement_text"]

            if original in text:
                text = text.replace(original, replacement)
                applied_count += 1

        if applied_count == 0:
            raise RuntimeError(
                f"No redactions were applied for plan_id={plan_id}."
            )

        return {
            "artifact_type": "search_context_document",
            "text_content": text,
            "redaction": {
                "profile": profile,
                "plan_id": plan_id,
                "approval_id": approval_id,
                "ruleset_version": ruleset_version,
                "ruleset_hash": ruleset_hash,
                "applied_count": applied_count,
                "created_utc": now_utc,
            },
        }