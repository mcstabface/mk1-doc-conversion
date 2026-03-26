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

        source_artifact_id = payload["source_artifact_id"]

        redacted_document = payload["redacted_document"]

        profile = payload["profile"]

        ruleset_version = payload["ruleset_version"]

        ruleset_hash = payload["ruleset_hash"]

        plan_id = payload["plan_id"]

        approval_id = payload["approval_id"]

        required_fields = [
            "source_artifact_id",
            "redacted_document",
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
                    "Active redaction already exists for this source."
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