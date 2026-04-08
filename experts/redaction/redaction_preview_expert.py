from __future__ import annotations

import sqlite3
import time
from typing import Dict, Any

from experts.base_expert import BaseExpert
from experts.redaction.redaction_commit_expert import RedactionCommitExpert


class RedactionPreviewExpert(BaseExpert):

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

        now_utc = int(time.time())

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        try:

            cursor = conn.cursor()

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
                    "Preview blocked: plan approval not verified."
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

            commit_expert = RedactionCommitExpert(self.db_path)

            redacted_document = commit_expert._build_redacted_document(
                conn,
                source_artifact_id=source_artifact_id,
                plan_id=plan_id,
                profile=profile,
                ruleset_version=ruleset_version,
                ruleset_hash=ruleset_hash,
                approval_id=approval_id,
                now_utc=now_utc,
            )

        finally:
            conn.close()

        return {
            "redaction_preview": {
                "status": "READY",
                "source_artifact_id": source_artifact_id,
                "plan_id": plan_id,
                "approval_id": approval_id,
                "document": redacted_document,
            }
        }