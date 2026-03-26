from __future__ import annotations

import json
import sqlite3
import time
from typing import Dict, Any

from experts.base_expert import BaseExpert


class RedactionApprovalRecordExpert(BaseExpert):

    def __init__(self, db_path: str):
        self.db_path = db_path

    def run(self, payload: Dict[str, Any]) -> Dict[str, Any]:

        required_fields = [
            "plan_id",
            "approval_flags",
        ]

        missing = [
            f for f in required_fields
            if f not in payload
        ]

        if missing:
            raise ValueError(
                f"Missing required payload fields: {missing}"
            )

        plan_id = payload["plan_id"]
        approval_flags = payload["approval_flags"]
        approved_utc = int(time.time())

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        try:
            cursor = conn.cursor()
            conn.execute("BEGIN")

            plan_row = cursor.execute(
                """
                SELECT plan_id, status
                FROM redaction_plan_runs
                WHERE plan_id = ?
                """,
                (plan_id,),
            ).fetchone()

            if not plan_row:
                raise RuntimeError(
                    f"No redaction plan found for plan_id={plan_id}."
                )

            if plan_row["status"] != "PLANNED":
                raise RuntimeError(
                    f"Plan status must be PLANNED, got {plan_row['status']}."
                )

            existing = cursor.execute(
                """
                SELECT approval_id
                FROM redaction_approvals
                WHERE plan_id = ?
                """,
                (plan_id,),
            ).fetchone()

            if existing:
                raise RuntimeError(
                    f"Approval already exists for plan_id={plan_id}."
                )

            cursor.execute(
                """
                INSERT INTO redaction_approvals (
                    plan_id,
                    approved_utc,
                    approval_flags
                )
                VALUES (?, ?, ?)
                """,
                (
                    plan_id,
                    approved_utc,
                    json.dumps(approval_flags, sort_keys=True),
                ),
            )

            approval_id = cursor.lastrowid

            conn.commit()

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        return {
            "redaction_approval_record": {
                "status": "RECORDED",
                "plan_id": plan_id,
                "approval_id": approval_id,
                "approved_utc": approved_utc,
            }
        }