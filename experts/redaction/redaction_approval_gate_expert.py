from __future__ import annotations

import sqlite3
from typing import Dict, Any

from experts.base_expert import BaseExpert


class RedactionApprovalGateExpert(BaseExpert):

    def __init__(self, db_path: str):
        self.db_path = db_path

    def run(self, payload: Dict[str, Any]) -> Dict[str, Any]:

        required_fields = [
            "plan_id",
            "profile",
            "ruleset_version",
            "ruleset_hash",
            "yes_commit",
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
        profile = payload["profile"]
        ruleset_version = payload["ruleset_version"]
        ruleset_hash = payload["ruleset_hash"]
        yes_commit = payload["yes_commit"]

        if yes_commit is not True:
            raise RuntimeError(
                "Commit requires explicit yes_commit=True."
            )

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        try:
            plan_row = conn.execute(
                """
                SELECT
                    plan_id,
                    run_id,
                    profile,
                    ruleset_version,
                    ruleset_hash,
                    status,
                    created_utc
                FROM redaction_plan_runs
                WHERE plan_id = ?
                """,
                (plan_id,),
            ).fetchone()

            if not plan_row:
                raise RuntimeError(
                    f"No redaction plan found for plan_id={plan_id}."
                )

            if plan_row["profile"] != profile:
                raise RuntimeError(
                    "Plan profile does not match requested profile."
                )

            if plan_row["ruleset_version"] != ruleset_version:
                raise RuntimeError(
                    "Plan ruleset_version does not match requested ruleset_version."
                )

            if plan_row["ruleset_hash"] != ruleset_hash:
                raise RuntimeError(
                    "Plan ruleset_hash does not match requested ruleset_hash."
                )

            if plan_row["status"] != "PLANNED":
                raise RuntimeError(
                    f"Plan status must be PLANNED, got {plan_row['status']}."
                )

            newer_plan = conn.execute(
                """
                SELECT plan_id
                FROM redaction_plan_runs
                WHERE
                    run_id = ?
                    AND profile = ?
                    AND created_utc > ?
                LIMIT 1
                """,
                (
                    plan_row["run_id"],
                    profile,
                    plan_row["created_utc"],
                ),
            ).fetchone()

            if newer_plan:
                raise RuntimeError(
                    "Plan is superseded by a newer plan for the same run/profile."
                )

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        return {
            "redaction_approval_gate": {
                "status": "APPROVED",
                "plan_id": plan_id,
                "profile": profile,
                "ruleset_version": ruleset_version,
                "ruleset_hash": ruleset_hash,
                "source_run_id": plan_row["run_id"],
            }
        }