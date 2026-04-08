from __future__ import annotations

import json
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from experts.base_expert import BaseExpert


class RedactionPlanExpert(BaseExpert):
    """
    First-slice redaction planning expert.

    Scope:
        - business_sensitive profile only
        - creates redaction_plan_runs row
        - creates redaction_plan_suggestions rows
        - reads active truth artifact first
        - never writes derived artifacts
        - never activates truth overrides
    """

    PROFILE_BUSINESS_SENSITIVE = "business_sensitive"
    PLAN_STATUS = "PLANNED"

    # Deterministic replacement tokens
    REPLACEMENTS = {
        "CURRENCY_AMOUNT": "[CURRENCY_AMOUNT]",
        "BANK_ACCOUNT": "[BANK_ACCOUNT]",
        "ROUTING_NUMBER": "[ROUTING_NUMBER]",
        "CREDIT_CARD": "[CREDIT_CARD]",
        "IBAN": "[IBAN]",
        "SWIFT_BIC": "[SWIFT_BIC]",
    }

    # Deterministic rule order matters.
    # These are best-effort v1 patterns for the first slice.
    RULES: List[Tuple[str, str, re.Pattern[str]]] = [
        (
            "IBAN",
            "iban_v1",
            re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b"),
        ),
        (
            "SWIFT_BIC",
            "swift_bic_v3",
            re.compile(
                r"(?i)\b(?:swift|bic|swift/bic|bic/swift)\s*(?:code|no\.?|#)?\s*[:\-]?\s*([A-Z]{4}[A-Z]{2}[A-Z0-9]{2}(?:[A-Z0-9]{3})?)\b"
            ),
        ),
        (
            "BANK_ACCOUNT",
            "bank_account_v2",
            re.compile(
                r"(?i)\b(?:account|acct|a/c)\s*(?:number|no\.?|#)?\s*[:\-]?\s*\d{6,17}\b"
            ),
        ),
        (
            "ROUTING_NUMBER",
            "routing_number_v2",
            re.compile(
                r"(?i)\b(?:routing|aba)\s*(?:number|no\.?|#)?\s*[:\-]?\s*\d{9}\b"
            ),
        ),
        (
            "CREDIT_CARD",
            "credit_card_v2",
            re.compile(
                r"(?i)\b(?:card|credit card|cc)\s*(?:number|no\.?|#)?\s*[:\-]?\s*(?:\d[ -]?){13,19}\b"
            ),
        ),
        (
            "CURRENCY_AMOUNT",
            "currency_amount_v2",
            re.compile(
                r"""
                (?:
                    \$\s?(?:\d{1,3}(?:,\d{3})+|\d{3,})(?:\.\d{2})?
                    |
                    (?:USD|EUR|GBP)\s?(?:\d{1,3}(?:,\d{3})+|\d{3,})(?:\.\d{2})?
                    |
                    (?:\d{1,3}(?:,\d{3})+|\d{3,})(?:\.\d{2})?\s?(?:USD|EUR|GBP)
                )
                """,
                re.VERBOSE,
            ),
        ),
    ]

    def __init__(self, db_path: str):
        self.db_path = db_path

    def run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        required_fields = [
            "run_id",
            "profile",
            "ruleset_version",
            "ruleset_hash",
            "artifact_ids",
        ]

        missing = [f for f in required_fields if f not in payload]
        if missing:
            raise ValueError(f"Missing required payload fields: {missing}")

        run_id = payload["run_id"]
        profile = payload["profile"]
        ruleset_version = payload["ruleset_version"]
        ruleset_hash = payload["ruleset_hash"]
        artifact_ids = payload["artifact_ids"]

        if profile != self.PROFILE_BUSINESS_SENSITIVE:
            raise ValueError(
                f"Unsupported profile '{profile}'. "
                f"Only '{self.PROFILE_BUSINESS_SENSITIVE}' is supported in this slice."
            )

        if not isinstance(artifact_ids, list) or not artifact_ids:
            raise ValueError("artifact_ids must be a non-empty list.")

        if not ruleset_version or not isinstance(ruleset_version, str):
            raise ValueError("ruleset_version must be a non-empty string.")

        if not ruleset_hash or not isinstance(ruleset_hash, str):
            raise ValueError("ruleset_hash must be a non-empty string.")

        now_utc = int(time.time())

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        try:
            cursor = conn.cursor()
            conn.execute("BEGIN")

            run_exists = cursor.execute(
                """
                SELECT run_id
                FROM runs
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()

            if not run_exists:
                raise RuntimeError(f"Run does not exist: run_id={run_id}")

            cursor.execute(
                """
                INSERT INTO redaction_plan_runs (
                    run_id,
                    profile,
                    ruleset_version,
                    ruleset_hash,
                    status,
                    created_utc
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    profile,
                    ruleset_version,
                    ruleset_hash,
                    self.PLAN_STATUS,
                    now_utc,
                ),
            )

            plan_id = cursor.lastrowid
            if not plan_id:
                raise RuntimeError("Failed to create redaction plan.")

            suggestions_created = 0
            artifacts_planned = 0
            category_counts = {category: 0 for category, _, _ in self.RULES}

            for artifact_id in artifact_ids:
                source_row = cursor.execute(
                    """
                    SELECT
                        artifact_id,
                        physical_path,
                        logical_path,
                        sha256
                    FROM source_artifacts
                    WHERE artifact_id = ?
                    """,
                    (artifact_id,),
                ).fetchone()

                if not source_row:
                    raise RuntimeError(
                        f"Source artifact does not exist: artifact_id={artifact_id}"
                    )

                physical_path = source_row["physical_path"]
                logical_path = source_row["logical_path"]
                source_hash = source_row["sha256"]

                logical_path = source_row["logical_path"]
                source_hash = source_row["sha256"]

                artifact_path = self._resolve_active_truth_artifact_path(
                    conn=conn,
                    physical_path=physical_path,
                    logical_path=logical_path,
                    source_hash=source_hash,
                )

                if not artifact_path:
                    raise RuntimeError(
                        "No active search_context_document artifact found for "
                        f"artifact_id={artifact_id}, logical_path={logical_path}"
                    )

                text = self._load_text_from_artifact(artifact_path)
                matches = self._detect_business_sensitive(text)

                for category, rule_id, original_text, replacement_text in matches:
                    cursor.execute(
                        """
                        INSERT INTO redaction_plan_suggestions (
                            plan_id,
                            artifact_id,
                            category,
                            original_text,
                            replacement_text,
                            rule_id,
                            created_utc
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            plan_id,
                            artifact_id,
                            category,
                            original_text,
                            replacement_text,
                            rule_id,
                            now_utc,
                        ),
                    )
                    suggestions_created += 1
                    category_counts[category] += 1

                artifacts_planned += 1

            conn.commit()

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        return {
            "redaction_plan": {
                "status": self.PLAN_STATUS,
                "plan_id": plan_id,
                "run_id": run_id,
                "profile": profile,
                "ruleset_version": ruleset_version,
                "ruleset_hash": ruleset_hash,
                "artifacts_planned": artifacts_planned,
                "suggestions_created": suggestions_created,
                "category_counts": category_counts,
                "created_utc": now_utc,
            }
        }

    def _resolve_active_truth_artifact_path(
        self,
        conn: sqlite3.Connection,
        physical_path: str,
        logical_path: str,
        source_hash: str,
    ) -> Optional[str]:
        """
        Resolve the current truth artifact for this source.

        Order:
            1) active redaction override
            2) persisted search_context_document in registry
        """

        override_row = conn.execute(
            """
            SELECT
                o.active_artifact_path
            FROM artifact_truth_overrides o
            JOIN source_artifacts s
                ON s.artifact_id = o.source_artifact_id
            WHERE
                s.logical_path = ?
                AND s.sha256 = ?
                AND o.active_artifact_type = 'search_context_document'
            """,
            (logical_path, source_hash),
        ).fetchone()

        if override_row:
            return override_row["active_artifact_path"]

        registry_row = conn.execute(
            """
            SELECT artifact_path
            FROM search_context_registry
            WHERE
                source_path = ?
                AND source_hash = ?
                AND artifact_type = 'search_context_document'
            """,
            (physical_path, source_hash),
        ).fetchone()

        if registry_row:
            return registry_row["artifact_path"]

        registry_row = conn.execute(
            """
            SELECT artifact_path
            FROM search_context_registry
            WHERE
                source_path = ?
                AND source_hash = ?
                AND artifact_type = 'search_context_document'
            """,
            (physical_path, source_hash),
        ).fetchone()

        if registry_row:
            return registry_row["artifact_path"]

        registry_row = conn.execute(
            """
            SELECT artifact_path
            FROM search_context_registry
            WHERE
                source_path = ?
                AND artifact_type = 'search_context_document'
            """,
            (physical_path,),
        ).fetchone()

        if registry_row:
            return registry_row["artifact_path"]

        return None

    def _load_text_from_artifact(self, artifact_path: str) -> str:
        path = Path(artifact_path).resolve()

        if not path.exists():
            raise RuntimeError(f"Artifact path does not exist: {path}")

        with open(path, "r", encoding="utf-8") as f:
            artifact = json.load(f)
        
        artifact_type = artifact.get("artifact_type")
        if artifact_type != "search_context_document":
            raise RuntimeError(
                f"Expected artifact_type='search_context_document', got '{artifact_type}'"
            )

        text = artifact.get("text_content")
        if not isinstance(text, str):
            raise RuntimeError(
                f"Artifact missing valid text_content: {path}"
            )

        if not text.strip():
            raise RuntimeError(
                f"Artifact text_content is empty: {path}"
            )

        return text

    def _detect_business_sensitive(
        self,
        text: str,
    ) -> List[Tuple[str, str, str, str]]:
        """
        Returns deterministic, non-overlapping matches:

            [
                (category, rule_id, original_text, replacement_text),
                ...
            ]
        """

        raw_matches: List[Tuple[int, int, int, str, str, str, str]] = []
        # (start, end, rule_order, category, rule_id, original_text, replacement)

        for rule_order, (category, rule_id, pattern) in enumerate(self.RULES):
            for match in pattern.finditer(text):
                start, end = match.span()
                original_text = match.group(0)

                if not original_text:
                    continue

                raw_matches.append(
                    (
                        start,
                        end,
                        rule_order,
                        category,
                        rule_id,
                        original_text,
                        self.REPLACEMENTS[category],
                    )
                )

        raw_matches.sort(key=lambda item: (item[0], item[1], item[2]))

        accepted: List[Tuple[int, int, int, str, str, str, str]] = []
        last_end = -1
        seen_spans = set()

        for item in raw_matches:
            start, end, _, _, _, _, _ = item
            span_key = (start, end)

            if span_key in seen_spans:
                continue

            if start < last_end:
                continue

            seen_spans = set()

            seen_spans.add(span_key)
            accepted.append(item)
            last_end = end

        return [
            (category, rule_id, original_text, replacement_text)
            for _, _, _, category, rule_id, original_text, replacement_text in accepted
        ]