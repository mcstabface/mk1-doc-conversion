# experts/query/query_rewrite_validator.py

from __future__ import annotations

import re
from typing import Any, Dict, List

_ALLOWED_STATUS = {"OK", "FALLBACK_ORIGINAL", "REJECTED"}

_TOKEN_RE = re.compile(r"[a-zA-Z0-9_]+")
_NUMBER_RE = re.compile(r"\b\d+\b")
_CAP_WORD_RE = re.compile(r"\b[A-Z][a-zA-Z0-9_-]+\b")


def _tokens(text: str) -> set[str]:
    return {t.lower() for t in _TOKEN_RE.findall(text)}


def _numbers(text: str) -> set[str]:
    return set(_NUMBER_RE.findall(text))


def _capitalized_words(text: str) -> set[str]:
    return set(_CAP_WORD_RE.findall(text))


def validate_query_rewrite(raw: Dict[str, Any], original_query: str) -> Dict[str, Any]:
    """
    Validate and sanitize LLM rewrite output.

    On any validation failure, fall back to the original query.
    """
    rejection_reasons: List[str] = []

    artifact_type = str(raw.get("artifact_type", "query_rewrite")).strip()
    schema_version = str(raw.get("schema_version", "query_rewrite_v1")).strip()
    rewritten_query = str(raw.get("rewritten_query", "")).strip()
    rewrite_status = str(raw.get("rewrite_status", "")).strip()
    used_model = str(raw.get("used_model", "unknown")).strip()
    rewrite_rationale = str(raw.get("rewrite_rationale", "")).strip()

    if not original_query.strip():
        raise ValueError("original_query must be non-empty")

    if artifact_type != "query_rewrite":
        rejection_reasons.append("invalid_artifact_type")

    if schema_version != "query_rewrite_v1":
        rejection_reasons.append("invalid_schema_version")

    if rewrite_status not in _ALLOWED_STATUS:
        rejection_reasons.append("invalid_rewrite_status")

    if not rewritten_query:
        rejection_reasons.append("empty_rewritten_query")

    if len(rewritten_query) > 160:
        rejection_reasons.append("rewritten_query_too_long")

    if len(rewrite_rationale) > 160:
        rejection_reasons.append("rewrite_rationale_too_long")

    original_tokens = _tokens(original_query)
    rewritten_tokens = _tokens(rewritten_query)

    original_numbers = _numbers(original_query)
    rewritten_numbers = _numbers(rewritten_query)

    original_caps = _capitalized_words(original_query)
    rewritten_caps = _capitalized_words(rewritten_query)

    added_terms = sorted(rewritten_tokens - original_tokens)
    removed_terms = sorted(original_tokens - rewritten_tokens)

    added_numbers = rewritten_numbers - original_numbers
    if added_numbers:
        rejection_reasons.append("added_numbers_not_in_original")

    added_caps = rewritten_caps - original_caps
    if added_caps:
        rejection_reasons.append("added_capitalized_terms_not_in_original")

    # crude answer-like guardrail
    lowered = rewritten_query.lower()
    answer_markers = (
        "the answer is",
        "enron executives discussed",
        "they discussed",
        "this means",
        "in summary",
    )
    if any(marker in lowered for marker in answer_markers):
        rejection_reasons.append("answer_like_output")

    # if anything failed, force fallback
    if rejection_reasons:
        return {
            "artifact_type": "query_rewrite",
            "schema_version": "query_rewrite_v1",
            "original_query": original_query,
            "rewritten_query": original_query,
            "rewrite_status": "FALLBACK_ORIGINAL",
            "used_model": used_model or "unknown",
            "used_fallback": True,
            "rejection_reasons": rejection_reasons,
            "preserved_terms": sorted(original_tokens),
            "added_terms": [],
            "removed_terms": [],
            "rewrite_rationale": "Validation failed; using original query.",
        }

    preserved_terms = sorted(original_tokens & rewritten_tokens)

    return {
        "artifact_type": "query_rewrite",
        "schema_version": "query_rewrite_v1",
        "original_query": original_query,
        "rewritten_query": rewritten_query,
        "rewrite_status": rewrite_status,
        "used_model": used_model or "unknown",
        "used_fallback": False,
        "rejection_reasons": [],
        "preserved_terms": preserved_terms,
        "added_terms": added_terms,
        "removed_terms": removed_terms,
        "rewrite_rationale": rewrite_rationale or "Rewrite accepted.",
    }