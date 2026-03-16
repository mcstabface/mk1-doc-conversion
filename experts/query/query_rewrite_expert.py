from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Dict, List, Literal, TypedDict

from experts.query.query_rewrite_validator import validate_query_rewrite


RewriteStatus = Literal["OK", "FALLBACK_ORIGINAL", "REJECTED"]


class QueryRewriteOutput(TypedDict):
    artifact_type: str
    schema_version: str
    original_query: str
    rewritten_query: str
    rewrite_status: RewriteStatus
    used_model: str
    used_fallback: bool
    rejection_reasons: List[str]
    preserved_terms: List[str]
    added_terms: List[str]
    removed_terms: List[str]
    rewrite_rationale: str


class QueryRewriteExpert:
    """
    Bounded LLM-backed query rewrite expert.

    Responsibilities:
    - send one rewrite request to a small local LLM
    - require strict JSON output
    - validate/sanitize model output
    - hard-fallback to original query on any failure
    """

    def __init__(
        self,
        model: str = "qwen2.5:3b-instruct",
        endpoint: str = "http://localhost:11434/api/generate",
        timeout_seconds: int = 30,
    ) -> None:
        self.model = model
        self.endpoint = endpoint
        self.timeout_seconds = timeout_seconds

    def run(self, payload: Dict[str, Any]) -> QueryRewriteOutput:
        original_query = str(payload.get("query_text", "")).strip()
        if not original_query:
            raise ValueError("QueryRewriteExpert requires 'query_text'.")

        prompt = self._build_prompt(original_query)

        try:
            raw_text = self._call_ollama(prompt)
            parsed = self._extract_json_object(raw_text)
            validated = validate_query_rewrite(parsed, original_query)
            return validated
        except Exception as exc:
            return self._fallback_output(
                original_query=original_query,
                used_model=self.model,
                rejection_reasons=[f"expert_failure:{type(exc).__name__}:{exc}"],
                rationale="Rewrite failed; using original query.",
            )

    def _build_prompt(self, original_query: str) -> str:
        return f"""You are a bounded retrieval-query rewriter.

Task:
Rewrite the user query into a concise retrieval-oriented query for enterprise document search.

Rules:
- Return JSON only.
- Do not answer the question.
- Do not add facts, dates, numbers, or named entities not already present in the query.
- Keep the rewritten query under 160 characters.
- Preserve the core meaning.
- Prefer retrieval-friendly terms and compact wording.
- If no improvement is possible, return the original query.
- rewrite_rationale must be short and plain.

Required JSON schema:
{{
  "artifact_type": "query_rewrite",
  "schema_version": "query_rewrite_v1",
  "rewritten_query": "<string>",
  "rewrite_status": "OK",
  "used_model": "{self.model}",
  "rewrite_rationale": "<string>"
}}

User query:
{original_query}
"""

    def _call_ollama(self, prompt: str) -> str:
        body = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {
                "temperature": 0,
            },
        }

        req = urllib.request.Request(
            self.endpoint,
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"ollama_http_{exc.code}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError("ollama_unreachable") from exc

        response_text = payload.get("response", "")
        if not isinstance(response_text, str) or not response_text.strip():
            raise RuntimeError("empty_model_response")

        return response_text.strip()

    def _extract_json_object(self, raw_text: str) -> Dict[str, Any]:
        try:
            parsed = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            raise RuntimeError("invalid_json_from_model") from exc

        if not isinstance(parsed, dict):
            raise RuntimeError("model_output_not_object")

        return parsed

    def _fallback_output(
        self,
        original_query: str,
        used_model: str,
        rejection_reasons: List[str],
        rationale: str,
    ) -> QueryRewriteOutput:
        return {
            "artifact_type": "query_rewrite",
            "schema_version": "query_rewrite_v1",
            "original_query": original_query,
            "rewritten_query": original_query,
            "rewrite_status": "FALLBACK_ORIGINAL",
            "used_model": used_model,
            "used_fallback": True,
            "rejection_reasons": rejection_reasons,
            "preserved_terms": [],
            "added_terms": [],
            "removed_terms": [],
            "rewrite_rationale": rationale,
        }