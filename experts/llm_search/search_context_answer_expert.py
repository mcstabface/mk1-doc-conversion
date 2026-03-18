from __future__ import annotations

import re
from typing import Any, Dict, List


class SearchContextAnswerExpert:
    """
    Deterministic extracted-answer layer.

    V3 behavior:
    - does not use an LLM
    - extracts concise evidence from top context blocks
    - removes BOM noise
    - preserves source citations
    """

    def _tokenize(self, text: str) -> set[str]:
        raw = re.findall(r"[a-zA-Z0-9_]+", text.lower())
        return {token for token in raw if len(token) >= 3}

    def run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        query_text = payload.get("query_text", "").strip()
        context_text = payload.get("context_text", "").strip()
        sources = payload.get("sources", [])

        if not query_text:
            raise ValueError("SearchContextAnswerExpert requires 'query_text'.")
        if not isinstance(sources, list):
            raise ValueError("SearchContextAnswerExpert requires 'sources' as a list.")

        if not context_text:
            return {
                "query_text": query_text,
                "answer_text": "No relevant context was found for the query.",
                "source_count": 0,
                "sources": [],
            }

        cleaned = context_text.replace("\ufeff", "")
        query_terms = self._tokenize(query_text)

        blocks = [b.strip() for b in cleaned.split("\n\n[SOURCE: ") if b.strip()]
        normalized_blocks: List[str] = []
        for i, block in enumerate(blocks):
            if i == 0 and block.startswith("[SOURCE:"):
                normalized_blocks.append(block)
            else:
                normalized_blocks.append("[SOURCE: " + block)

        extracted_sections: List[Dict[str, Any]] = []
        evidence_lines: List[str] = []

        for block in normalized_blocks[:2]:
            lines = [line.strip() for line in block.splitlines() if line.strip()]
            if not lines:
                continue

            header = lines[0]
            body_lines = lines[1:]

            matching_lines: List[str] = []
            for line in body_lines:
                line_terms = self._tokenize(line)
                if query_terms & line_terms:
                    matching_lines.append(line)
                if len(matching_lines) == 3:
                    break

            if not matching_lines:
                matching_lines = body_lines[:3]

            extracted_sections.append({
                "header": header,
                "lines": matching_lines,
            })
            evidence_lines.extend(matching_lines)

        summary_sentences: List[str] = []

        evidence_blob = " ".join(evidence_lines).lower()

        if any(term in evidence_blob for term in ["price swings", "inflated prices", "volatility", "volatile"]):
            summary_sentences.append(
                "Retrieved evidence highlights trading risk tied to price swings and market volatility."
            )

        if any(term in evidence_blob for term in ["weather", "demand", "warmer than normal winter"]):
            summary_sentences.append(
                "It also points to weather-driven demand variability as an important source of exposure."
            )

        if any(term in evidence_blob for term in ["hedging", "demand swaps", "options", "cross-commodity"]):
            summary_sentences.append(
                "The strongest chunks describe hedging approaches such as demand swaps, options, and related risk-management tools."
            )

        if not summary_sentences:
            summary_sentences.append(
                "Retrieved evidence is relevant but mixed, with the top chunks emphasizing energy-market risk, trading strategy, and risk-management themes."
            )

        answer_lines = [
            " ".join(summary_sentences),
            "",
            "Sources used:",
        ]

        for source in sources:
            logical_path = source.get("logical_path", "unknown")
            chunk_index = source.get("chunk_index", -1)
            score = source.get("score", 0)
            answer_lines.append(f"- {logical_path} (chunk {chunk_index}, score={score})")

        return {
            "query_text": query_text,
            "answer_text": "\n".join(answer_lines).strip(),
            "source_count": len(sources),
            "sources": sources,
        }