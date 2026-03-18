from __future__ import annotations

from typing import Any, Dict, List
from experts.llm_search.tokenization import tokenize


class SearchContextRankExpert:
    """
    Deterministic V1 ranking over query results.

    Strategy:
    - canonical tokenization via shared tokenizer
    - score by count of overlapping unique terms
    - add phrase bonus if the full query appears in the chunk
    - drop zero-score results
    - return top_k only
    """

    def run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        query_text = payload.get("query_text", "").strip()
        results = payload.get("results", [])
        top_k = int(payload.get("top_k", 5))

        if not query_text:
            raise ValueError("SearchContextRankExpert requires 'query_text'.")
        if not isinstance(results, list):
            raise ValueError("SearchContextRankExpert requires 'results' as a list.")
        if top_k <= 0:
            raise ValueError("top_k must be > 0.")

        query_terms = tokenize(query_text)

        ranked: List[Dict[str, Any]] = []
        for result in results:
            text = result.get("text", "")
            text_terms = tokenize(text)

            overlap = sorted(query_terms & text_terms)
            score = len(overlap)

            text_lower = text.lower()
            query_lower = query_text.lower()

            phrase_bonus = 0
            if query_lower in text_lower:
                phrase_bonus = 3
                score += phrase_bonus

            ranked_result = dict(result)
            ranked_result["score"] = score
            ranked_result["matched_terms"] = overlap
            ranked_result["phrase_bonus"] = phrase_bonus
            ranked.append(ranked_result)

        filtered = [r for r in ranked if r["score"] > 0]

        filtered.sort(
            key=lambda r: (
                -r["score"],
                r.get("chunk_index", 0),
                r.get("logical_path", ""),
            )
        )

        top_results = filtered[:top_k]

        return {
            "query_text": query_text,
            "candidate_count": len(results),
            "ranked_count": len(filtered),
            "returned_count": len(top_results),
            "results": top_results,
        }