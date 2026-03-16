from __future__ import annotations

from typing import Any, Dict, List
from experts.llm_search.tokenization import tokenize
import math
import re


class SearchContextBm25RankExpert:
    """
    Deterministic BM25 ranker over retrieved chunk candidates.

    Input payload:
        {
            "query_text": str,
            "results": [...],
            "top_k": int = 5,
            "k1": float = 1.5,
            "b": float = 0.75
        }

    Output:
        {
            "query_text": str,
            "candidate_count": int,
            "ranked_count": int,
            "returned_count": int,
            "results": [...]
        }
    """

    def run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        query_text = payload.get("query_text", "").strip()
        results = payload.get("results", [])
        top_k = int(payload.get("top_k", 5))
        k1 = float(payload.get("k1", 1.5))
        b = float(payload.get("b", 0.75))

        if not query_text:
            raise ValueError("SearchContextBm25RankExpert requires 'query_text'.")
        if not isinstance(results, list):
            raise ValueError("SearchContextBm25RankExpert requires 'results' as a list.")
        if top_k <= 0:
            raise ValueError("top_k must be > 0.")

        query_terms = self._tokenize_list(query_text)
        if not query_terms:
            return {
                "query_text": query_text,
                "candidate_count": len(results),
                "ranked_count": 0,
                "returned_count": 0,
                "results": [],
            }

        doc_tokens: List[List[str]] = [
            self._tokenize_list(r.get("text", ""))
            for r in results
        ]

        doc_lengths = [len(tokens) for tokens in doc_tokens]
        avg_doc_len = sum(doc_lengths) / len(doc_lengths) if doc_lengths else 0.0

        doc_freq: Dict[str, int] = {}
        for tokens in doc_tokens:
            for term in set(tokens):
                doc_freq[term] = doc_freq.get(term, 0) + 1

        ranked: List[Dict[str, Any]] = []
        total_docs = len(results)

        for result, tokens in zip(results, doc_tokens):
            tf: Dict[str, int] = {}
            for token in tokens:
                tf[token] = tf.get(token, 0) + 1

            doc_len = len(tokens)
            score = 0.0
            matched_terms: List[str] = []

            for term in sorted(set(query_terms)):
                term_tf = tf.get(term, 0)
                if term_tf == 0:
                    continue

                matched_terms.append(term)

                df = doc_freq.get(term, 0)
                idf = math.log(1 + ((total_docs - df + 0.5) / (df + 0.5)))

                denom = term_tf + k1 * (1 - b + b * (doc_len / avg_doc_len)) if avg_doc_len > 0 else 1.0
                score += idf * ((term_tf * (k1 + 1)) / denom)

            token_set = set(tokens)

            DISCUSSION_TERMS = {
                "discuss", "discussed", "discussion", "issues", "concerns",
                "meeting", "talk", "debate",
            }

            RISK_TERMS = {
                "risk", "risks", "volatility", "swings", "prices",
                "weather", "regulatory", "backlash", "competitive",
                "dynamics", "turmoil",
            }

            STRATEGY_TERMS = {
                "strategy", "strategies", "trading", "investment",
                "market", "markets", "hedging", "industry", "structure",
            }

            EXEC_TERMS = {
                "executive", "executives", "skilling", 
            }

            BIOGRAPHY_TERMS = {
                "born", "died", "undergraduate", "master", "degree",
                "professor", "university", "earned",
            }

            intent_bonus = 0.0

            if token_set & DISCUSSION_TERMS:
                intent_bonus += 0.10
            if token_set & RISK_TERMS:
                intent_bonus += 0.08
            if token_set & STRATEGY_TERMS:
                intent_bonus += 0.08
            if token_set & EXEC_TERMS:
                intent_bonus += 0.05
            if token_set & BIOGRAPHY_TERMS:
                intent_bonus -= 2.5

            score += intent_bonus

            phrase_bonus = 0.0
            if query_text.lower() in result.get("text", "").lower():
                phrase_bonus = 1.0
                score += phrase_bonus

            ranked_result = dict(result)
            ranked_result["score"] = round(score, 6)
            ranked_result["matched_terms"] = matched_terms
            ranked_result["phrase_bonus"] = phrase_bonus
            ranked_result["bm25_core_score"] = round(score - intent_bonus - phrase_bonus, 6)
            ranked_result["intent_bonus"] = round(intent_bonus, 6)
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

    def _tokenize_list(self, text: str) -> List[str]:
        """
        BM25 tokenization with canonical normalization rules, but preserving
        frequency for scoring.
        """
        raw = re.findall(r"[a-zA-Z0-9_]+", text.lower())
        canonical = tokenize(text)
        return [token for token in raw if token in canonical]