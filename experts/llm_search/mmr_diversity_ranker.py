from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Set


_TOKEN_RE = re.compile(r"\b[a-z0-9]+\b")


@dataclass(frozen=True)
class MMRDiversityRankerConfig:
    lambda_weight: float = 0.50
    max_results: int = 5
    same_source_penalty: float = 0.20


class MMRDiversityRanker:
    """
    Deterministic Maximal Marginal Relevance reranker.

    Input:
        ranked_candidates: list[dict]
            Expected fields:
                - score: numeric relevance score
                - text: chunk text

    Output:
        A reordered subset of the input candidates that balances:
            - relevance to the original query/ranker score
            - diversity from already selected chunks

    Notes:
        - preserves original candidate payloads
        - deterministic
        - lexical similarity only (Jaccard over normalized token sets)
        - no embeddings required
    """

    def __init__(self, config: MMRDiversityRankerConfig | None = None) -> None:
        self.config = config or MMRDiversityRankerConfig()

    @staticmethod
    def _get_candidate_text(candidate: Dict[str, Any]) -> str:
        for field_name in ("text", "chunk_text", "content", "body_text"):
            value = candidate.get(field_name)
            if isinstance(value, str) and value.strip():
                return value
        return ""

    def rerank(self, ranked_candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not ranked_candidates:
            return []

        max_results = min(self.config.max_results, len(ranked_candidates))
        selected: List[Dict[str, Any]] = []
        remaining = ranked_candidates[:]

        max_score = max(self._safe_score(candidate) for candidate in ranked_candidates)
        if max_score <= 0:
            max_score = 1.0

        token_cache = {
            id(candidate): self._tokenize(self._get_candidate_text(candidate))
            for candidate in ranked_candidates
        }

        while remaining and len(selected) < max_results:
            if not selected:
                best = remaining.pop(0)
                selected.append(best)
                continue

            best_index = 0
            best_mmr_score = -math.inf

            for index, candidate in enumerate(remaining):
                relevance = self._safe_score(candidate) / max_score
                candidate_tokens = token_cache[id(candidate)]

                max_similarity = 0.0
                for prior in selected:
                    prior_tokens = token_cache[id(prior)]
                    similarity = self._jaccard_similarity(candidate_tokens, prior_tokens)
                    if similarity > max_similarity:
                        max_similarity = similarity

                same_source_penalty = 0.0
                candidate_source = (
                    candidate.get("source_name")
                    or candidate.get("logical_path")
                    or candidate.get("source_path")
                )

                for prior in selected:
                    prior_source = (
                        prior.get("source_name")
                        or prior.get("logical_path")
                        or prior.get("source_path")
                    )
                    if candidate_source and prior_source and candidate_source == prior_source:
                        same_source_penalty = self.config.same_source_penalty
                        break

                mmr_score = (
                    self.config.lambda_weight * relevance
                    - (1.0 - self.config.lambda_weight) * max_similarity
                    - same_source_penalty
                )

                if mmr_score > best_mmr_score:
                    best_mmr_score = mmr_score
                    best_index = index

            selected.append(remaining.pop(best_index))

        return selected

    @staticmethod
    def _safe_score(candidate: Dict[str, Any]) -> float:
        value = candidate.get("score", 0.0)
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _tokenize(text: str) -> Set[str]:
        return set(_TOKEN_RE.findall(text.lower()))

    @staticmethod
    def _jaccard_similarity(left: Set[str], right: Set[str]) -> float:
        if not left or not right:
            return 0.0

        union = left | right
        if not union:
            return 0.0

        intersection = left & right
        return len(intersection) / len(union)