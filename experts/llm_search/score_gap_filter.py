from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass(frozen=True)
class ScoreGapFilterConfig:
    relative_score_floor: float = 0.35
    min_results: int = 3
    max_results: int | None = None


class ScoreGapFilter:
    """
    Deterministic pruning pass for ranked retrieval candidates.

    Expected input:
        ranked_candidates: list[dict]
            Each dict must contain at least:
            - "score": numeric relevance score

    Behavior:
        - Always preserves original order
        - Drops candidates whose score falls below top_score * relative_score_floor
        - Guarantees at least min_results if enough candidates exist
        - Optionally caps total retained results with max_results
    """

    def __init__(self, config: ScoreGapFilterConfig | None = None) -> None:
        self.config = config or ScoreGapFilterConfig()

    def filter_candidates(self, ranked_candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not ranked_candidates:
            return []

        top_score = self._safe_score(ranked_candidates[0])
        if top_score <= 0:
            retained = ranked_candidates[:]
        else:
            cutoff = top_score * self.config.relative_score_floor
            retained = [
                candidate
                for candidate in ranked_candidates
                if self._safe_score(candidate) >= cutoff
            ]

        if len(retained) < self.config.min_results:
            retained = ranked_candidates[: self.config.min_results]

        if self.config.max_results is not None:
            retained = retained[: self.config.max_results]

        return retained

    @staticmethod
    def _safe_score(candidate: Dict[str, Any]) -> float:
        score = candidate.get("score", 0.0)
        try:
            return float(score)
        except (TypeError, ValueError):
            return 0.0