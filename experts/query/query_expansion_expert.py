from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass(frozen=True)
class QueryExpansionConfig:
    synonym_map: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "statement of work": ["sow"],
            "sow": ["statement of work"],
            "deliverables": ["milestones", "outputs"],
            "risk": ["risk_management", "hedging"],
            "risks": ["risk_management", "hedging"],
            "issue": ["watchlist"],
            "issues": ["watchlist"],
            "procurement": ["acquisition", "requisition", "purchasing"],
            "simulation": ["model", "modeling"],
        }
    )
    max_expansions_per_term: int = 2


class QueryExpansionExpert:
    """
    Deterministic lexical query expansion.

    Expands only from a fixed synonym map.
    No LLM usage.
    """

    def __init__(self, config: QueryExpansionConfig | None = None) -> None:
        self.config = config or QueryExpansionConfig()

    def expand(self, query_text: str) -> Dict[str, object]:
        normalized_query = self._normalize(query_text)
        expansions: List[str] = [query_text]
        seen = {normalized_query}

        # token-level additive expansion
        tokens = normalized_query.split()
        additive_terms: List[str] = []
        for token in tokens:
            synonyms = self.config.synonym_map.get(token, [])
            additive_terms.extend(synonyms[: self.config.max_expansions_per_term])

        if additive_terms:
            additive_query = normalized_query + " " + " ".join(additive_terms)
            if additive_query not in seen:
                expansions.append(additive_query)
                seen.add(additive_query)

        return {
            "query_text": query_text,
            "expanded_queries": expansions,
            "expansion_count": len(expansions),
        }

    @staticmethod
    def _normalize(text: str) -> str:
        text = text.lower().strip()
        text = re.sub(r"\s+", " ", text)
        return text