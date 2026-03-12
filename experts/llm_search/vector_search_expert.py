from __future__ import annotations

import json
import math
from pathlib import Path


class VectorSearchExpert:
    def run(self, payload: dict) -> dict:
        query_vector = payload["query_vector"]
        embedding_root = Path(payload.get("embedding_root", "artifacts/embeddings"))
        top_k = int(payload.get("top_k", 5))

        if not isinstance(query_vector, list) or not query_vector:
            raise ValueError("VectorSearchExpert requires non-empty query_vector.")

        results = []

        for path in sorted(embedding_root.glob("*.embedding.json")):
            data = json.loads(path.read_text(encoding="utf-8"))
            vector = data.get("vector", [])

            if len(vector) != len(query_vector):
                continue

            score = self._cosine_similarity(query_vector, vector)

            results.append({
                "logical_path": data.get("logical_path"),
                "chunk_index": data.get("chunk_index"),
                "chunk_id": data.get("chunk_id"),
                "score": score,
                "embedding_model": data.get("embedding_model"),
                "artifact_path": str(path),
            })

        results.sort(key=lambda x: x["score"], reverse=True)

        return {
            "artifact_type": "vector_search_result",
            "result_count": len(results[:top_k]),
            "results": results[:top_k],
        }

    def _cosine_similarity(self, a: list[float], b: list[float]) -> float:
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(y * y for y in b))

        if norm_a == 0.0 or norm_b == 0.0:
            return 0.0

        return dot / (norm_a * norm_b)