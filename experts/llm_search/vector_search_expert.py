from __future__ import annotations

import json
from pathlib import Path

import faiss
import numpy as np


class VectorSearchExpert:

    def run(self, payload: dict) -> dict:

        query_vector = payload["query_vector"]
        top_k = int(payload.get("top_k", 5))

        index_path = Path("artifacts/vector_index/vector_index.faiss")
        metadata_path = Path("artifacts/vector_index/vector_metadata.json")

        if not index_path.exists():
            raise ValueError(f"Vector index not found: {index_path}")

        if not metadata_path.exists():
            raise ValueError(f"Vector metadata not found: {metadata_path}")

        index = faiss.read_index(str(index_path))

        metadata_artifact = json.loads(metadata_path.read_text(encoding="utf-8"))
        entries = metadata_artifact.get("entries", [])

        if not isinstance(query_vector, list) or not query_vector:
            raise ValueError("VectorSearchExpert requires non-empty query_vector.")

        query = np.array([query_vector], dtype="float32")
        faiss.normalize_L2(query)

        scores, indices = index.search(query, top_k)

        results = []

        for score, idx in zip(scores[0], indices[0]):

            if idx < 0 or idx >= len(entries):
                continue

            meta = entries[idx]

            results.append({
                "logical_path": meta.get("logical_path"),
                "chunk_id": meta.get("chunk_id"),
                "score": float(score),
                "embedding_model": meta.get("embedding_model"),
                "artifact_path": meta.get("artifact_path"),
            })

        return {
            "artifact_type": "vector_search_result",
            "result_count": len(results),
            "results": results,
        }