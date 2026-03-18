from __future__ import annotations

import json
from urllib.request import Request, urlopen


class QueryEmbeddingExpert:
    def run(self, payload: dict) -> dict:
        query_text = payload["query_text"].strip()
        model = payload.get("embedding_model", "nomic-embed-text")
        endpoint = payload.get("endpoint", "http://localhost:11434/api/embeddings")

        if not query_text:
            raise ValueError("QueryEmbeddingExpert requires non-empty query_text.")

        body = json.dumps({"model": model, "input": query_text}).encode("utf-8")
        req = Request(endpoint, data=body, headers={"Content-Type": "application/json"})

        with urlopen(req, timeout=120) as resp:
            raw = resp.read().decode("utf-8")

        data = json.loads(raw)

        if "embedding" in data:
            vector = data["embedding"]
        elif "embeddings" in data and data["embeddings"]:
            vector = data["embeddings"][0]
        else:
            raise ValueError(
                f"Embedding response missing 'embedding'/'embeddings' field. "
                f"Top-level keys: {list(data.keys())}. Response preview: {raw[:1000]}"
            )

        return {
            "artifact_type": "query_embedding",
            "query_text": query_text,
            "embedding_model": model,
            "embedding_dimensions": len(vector),
            "vector": vector,
        }