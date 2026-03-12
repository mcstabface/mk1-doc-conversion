from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List
import json


class SearchContextQueryExpert:
    """
    Deterministic retrieval over persisted search_context_chunks artifacts.

    V1 scope:
    - load all chunk artifacts from disk
    - flatten chunks into a candidate set
    - no embeddings yet
    - no ranking yet beyond simple placeholder ordering

    Input payload:
        {
            "query_text": str,
            "chunk_artifact_root": str
        }

    Output:
        {
            "query_text": str,
            "candidate_count": int,
            "results": [...]
        }
    """

    def run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        query_text = payload.get("query_text", "").strip()
        chunk_artifact_root = payload.get("chunk_artifact_root")

        if not query_text:
            raise ValueError("SearchContextQueryExpert requires 'query_text'.")
        if not chunk_artifact_root:
            raise ValueError("SearchContextQueryExpert requires 'chunk_artifact_root'.")

        root = Path(chunk_artifact_root)
        if not root.exists():
            raise ValueError(f"Chunk artifact root does not exist: {root}")

        chunk_files = sorted(root.glob("*.search_context_chunks.json"))

        results: List[Dict[str, Any]] = []
        for chunk_file in chunk_files:
            with open(chunk_file, "r", encoding="utf-8") as f:
                artifact = json.load(f)

            source = artifact.get("source", {})
            chunks = artifact.get("chunks", [])

            for chunk in chunks:
                results.append(
                    {
                        "source_hash": source.get("source_hash"),
                        "logical_path": source.get("logical_path"),
                        "chunk_id": chunk.get("chunk_id"),
                        "chunk_index": chunk.get("chunk_index"),
                        "text": chunk.get("content", {}).get("text", ""),
                        "char_count": chunk.get("content", {}).get("char_count", 0),
                        "token_estimate": chunk.get("content", {}).get("token_estimate", 0),
                    }
                )

        return {
            "query_text": query_text,
            "candidate_count": len(results),
            "results": results,
        }