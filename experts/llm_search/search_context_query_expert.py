from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List
import json


def _normalize_allowed_file_types(raw: Any) -> set[str]:
    if not raw:
        return set()

    normalized = set()
    for item in raw:
        value = str(item).strip().lower()
        if value.startswith("."):
            value = value[1:]
        if value:
            normalized.add(value)

    return normalized


def _derive_file_type(logical_path: str | None) -> str | None:
    if not logical_path:
        return None

    suffix = Path(logical_path).suffix.strip().lower()
    if suffix.startswith("."):
        suffix = suffix[1:]

    return suffix or None

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
        allowed_file_types = _normalize_allowed_file_types(
            payload.get("allowed_file_types")
        )

        if not query_text:
            raise ValueError("SearchContextQueryExpert requires 'query_text'.")
        if not chunk_artifact_root:
            raise ValueError("SearchContextQueryExpert requires 'chunk_artifact_root'.")

        root = Path(chunk_artifact_root)
        if not root.exists():
            raise ValueError(f"Chunk artifact root does not exist: {root}")

        chunk_files = sorted(root.glob("*.search_context_chunks.json"))
        total_chunks_seen = 0
        filtered_out_by_file_type = 0

        results: List[Dict[str, Any]] = []
        for chunk_file in chunk_files:
            with open(chunk_file, "r", encoding="utf-8") as f:
                artifact = json.load(f)

            source = artifact.get("source", {})
            chunks = artifact.get("chunks", [])

            logical_path = source.get("logical_path")
            source_file_type = _derive_file_type(logical_path)

            if allowed_file_types and source_file_type not in allowed_file_types:
                filtered_out_by_file_type += len(chunks)
                total_chunks_seen += len(chunks)
                continue

            for chunk in chunks:
                total_chunks_seen += 1
                results.append(
                    {
                        "source_hash": source.get("source_hash"),
                        "logical_path": logical_path,
                        "source_file_type": source_file_type,
                        "chunk_id": chunk.get("chunk_id"),
                        "chunk_index": chunk.get("chunk_index"),
                        "text": chunk.get("content", {}).get("text", ""),
                        "char_count": chunk.get("content", {}).get("char_count", 0),
                        "token_estimate": chunk.get("content", {}).get("token_estimate", 0),
                    }
                )

        return {
            "query_text": query_text,
            "allowed_file_types": sorted(allowed_file_types),
            "candidate_count": len(results),
            "total_chunks_seen": total_chunks_seen,
            "filtered_out_by_file_type": filtered_out_by_file_type,
            "results": results,
        }