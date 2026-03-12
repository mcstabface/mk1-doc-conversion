from __future__ import annotations

from typing import Any, Dict, List


class SearchContextAssembleExpert:
    """
    Build an LLM-ready context block from ranked retrieval results.

    Input payload:
        {
            "query_text": str,
            "results": [...],
            "max_context_chars": int = 6000
        }

    Output:
        {
            "query_text": str,
            "result_count": int,
            "used_count": int,
            "context_text": str,
            "sources": [...]
        }
    """

    def run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        query_text = payload.get("query_text", "").strip()
        results = payload.get("results", [])
        max_context_chars = int(payload.get("max_context_chars", 6000))
        max_chunks_per_source = int(payload.get("max_chunks_per_source", 2))
        if max_chunks_per_source <= 0:
            raise ValueError("max_chunks_per_source must be > 0.")

        if not query_text:
            raise ValueError("SearchContextAssembleExpert requires 'query_text'.")
        if not isinstance(results, list):
            raise ValueError("SearchContextAssembleExpert requires 'results' as a list.")
        if max_context_chars <= 0:
            raise ValueError("max_context_chars must be > 0.")

        context_parts: List[str] = []
        sources: List[Dict[str, Any]] = []
        used_count = 0
        total_chars = 0
        source_counts: Dict[str, int] = {}

        for result in results:
            logical_path = result.get("logical_path", "unknown")
            current_count = source_counts.get(logical_path, 0)
            if current_count >= max_chunks_per_source:
                continue
            chunk_index = result.get("chunk_index", -1)
            text = result.get("text", "").strip()
            score = result.get("score", 0)

            if not text:
                continue

            block = (
                f"[SOURCE: {logical_path} | CHUNK: {chunk_index} | SCORE: {score}]\n"
                f"{text}\n"
            )

            projected = total_chars + len(block) + 2
            if projected > max_context_chars:
                break

            context_parts.append(block)
            sources.append(
                {
                    "logical_path": logical_path,
                    "chunk_index": chunk_index,
                    "score": score,
                    "chunk_id": result.get("chunk_id"),
                }
            )
            total_chars = projected
            used_count += 1
            source_counts[logical_path] = current_count + 1

        context_text = "\n".join(context_parts)

        return {
            "query_text": query_text,
            "expanded_queries": payload.get("expanded_queries", []),
            "result_count": len(results),
            "used_count": used_count,
            "context_text": context_text,
            "sources": sources,
        }