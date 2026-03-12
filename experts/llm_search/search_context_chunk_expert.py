from __future__ import annotations

from typing import Any, Dict, List

from experts.base_expert import BaseExpert


class SearchContextChunkExpert(BaseExpert):
    """
    Deterministic V3 retrieval-layer expert.

    Input:
        payload["search_context_document"]

    Output:
        {
            "search_context_chunks": {
                "artifact_type": "search_context_chunk_collection",
                "schema_version": "v1",
                "source": {...},
                "chunking": {...},
                "chunks": [...]
            }
        }

    Responsibility:
    - Normalize chunk boundaries from a search_context_document
    - Preserve deterministic chunk identity
    - Prepare embedding-ready segments
    """

    DEFAULT_TARGET_CHARS = 2000
    DEFAULT_OVERLAP_CHARS = 200

    def run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        doc = payload.get("search_context_document")
        if not doc:
            raise ValueError("SearchContextChunkExpert requires 'search_context_document' in payload.")

        source = doc.get("source", {})
        source_path = source.get("source_path")
        logical_path = source.get("logical_path")
        source_hash = source.get("source_hash")

        if not source_path or not logical_path or not source_hash:
            raise ValueError(
                "search_context_document.source must include "
                "'source_path', 'logical_path', and 'source_hash'."
            )

        chunking = doc.get("chunking", {})
        target_chars = int(chunking.get("target_chars", self.DEFAULT_TARGET_CHARS))
        overlap_chars = int(chunking.get("overlap_chars", self.DEFAULT_OVERLAP_CHARS))

        if target_chars <= 0:
            raise ValueError("target_chars must be > 0.")
        if overlap_chars < 0:
            raise ValueError("overlap_chars must be >= 0.")
        if overlap_chars >= target_chars:
            raise ValueError("overlap_chars must be smaller than target_chars.")

        normalized_chunks = self._normalize_chunks(
            doc=doc,
            logical_path=logical_path,
            source_hash=source_hash,
            target_chars=target_chars,
            overlap_chars=overlap_chars,
        )

        return {
            "search_context_chunks": {
                "artifact_type": "search_context_chunk_collection",
                "schema_version": "v1",
                "source": source,
                "chunking": {
                    "strategy": "fixed_chars_with_overlap",
                    "target_chars": target_chars,
                    "overlap_chars": overlap_chars,
                },
                "chunks": normalized_chunks,
            }
        }

    def _normalize_chunks(
        self,
        doc: Dict[str, Any],
        logical_path: str,
        source_hash: str,
        target_chars: int,
        overlap_chars: int,
    ) -> List[Dict[str, Any]]:
        """
        For the first V3 implementation, reuse existing chunk text if present and
        re-emit it under a stricter retrieval-layer schema.

        This is intentionally boring and deterministic.
        """
        input_chunks = doc.get("chunks", [])
        if not isinstance(input_chunks, list):
            raise ValueError("search_context_document.chunks must be a list.")

        normalized: List[Dict[str, Any]] = []

        for idx, raw_chunk in enumerate(input_chunks):
            content = raw_chunk.get("content", {})
            text = content.get("text")

            # Backward-compat fallback for the simpler bootstrap artifact shape
            if text is None:
                text = raw_chunk.get("text", "")

            if not isinstance(text, str):
                raise ValueError(f"Chunk {idx} text must be a string.")

            position = raw_chunk.get("position", {})
            start_char = position.get("start_char")
            end_char = position.get("end_char")

            if start_char is None or end_char is None:
                # Deterministic fallback if legacy chunk lacks explicit offsets
                start_char = 0 if idx == 0 else normalized[-1]["position"]["end_char"] - overlap_chars
                end_char = start_char + len(text)

            normalized.append(
                {
                    "chunk_id": f"{logical_path}::{source_hash}::{idx:04d}",
                    "chunk_index": idx,
                    "content": {
                        "text": text,
                        "char_count": len(text),
                        "token_estimate": max(1, len(text) // 4),
                    },
                    "position": {
                        "start_char": int(start_char),
                        "end_char": int(end_char),
                    },
                    "embedding_ready": True,
                }
            )

        chunk_count = len(normalized)
        for chunk in normalized:
            chunk["chunk_count"] = chunk_count

        return normalized