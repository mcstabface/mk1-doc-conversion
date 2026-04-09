from __future__ import annotations

from typing import Any, Dict, List
from datetime import datetime, timezone
from experts.base_expert import BaseExpert

import hashlib


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

        metadata = doc.get("metadata", {})
        chunking_metadata = metadata.get("chunking", {})

        target_chars = int(
            chunking_metadata.get("target_chars", self.DEFAULT_TARGET_CHARS)
        )
        overlap_chars = int(
            chunking_metadata.get("overlap_chars", self.DEFAULT_OVERLAP_CHARS)
        )

        if target_chars <= 0:
            raise ValueError("target_chars must be > 0.")
        if overlap_chars < 0:
            raise ValueError("overlap_chars must be >= 0.")
        if overlap_chars >= target_chars:
            raise ValueError("overlap_chars must be smaller than target_chars.")

        normalized_chunks, chunk_source_mode = self._normalize_chunks(
            doc=doc,
            logical_path=logical_path,
            source_hash=source_hash,
            target_chars=target_chars,
            overlap_chars=overlap_chars,
        )

        now_utc = int(datetime.now(timezone.utc).timestamp())
        run_id = payload.get("search_context_document", {}).get("run_id")

        result_chunking = {
            "strategy": "document_chunks_to_search_context_chunks",
            "chunk_count": len(normalized_chunks),
            "target_chars": target_chars,
            "overlap_chars": overlap_chars,
            "chunk_source_mode": chunk_source_mode,
        }

        redaction_metadata = metadata.get("redaction")
        redaction_provenance = metadata.get("redaction_provenance")
        source_chunking = metadata.get("source_chunking")

        if redaction_metadata is not None:
            result_chunking["redaction_present"] = True
        if source_chunking is not None:
            result_chunking["source_chunking"] = source_chunking
        if chunking_metadata.get("status") is not None:
            result_chunking["source_chunking_status"] = chunking_metadata.get("status")
        if chunking_metadata.get("reason") is not None:
            result_chunking["source_chunking_reason"] = chunking_metadata.get("reason")

        result = {
            "search_context_chunks": {
                "artifact_type": "search_context_chunk_collection",
                "schema_version": "search_context_chunk_collection_v1",
                "created_utc": now_utc,
                "producer_expert": "SearchContextChunkExpert",
                "run_id": run_id,
                "status": "COMPLETE",
                "source": {
                    "source_path": source.get("source_path"),
                    "logical_path": logical_path,
                    "source_hash": source_hash,
                },
                "chunking": result_chunking,
                "chunks": normalized_chunks,
            }
        }

        if redaction_metadata is not None:
            result["search_context_chunks"]["redaction"] = redaction_metadata
        if redaction_provenance is not None:
            result["search_context_chunks"]["redaction_provenance"] = redaction_provenance

        return result

    def _normalize_chunks(
        self,
        doc: Dict[str, Any],
        logical_path: str,
        source_hash: str,
        target_chars: int,
        overlap_chars: int,
    ) -> tuple[List[Dict[str, Any]], str]:

        metadata = doc.get("metadata", {})
        chunking_metadata = metadata.get("chunking", {})
        input_chunks = doc.get("chunks", [])

        requires_rechunk = chunking_metadata.get("status") == "REQUIRES_RECHUNK"

        if requires_rechunk:
            normalized = self._chunk_from_text_content(
                doc=doc,
                logical_path=logical_path,
                source_hash=source_hash,
                target_chars=target_chars,
                overlap_chars=overlap_chars,
            )
            return normalized, "text_content_rechunk"

        if not isinstance(input_chunks, list):
            raise ValueError("search_context_document.chunks must be a list.")

        if not input_chunks:
            normalized = self._chunk_from_text_content(
                doc=doc,
                logical_path=logical_path,
                source_hash=source_hash,
                target_chars=target_chars,
                overlap_chars=overlap_chars,
            )
            return normalized, "text_content_fallback"

        normalized: List[Dict[str, Any]] = []
        chunk_index = 0

        for raw_chunk in input_chunks:
            content = raw_chunk.get("content", {})
            text = content.get("text")

            if text is None:
                text = raw_chunk.get("text", "")

            if not isinstance(text, str):
                raise ValueError("Chunk text must be a string.")

            text_len = len(text)

            start = 0
            while start < text_len:
                end = min(start + target_chars, text_len)
                chunk_text = text[start:end]

                text_hash = hashlib.sha256(chunk_text.encode("utf-8")).hexdigest()

                normalized.append(
                    {
                        "chunk_id": f"{logical_path}::{source_hash}::{chunk_index:04d}",
                        "chunk_index": chunk_index,
                        "logical_path": logical_path,
                        "source_path": doc.get("source", {}).get("source_path"),
                        "document_hash": source_hash,
                        "text": chunk_text,
                        "text_hash": text_hash,
                        "token_count": max(1, len(chunk_text) // 4),
                        "content": {
                            "text": chunk_text,
                            "char_count": len(chunk_text),
                            "token_estimate": max(1, len(chunk_text) // 4),
                        },
                        "position": {
                            "start_char": start,
                            "end_char": end,
                        },
                        "embedding_ready": True,
                    }
                )

                chunk_index += 1

                if end == text_len:
                    break

                start = end - overlap_chars

        chunk_count = len(normalized)

        for chunk in normalized:
            chunk["chunk_count"] = chunk_count

        return normalized, "document_chunks"

    def _chunk_from_text_content(
        self,
        *,
        doc: Dict[str, Any],
        logical_path: str,
        source_hash: str,
        target_chars: int,
        overlap_chars: int,
    ) -> List[Dict[str, Any]]:
        text = doc.get("text_content")
        if not isinstance(text, str) or not text.strip():
            raise ValueError(
                "search_context_document.text_content must be a non-empty string "
                "when rechunking from redacted truth."
            )

        normalized: List[Dict[str, Any]] = []
        chunk_index = 0
        text_len = len(text)
        start = 0

        while start < text_len:
            end = min(start + target_chars, text_len)
            chunk_text = text[start:end]
            text_hash = hashlib.sha256(chunk_text.encode("utf-8")).hexdigest()

            normalized.append(
                {
                    "chunk_id": f"{logical_path}::{source_hash}::{chunk_index:04d}",
                    "chunk_index": chunk_index,
                    "logical_path": logical_path,
                    "source_path": doc.get("source", {}).get("source_path"),
                    "document_hash": source_hash,
                    "text": chunk_text,
                    "text_hash": text_hash,
                    "token_count": max(1, len(chunk_text) // 4),
                    "content": {
                        "text": chunk_text,
                        "char_count": len(chunk_text),
                        "token_estimate": max(1, len(chunk_text) // 4),
                    },
                    "position": {
                        "start_char": start,
                        "end_char": end,
                    },
                    "embedding_ready": True,
                }
            )

            chunk_index += 1

            if end == text_len:
                break

            start = end - overlap_chars

        chunk_count = len(normalized)

        for chunk in normalized:
            chunk["chunk_count"] = chunk_count

        return normalized