from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

from mk1_io.artifact_writer import write_validated_artifact


class EmbeddingChunkExpert:
    def run(self, payload: dict) -> dict:
        chunk_path = Path(payload["chunk_artifact_path"])
        output_dir = Path(payload.get("output_dir", "artifacts/embeddings"))
        model = payload.get("embedding_model", "nomic-embed-text")
        endpoint = payload.get("endpoint", "http://localhost:11434/api/embeddings")
        batch_size = int(payload.get("batch_size", 64))

        if batch_size <= 0:
            raise ValueError("batch_size must be a positive integer.")

        artifact = json.loads(chunk_path.read_text(encoding="utf-8"))
        output_dir.mkdir(parents=True, exist_ok=True)

        source = artifact.get("source", {})
        source_hash = source.get("source_hash") or artifact.get("document_hash")
        logical_path = source.get("logical_path") or artifact.get("logical_path")
        run_id = artifact.get("run_id")

        if "chunks" not in artifact:
            raise ValueError("Chunk collection artifact missing 'chunks' field.")

        chunks = artifact["chunks"]

        if not chunks:
            return {
                "status": "SKIPPED",
                "reason": "empty_chunks",
                "source_artifact_path": str(chunk_path),
                "logical_path": logical_path,
                "embedding_model": model,
                "batch_size": batch_size,
                "written_count": 0,
                "skipped_valid_count": 0,
                "artifact_paths": [],
            }

        if not isinstance(chunks, list):
            raise ValueError("Chunk collection artifact has non-list 'chunks' field.")

        if len(chunks) == 0:
            raise ValueError("Chunk collection artifact has empty 'chunks' list.")

        written_paths: list[str] = []
        skipped_valid_count = 0
        embeddable_chunk_count = 0
        pending_items: list[dict] = []

        for chunk in chunks:
            text = (
                chunk.get("content", {}).get("text")
                or chunk.get("text")
                or ""
            ).strip()
            
            if not text:
                continue
            embeddable_chunk_count += 1

            chunk_id = chunk["chunk_id"]
            safe_chunk_id = re.sub(r"[^A-Za-z0-9._-]+", "_", chunk_id)
            safe_model = re.sub(r"[^A-Za-z0-9._-]+", "_", model)

            artifact_path = output_dir / f"{safe_chunk_id}.{safe_model}.embedding.json"

            text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()

            # ---- incremental check ----

            if artifact_path.exists():
                try:
                    existing = json.loads(artifact_path.read_text(encoding="utf-8"))

                    if (
                        existing.get("text_hash") == text_hash
                        and existing.get("embedding_model") == model
                    ):
                        skipped_valid_count += 1
                        continue

                except Exception:
                    pass

            pending_items.append(
                {
                    "chunk_id": chunk_id,
                    "text": text,
                    "text_hash": text_hash,
                    "artifact_path": artifact_path,
                }
            )

        for start in range(0, len(pending_items), batch_size):
            batch = pending_items[start:start + batch_size]
            batch_texts = [item["text"] for item in batch]
            vectors = self._embed_batch(endpoint, model, batch_texts)

            if len(vectors) != len(batch):
                raise ValueError(
                    f"Embedding endpoint returned {len(vectors)} vectors "
                    f"for {len(batch)} inputs."
                )

            now_utc = int(datetime.now(timezone.utc).timestamp())

            for item, vector in zip(batch, vectors):
                embedding_artifact = {
                    "artifact_type": "embedding_artifact",
                    "schema_version": "embedding_artifact_v1",
                    "created_utc": now_utc,
                    "producer_expert": "EmbeddingChunkExpert",
                    "run_id": run_id,
                    "status": "COMPLETE",
                    "chunk_id": item["chunk_id"],
                    "logical_path": logical_path,
                    "document_hash": source_hash,
                    "text_hash": item["text_hash"],
                    "embedding_model": model,
                    "batch_size": batch_size,
                    "embedding_dim": len(vector),
                    "vector": vector,
                    "source_path": source.get("source_path"),
                }

                write_validated_artifact(item["artifact_path"], embedding_artifact)
                written_paths.append(str(item["artifact_path"]))

        if embeddable_chunk_count == 0:
            raise ValueError("No chunk texts found to embed.")

        return {
            "artifact_type": "embedding_vector_batch",
            "source_artifact_path": str(chunk_path),
            "logical_path": logical_path,
            "embedding_model": model,
            "batch_size": batch_size,
            "written_count": len(written_paths),
            "skipped_valid_count": skipped_valid_count,
            "artifact_paths": written_paths,
        }

    def _embed_batch(self, endpoint: str, model: str, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        body = json.dumps({"model": model, "input": texts}).encode("utf-8")
        req = Request(endpoint, data=body, headers={"Content-Type": "application/json"})
        with urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        if "embeddings" in data:
            embeddings = data["embeddings"]
        elif "embedding" in data:
            # Defensive fallback for single-item responses.
            embeddings = [data["embedding"]]
        else:
            raise ValueError("Embedding response missing 'embeddings'/'embedding' field.")

        if not isinstance(embeddings, list):
            raise ValueError("Embedding response field is not a list.")

        return embeddings