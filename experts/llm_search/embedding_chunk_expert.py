from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen


class EmbeddingChunkExpert:
    def run(self, payload: dict) -> dict:
        chunk_path = Path(payload["chunk_artifact_path"])
        output_dir = Path(payload.get("output_dir", "artifacts/embeddings"))
        model = payload.get("embedding_model", "nomic-embed-text")
        endpoint = payload.get("endpoint", "http://localhost:11434/api/embeddings")

        artifact = json.loads(chunk_path.read_text(encoding="utf-8"))
        output_dir.mkdir(parents=True, exist_ok=True)

        source = artifact.get("source", {})
        source_hash = source.get("source_hash") or artifact.get("document_hash")
        logical_path = source.get("logical_path") or artifact.get("logical_path")

        if "chunks" not in artifact:
            raise ValueError("Chunk collection artifact missing 'chunks' field.")

        chunks = artifact["chunks"]

        if not isinstance(chunks, list):
            raise ValueError("Chunk collection artifact has non-list 'chunks' field.")

        if len(chunks) == 0:
            raise ValueError("Chunk collection artifact has empty 'chunks' list.")

        written_paths: list[str] = []

        for chunk in chunks:
            text = chunk.get("content", {}).get("text", "").strip()
            if not text:
                continue

            vector = self._embed(endpoint, model, text)
            chunk_id = chunk["chunk_id"]
            safe_chunk_id = re.sub(r"[^A-Za-z0-9._-]+", "_", chunk_id)
            safe_model = re.sub(r"[^A-Za-z0-9._-]+", "_", model)

            embedding_artifact = {
                "artifact_type": "embedding_vector",
                "chunk_id": chunk_id,
                "logical_path": logical_path,
                "chunk_index": chunk.get("chunk_index"),
                "document_hash": source_hash,
                "embedding_model": model,
                "embedding_dimensions": len(vector),
                "text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
                "vector": vector,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }

            artifact_path = output_dir / f"{safe_chunk_id}.{safe_model}.embedding.json"
            artifact_path.write_text(
                json.dumps(embedding_artifact, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            written_paths.append(str(artifact_path))

        if not written_paths:
            raise ValueError("No chunk texts found to embed.")

        return {
            "artifact_type": "embedding_vector_batch",
            "source_artifact_path": str(chunk_path),
            "logical_path": logical_path,
            "embedding_model": model,
            "written_count": len(written_paths),
            "artifact_paths": written_paths,
        }

    def _embed(self, endpoint: str, model: str, text: str) -> list[float]:
        body = json.dumps({"model": model, "prompt": text}).encode("utf-8")
        req = Request(endpoint, data=body, headers={"Content-Type": "application/json"})
        with urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return data["embedding"]