from __future__ import annotations

import json
from pathlib import Path

import faiss
import numpy as np


EMBED_ROOT = Path("artifacts/test_source_mid/embeddings")
INDEX_DIR = Path("artifacts/vector_index")
INDEX_PATH = INDEX_DIR / "vector_index.faiss"
METADATA_PATH = INDEX_DIR / "vector_metadata.json"


def load_embeddings():
    vectors = []
    metadata = []

    for path in sorted(EMBED_ROOT.glob("*.embedding.json")):
        artifact = json.loads(path.read_text(encoding="utf-8"))

        vector = artifact.get("vector")
        if not isinstance(vector, list) or not vector:
            continue

        vectors.append(vector)
        metadata.append(
            {
                "chunk_id": artifact.get("chunk_id"),
                "logical_path": artifact.get("logical_path"),
                "document_hash": artifact.get("document_hash"),
                "embedding_model": artifact.get("embedding_model"),
                "embedding_dim": artifact.get("embedding_dim"),
                "artifact_path": str(path),
            }
        )

    return vectors, metadata


def main():
    vectors, metadata = load_embeddings()

    if not vectors:
        raise ValueError("No embedding artifacts found.")

    dim = len(vectors[0])

    for i, vec in enumerate(vectors):
        if len(vec) != dim:
            raise ValueError(
                f"Inconsistent embedding dimension at index {i}: expected {dim}, got {len(vec)}"
            )

    matrix = np.array(vectors, dtype="float32")
    faiss.normalize_L2(matrix)

    index = faiss.IndexFlatIP(dim)
    index.add(matrix)

    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    faiss.write_index(index, str(INDEX_PATH))

    from datetime import datetime, timezone

    now_utc = int(datetime.now(timezone.utc).timestamp())

    metadata_artifact = {
        "artifact_type": "vector_index_metadata",
        "schema_version": "vector_index_metadata_v1",
        "created_utc": now_utc,
        "producer_expert": "build_vector_index",
        "run_id": None,
        "status": "COMPLETE",
        "embedding_count": len(metadata),
        "embedding_dim": dim,
        "index_type": "faiss.IndexFlatIP",
        "distance_metric": "cosine_via_normalized_inner_product",
        "entries": metadata,
    }

    METADATA_PATH.write_text(
        json.dumps(metadata_artifact, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"INDEX WRITTEN: {INDEX_PATH}")
    print(f"METADATA WRITTEN: {METADATA_PATH}")
    print(f"EMBEDDINGS INDEXED: {len(metadata)}")
    print(f"DIMENSION: {dim}")


if __name__ == "__main__":
    main()