from __future__ import annotations

import json
import time
from pathlib import Path

from mk1_io.artifact_writer import write_validated_artifact


CHUNK_ROOT = Path("artifacts/search_context_chunks")
EMBED_ROOT = Path("artifacts/embeddings")
DOC_ROOT = Path("artifacts/search_context")
OUTPUT = Path("artifacts/corpus_stats/corpus_stats.json")


def load_chunk_ids():
    chunk_ids = set()
    doc_ids = set()

    for path in CHUNK_ROOT.glob("*.search_context_chunks.json"):
        artifact = json.loads(path.read_text())

        doc_hash = (
            artifact.get("document_hash")
            or artifact.get("source", {}).get("source_hash")
            or artifact.get("source", {}).get("document_hash")
        )
        if doc_hash:
            doc_ids.add(doc_hash)

        for chunk in artifact.get("chunks", []):
            chunk_ids.add(chunk["chunk_id"])

    return doc_ids, chunk_ids


def load_embedding_ids():
    ids = set()

    for path in EMBED_ROOT.glob("*.embedding.json"):
        artifact = json.loads(path.read_text())
        cid = artifact.get("chunk_id")

        if cid:
            ids.add(cid)

    return ids


def main():

    docs, chunks = load_chunk_ids()
    embeddings = load_embedding_ids()

    matched = chunks & embeddings
    missing = chunks - embeddings
    orphan = embeddings - chunks

    coverage = 0.0
    if chunks:
        coverage = len(matched) / len(chunks)

    artifact = {
        "artifact_type": "corpus_stats",
        "schema_version": "corpus_stats_v1",
        "created_utc": int(time.time()),
        "producer_expert": "report_corpus_stats",
        "run_id": None,
        "status": "COMPLETE",
        "document_count": len(docs),
        "chunk_count": len(chunks),
        "embedding_count": len(embeddings),
        "matched_embedding_count": len(matched),
        "missing_embedding_count": len(missing),
        "orphan_embedding_count": len(orphan),
        "embedding_coverage_pct": coverage,
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    write_validated_artifact(OUTPUT, artifact)

    print("CORPUS STATS WRITTEN:", OUTPUT)


if __name__ == "__main__":
    main()