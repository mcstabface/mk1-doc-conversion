from __future__ import annotations

import json
from pathlib import Path

import argparse


CHUNK_ROOT = Path("artifacts/search_context_chunks")
EMBED_ROOT = Path("artifacts/embeddings")


def load_active_chunk_ids(source_contains: str | None = None) -> set[str]:
    chunk_ids = set()
    total_chunks = 0

    for path in sorted(CHUNK_ROOT.glob("*.search_context_chunks.json")):
        artifact = json.loads(path.read_text(encoding="utf-8"))

        source_path = artifact.get("source", {}).get("source_path", "")
        if source_contains and source_contains not in source_path:
            continue

        for chunk in artifact.get("chunks", []):
            chunk_ids.add(chunk["chunk_id"])
            total_chunks += 1

    return chunk_ids, total_chunks


def load_embedding_chunk_ids():
    chunk_ids = set()

    for path in sorted(EMBED_ROOT.glob("*.embedding.json")):
        artifact = json.loads(path.read_text(encoding="utf-8"))
        cid = artifact.get("chunk_id")
        if cid:
            chunk_ids.add(cid)

    return chunk_ids


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-contains",
        default=None,
        help="Optional substring filter for source_path, e.g. test_source_mid",
    )
    args = parser.parse_args()

    active_chunk_ids, _ = load_active_chunk_ids(args.source_contains)
    embedded_chunk_ids = load_embedding_chunk_ids()

    matched = active_chunk_ids & embedded_chunk_ids
    missing = active_chunk_ids - embedded_chunk_ids
    orphaned = embedded_chunk_ids - active_chunk_ids

    coverage = 0.0
    if active_chunk_ids:
        coverage_pct = len(matched) / len(active_chunk_ids)
    else:
        coverage_pct = 1.0

    print("\nEMBEDDING COVERAGE REPORT\n")

    print(f"Active chunks: {len(active_chunk_ids)}")
    print(f"Embeddings present: {len(embedded_chunk_ids)}")
    print(f"Matched embeddings: {len(matched)}")
    print(f"Missing embeddings: {len(missing)}")
    print(f"Orphan embeddings: {len(orphaned)}")
    print(f"Coverage: {coverage_pct:.2%}")

    if missing:
        print("\nMissing chunk_ids (first 10):")
        for cid in sorted(list(missing))[:10]:
            print(f"  {cid}")

    if orphaned:
        print("\nOrphan embedding chunk_ids (first 10):")
        for cid in sorted(list(orphaned))[:10]:
            print(f"  {cid}")


if __name__ == "__main__":
    main()