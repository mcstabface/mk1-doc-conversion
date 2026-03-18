from __future__ import annotations

from pathlib import Path
from collections import defaultdict
import hashlib
import json
import re
import sqlite3

TOKEN_RE = re.compile(r"[a-zA-Z0-9_]+")
MIN_LEN = 3
SHARD_COUNT = 64

CHUNK_ROOT = Path("artifacts/enron_full_v2/search_context_chunks")
INDEX_ROOT = Path("artifacts/enron_full_v2/lexical_index_sharded")


def tokenize(text: str) -> set[str]:
    return {
        t for t in TOKEN_RE.findall(text.lower())
        if len(t) >= MIN_LEN
    }


def shard_for_term(term: str) -> int:
    digest = hashlib.sha256(term.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % SHARD_COUNT


def open_shard(shard_id: int) -> sqlite3.Connection:
    path = INDEX_ROOT / f"shard_{shard_id:02d}.db"
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=OFF;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS postings (
            term TEXT NOT NULL,
            source_hash TEXT NOT NULL,
            logical_path TEXT,
            chunk_index INTEGER NOT NULL,
            chunk_id TEXT NOT NULL,
            PRIMARY KEY (term, source_hash, chunk_index, chunk_id)
        )
    """)
    return conn


def main() -> None:
    if not CHUNK_ROOT.exists():
        raise SystemExit(f"missing chunk root: {CHUNK_ROOT}")

    if INDEX_ROOT.exists():
        for old in INDEX_ROOT.glob("*.db"):
            old.unlink()
        manifest = INDEX_ROOT / "manifest.json"
        if manifest.exists():
            manifest.unlink()
    else:
        INDEX_ROOT.mkdir(parents=True, exist_ok=True)

    conns = [open_shard(i) for i in range(SHARD_COUNT)]
    curs = [c.cursor() for c in conns]
    batches: list[list[tuple[str, str, str, int, str]]] = [[] for _ in range(SHARD_COUNT)]

    doc_count = 0
    chunk_count = 0

    for chunk_file in sorted(CHUNK_ROOT.glob("*.search_context_chunks.json")):
        artifact = json.loads(chunk_file.read_text(encoding="utf-8"))
        source = artifact.get("source", {})
        source_hash = source.get("source_hash", "")
        logical_path = source.get("logical_path", "")

        for chunk in artifact.get("chunks", []):
            text = (
                chunk.get("content", {}).get("text")
                or chunk.get("text")
                or ""
            )
            terms = tokenize(text)
            chunk_index = int(chunk.get("chunk_index"))
            chunk_id = chunk.get("chunk_id")

            for term in terms:
                shard_id = shard_for_term(term)
                batches[shard_id].append(
                    (term, source_hash, logical_path, chunk_index, chunk_id)
                )

            chunk_count += 1

        doc_count += 1

        if doc_count % 2000 == 0:
            for shard_id in range(SHARD_COUNT):
                batch = batches[shard_id]
                if not batch:
                    continue
                curs[shard_id].executemany(
                    """
                    INSERT OR IGNORE INTO postings
                    (term, source_hash, logical_path, chunk_index, chunk_id)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    batch,
                )
                conns[shard_id].commit()
                batch.clear()
            print(f"processed_docs={doc_count} processed_chunks={chunk_count}")

    for shard_id in range(SHARD_COUNT):
        batch = batches[shard_id]
        if batch:
            curs[shard_id].executemany(
                """
                INSERT OR IGNORE INTO postings
                (term, source_hash, logical_path, chunk_index, chunk_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                batch,
            )
            conns[shard_id].commit()
            batch.clear()

    for shard_id in range(SHARD_COUNT):
        curs[shard_id].execute("CREATE INDEX IF NOT EXISTS idx_postings_term ON postings(term)")
        curs[shard_id].execute("CREATE INDEX IF NOT EXISTS idx_postings_source_hash ON postings(source_hash)")
        curs[shard_id].execute("ANALYZE")
        conns[shard_id].commit()
        conns[shard_id].close()

    manifest = {
        "artifact_type": "lexical_index_sharded",
        "schema_version": "lexical_index_sharded_v1",
        "shard_count": SHARD_COUNT,
        "doc_count": doc_count,
        "chunk_count": chunk_count,
        "shard_pattern": "shard_{id:02d}.db",
        "hash": "sha256(term) % shard_count",
    }
    (INDEX_ROOT / "manifest.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    print(f"wrote: {INDEX_ROOT}")
    print(f"docs: {doc_count}")
    print(f"chunks: {chunk_count}")
    print(f"shards: {SHARD_COUNT}")


if __name__ == "__main__":
    main()