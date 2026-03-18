from __future__ import annotations

from pathlib import Path
import sqlite3
import json
import re

TOKEN_RE = re.compile(r"[a-zA-Z0-9_]+")
MIN_LEN = 3

CHUNK_ROOT = Path("artifacts/enron_full_v2/search_context_chunks")
DB_PATH = Path("artifacts/enron_full_v2/lexical_index.db")


def tokenize(text: str) -> set[str]:
    return {
        t for t in TOKEN_RE.findall(text.lower())
        if len(t) >= MIN_LEN
    }


def main() -> None:
    if not CHUNK_ROOT.exists():
        raise SystemExit(f"missing chunk root: {CHUNK_ROOT}")

    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=OFF;")
    cur.execute("PRAGMA temp_store=MEMORY;")

    cur.execute("""
        CREATE TABLE postings (
            term TEXT NOT NULL,
            source_hash TEXT NOT NULL,
            logical_path TEXT,
            chunk_index INTEGER NOT NULL,
            chunk_id TEXT NOT NULL,
            PRIMARY KEY (term, source_hash, chunk_index, chunk_id)
        )
    """)

    cur.execute("""
        CREATE TABLE meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    doc_count = 0
    chunk_count = 0
    batch: list[tuple[str, str, str, int, str]] = []

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
                batch.append((term, source_hash, logical_path, chunk_index, chunk_id))

            chunk_count += 1

            if len(batch) >= 50000:
                cur.executemany(
                    """
                    INSERT OR IGNORE INTO postings
                    (term, source_hash, logical_path, chunk_index, chunk_id)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    batch,
                )
                conn.commit()
                batch.clear()

        doc_count += 1

    if batch:
        cur.executemany(
            """
            INSERT OR IGNORE INTO postings
            (term, source_hash, logical_path, chunk_index, chunk_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            batch,
        )
        conn.commit()

    cur.execute("CREATE INDEX idx_postings_term ON postings(term)")
    cur.execute("CREATE INDEX idx_postings_source_hash ON postings(source_hash)")
    cur.execute("ANALYZE")

    cur.executemany(
        "INSERT INTO meta(key, value) VALUES (?, ?)",
        [
            ("doc_count", str(doc_count)),
            ("chunk_count", str(chunk_count)),
        ],
    )
    conn.commit()
    conn.close()

    print(f"wrote: {DB_PATH}")
    print(f"docs: {doc_count}")
    print(f"chunks: {chunk_count}")


if __name__ == "__main__":
    main()