from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List
from functools import lru_cache
from collections import defaultdict
from experts.llm_search.tokenization import tokenize
import json
import re
import hashlib
import sqlite3


_TOKEN_RE = re.compile(r"[a-zA-Z0-9_]+")

_STOPWORDS = {
    "the", "and", "for", "are", "with", "from", "that", "this",
    "what", "when", "where", "which", "who", "whom", "whose", "why", "how",
    "did", "does", "do", "is", "was", "were", "be", "been", "being",
    "regarding", "discuss", "discussed", "about",
}

def _normalize_allowed_file_types(raw: Any) -> set[str]:
    if not raw:
        return set()

    normalized = set()
    for item in raw:
        value = str(item).strip().lower()
        if value.startswith("."):
            value = value[1:]
        if value:
            normalized.add(value)

    return normalized


def _derive_file_type(logical_path: str | None) -> str | None:
    if not logical_path:
        return None

    suffix = Path(logical_path).suffix.strip().lower()
    if suffix.startswith("."):
        suffix = suffix[1:]

    return suffix or None

_SHARD_COUNT = 64


def _shard_for_term(term: str) -> int:
    digest = hashlib.sha256(term.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % _SHARD_COUNT


@lru_cache(maxsize=_SHARD_COUNT)
def _open_shard_db(root_str: str, shard_id: int) -> sqlite3.Connection:
    shard_root = Path(root_str).parent / "lexical_index_sharded"
    db_path = shard_root / f"shard_{shard_id:02d}.db"

    if not db_path.exists():
        raise ValueError(f"Lexical shard not found: {db_path}")

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    return conn


def _load_lexical_index(root: Path, terms: set[str]) -> dict[str, list[list]]:
    postings_by_term: dict[str, list[list]] = {}
    shard_terms: dict[int, list[str]] = defaultdict(list)

    for term in terms:
        shard_terms[_shard_for_term(term)].append(term)

    for shard_id, term_list in shard_terms.items():
        conn = _open_shard_db(str(root), shard_id)
        placeholders = ",".join("?" for _ in term_list)

        rows = conn.execute(
            f"""
            SELECT term, logical_path, chunk_index, chunk_id
            FROM postings
            WHERE term IN ({placeholders})
            """,
            term_list,
        ).fetchall()

        for term, logical_path, chunk_index, chunk_id in rows:
            postings_by_term.setdefault(term, []).append(
                [logical_path, int(chunk_index), chunk_id]
            )

    return postings_by_term

class SearchContextQueryExpert:
    """
    Deterministic retrieval over persisted search_context_chunks artifacts.

    V1 scope:
    - load all chunk artifacts from disk
    - flatten chunks into a candidate set
    - no embeddings yet
    - no ranking yet beyond simple placeholder ordering

    Input payload:
        {
            "query_text": str,
            "chunk_artifact_root": str
        }

    Output:
        {
            "query_text": str,
            "candidate_count": int,
            "results": [...]
        }
    """

    def run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        query_text = payload.get("query_text", "").strip()
        chunk_artifact_root = payload.get("chunk_artifact_root")
        allowed_file_types = _normalize_allowed_file_types(
            payload.get("allowed_file_types")
        )

        if not query_text:
            raise ValueError("SearchContextQueryExpert requires 'query_text'.")
        if not chunk_artifact_root:
            raise ValueError("SearchContextQueryExpert requires 'chunk_artifact_root'.")

        root = Path(chunk_artifact_root)
        if not root.exists():
            raise ValueError(f"Chunk artifact root does not exist: {root}")

        query_terms = self._tokenize(query_text)

        lexical_index = _load_lexical_index(root, query_terms)

        term_rows = []
        for term in query_terms:
            postings = lexical_index.get(term, [])
            if postings:
                term_rows.append((term, postings, len(postings)))

        # Use the most selective terms for candidate generation, but prefer a
        # stronger intersection before falling back. This reduces broad lexical
        # leakage while preserving deterministic behavior.
        term_rows.sort(key=lambda x: x[2])

        refs_by_source_hash: dict[str, list[tuple[str, int, str]]] = defaultdict(list)

        candidate_ref_set: set[tuple[str, int, str]] = set()

        def _intersect_postings(postings_list: list[list[list]]) -> set[tuple[str, int, str]]:
            if not postings_list:
                return set()

            smallest = postings_list[0]
            other_sets = [
                {
                    (ref[0], int(ref[1]), ref[2])
                    for ref in postings
                }
                for postings in postings_list[1:]
            ]

            out: set[tuple[str, int, str]] = set()
            for ref in smallest:
                ref_tuple = (ref[0], int(ref[1]), ref[2])
                if all(ref_tuple in s for s in other_sets):
                    out.add(ref_tuple)
            return out

        if term_rows:
            posting_groups = [postings for _, postings, _ in term_rows]

            # Prefer 3-way intersection, then 2-way, then rarest-term fallback.
            for width in (3, 2, 1):
                selected_postings = posting_groups[:width]
                if not selected_postings:
                    continue

                if width == 1:
                    candidate_ref_set = {
                        (ref[0], int(ref[1]), ref[2])
                        for ref in selected_postings[0]
                    }
                else:
                    candidate_ref_set = _intersect_postings(selected_postings)

                if candidate_ref_set:
                    break

            for logical_path, chunk_index, chunk_id in candidate_ref_set:
                try:
                    _, source_hash, _ = chunk_id.rsplit("::", 2)
                except ValueError:
                    continue

                refs_by_source_hash[source_hash].append(
                    (logical_path, int(chunk_index), chunk_id)
                )

        total_chunks_seen = sum(len(v) for v in refs_by_source_hash.values())
        filtered_out_by_file_type = 0
        filtered_out_by_query = 0

        results: List[Dict[str, Any]] = []

        for source_hash, ref_rows in refs_by_source_hash.items():
            chunk_file = root / f"{source_hash}.search_context_chunks.json"
            if not chunk_file.exists():
                continue

            with open(chunk_file, "r", encoding="utf-8") as f:
                artifact = json.load(f)

            source = artifact.get("source", {})
            chunks = artifact.get("chunks", [])
            logical_path = source.get("logical_path")
            source_file_type = _derive_file_type(logical_path)

            if allowed_file_types and source_file_type not in allowed_file_types:
                filtered_out_by_file_type += len(ref_rows)
                continue

            chunk_lookup = {
                int(c.get("chunk_index")): c
                for c in chunks
                if c.get("chunk_index") is not None
            }

            seen_chunk_indexes = set()

            for _, chunk_index, chunk_id in ref_rows:
                if chunk_index in seen_chunk_indexes:
                    continue
                seen_chunk_indexes.add(chunk_index)

                chunk = chunk_lookup.get(chunk_index)
                if not chunk:
                    continue

                text = (
                    chunk.get("content", {}).get("text")
                    or chunk.get("text")
                    or ""
                )

                matched_terms = sorted(query_terms & self._tokenize(text))
                if not matched_terms:
                    filtered_out_by_query += 1
                    continue

                results.append(
                    {
                        "source_hash": source_hash,
                        "logical_path": logical_path,
                        "source_file_type": source_file_type,
                        "chunk_id": chunk_id,
                        "chunk_index": chunk_index,
                        "text": text,
                        "char_count": chunk.get("content", {}).get("char_count", 0),
                        "token_estimate": chunk.get("content", {}).get("token_estimate", 0),
                        "matched_terms_prefilter": matched_terms,
                    }
                )

        return {
            "query_text": query_text,
            "allowed_file_types": sorted(allowed_file_types),
            "candidate_count": len(results),
            "total_chunks_seen": total_chunks_seen,
            "filtered_out_by_file_type": filtered_out_by_file_type,
            "filtered_out_by_query": filtered_out_by_query,
            "results": results,
        }

    @staticmethod
    def _tokenize(text: str) -> set[str]:
        raw = _TOKEN_RE.findall(text.lower())
        return {
            token
            for token in raw
            if len(token) >= 3 and token not in _STOPWORDS
        }