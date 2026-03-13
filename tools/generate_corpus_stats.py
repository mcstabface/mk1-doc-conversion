from __future__ import annotations

import json
import time
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

import sys

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mk1_io.artifact_writer import write_validated_artifact


def _percentile(sorted_values: list[int], p: float) -> int:
    if not sorted_values:
        return 0
    if p <= 0:
        return int(sorted_values[0])
    if p >= 1:
        return int(sorted_values[-1])
    idx = int(round((len(sorted_values) - 1) * p))
    return int(sorted_values[idx])


def _infer_file_type(logical_path: str | None) -> str:
    if not logical_path:
        return "unknown"
    suffix = Path(str(logical_path)).suffix.lower().lstrip(".")
    return suffix or "unknown"


def generate_corpus_stats(dataset_root: Path) -> Path:
    chunk_dir = dataset_root / "search_context_chunks"
    if not chunk_dir.exists():
        raise ValueError(f"search_context_chunks not found: {chunk_dir}")

    chunk_paths = sorted(chunk_dir.glob("*.json"))

    chunk_count = 0
    document_count = 0
    source_paths = set()
    file_type_counts: Counter[str] = Counter()

    # Embedding coverage (optional; deterministic based on local artifact files)
    embedding_dir = dataset_root / "embeddings"
    embedding_paths = sorted(embedding_dir.glob("*.json")) if embedding_dir.exists() else []
    embedding_count = len(embedding_paths)
    embedding_chunk_ids = set()

    invalid_embedding_json_count = 0
    invalid_embedding_missing_chunk_id_count = 0

    for p in embedding_paths:
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            invalid_embedding_json_count += 1
            continue

        chunk_id = data.get("chunk_id")
        if chunk_id:
            embedding_chunk_ids.add(str(chunk_id))
        else:
            invalid_embedding_missing_chunk_id_count += 1

    chunk_ids = set()
    chunk_id_seen_counts: Counter[str] = Counter()

    valid_chunk_id_count = 0
    valid_text_chunk_count = 0

    char_counts: list[int] = []
    token_estimates: list[int] = []

    missing_logical_path = 0
    missing_text = 0

    invalid_chunk_collection_json_count = 0
    ignored_non_chunk_collection_count = 0
    invalid_chunk_collection_missing_chunks_count = 0

    invalid_chunk_item_non_dict_count = 0
    invalid_chunk_missing_chunk_id_count = 0
    invalid_chunk_missing_text_count = 0
    invalid_chunk_missing_token_estimate_count = 0
    empty_text_chunk_count = 0

    for p in chunk_paths:
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            invalid_chunk_collection_json_count += 1
            continue

        # search_context_chunks are stored as search_context_chunk_collection artifacts.
        if data.get("artifact_type") != "search_context_chunk_collection":
            ignored_non_chunk_collection_count += 1
            continue

        source = data.get("source") or {}
        logical_path = source.get("logical_path")
        if not logical_path:
            missing_logical_path += 1
        else:
            source_paths.add(str(logical_path))

        file_type_counts[_infer_file_type(logical_path)] += 1

        chunks = data.get("chunks")
        if not isinstance(chunks, list):
            invalid_chunk_collection_missing_chunks_count += 1
            continue

        document_count += 1

        for chunk in chunks:
            if not isinstance(chunk, dict):
                invalid_chunk_item_non_dict_count += 1
                continue

            chunk_id = chunk.get("chunk_id")
            if chunk_id:
                chunk_id_str = str(chunk_id)
                chunk_ids.add(chunk_id_str)
                chunk_id_seen_counts[chunk_id_str] += 1
                valid_chunk_id_count += 1
            else:
                invalid_chunk_missing_chunk_id_count += 1

            text = chunk.get("text")
            if not isinstance(text, str):
                missing_text += 1
                invalid_chunk_missing_text_count += 1
                text = ""

            if text:
                valid_text_chunk_count += 1
            else:
                # Distinguish empty-string text from missing/non-string text.
                # If it was missing/non-string, invalid_chunk_missing_text_count already captured it.
                empty_text_chunk_count += 1

            chunk_count += 1
            char_counts.append(len(text))

            token_est = None
            content = chunk.get("content")
            if isinstance(content, dict):
                token_est = content.get("token_estimate")

            if isinstance(token_est, int):
                token_estimates.append(token_est)
            else:
                invalid_chunk_missing_token_estimate_count += 1
                token_estimates.append(int(round(len(text) / 4)) if text else 0)

    char_counts_sorted = sorted(char_counts)
    token_sorted = sorted(token_estimates)

    matched_embedding_count = len(chunk_ids & embedding_chunk_ids)
    missing_embedding_count = len(chunk_ids - embedding_chunk_ids)
    orphan_embedding_count = len(embedding_chunk_ids - chunk_ids)

    if len(chunk_ids) > 0:
        embedding_coverage_pct = float(round((matched_embedding_count / len(chunk_ids)) * 100.0, 6))
    else:
        embedding_coverage_pct = 0.0

    if valid_chunk_id_count > 0:
        embedding_coverage_pct_valid_chunks = float(
            round((matched_embedding_count / valid_chunk_id_count) * 100.0, 6)
        )
    else:
        embedding_coverage_pct_valid_chunks = 0.0

    duplicate_chunk_id_key_count = sum(
        1 for count in chunk_id_seen_counts.values() if count > 1
    )

    duplicate_chunk_id_count = sum(
        (count - 1) for count in chunk_id_seen_counts.values() if count > 1
    )

    duplicate_chunk_id_examples = [
        chunk_id
        for chunk_id, count in sorted(chunk_id_seen_counts.items(), key=lambda kv: kv[0])
        if count > 1
    ][:25]

    artifact = {
        "artifact_type": "corpus_stats",
        "schema_version": "corpus_stats_v1",
        "created_utc": int(time.time()),
        "producer_expert": "corpus_stats_tool",
        "run_id": None,
        "status": "COMPLETE",
        "dataset_root": str(dataset_root),
        "chunk_dir": str(chunk_dir),
        "document_count": document_count,
        "chunk_count": chunk_count,
        "embedding_count": embedding_count,
        "matched_embedding_count": matched_embedding_count,
        "missing_embedding_count": missing_embedding_count,
        "orphan_embedding_count": orphan_embedding_count,
        "embedding_coverage_pct": float(embedding_coverage_pct),
        "valid_chunk_id_count": valid_chunk_id_count,
        "valid_text_chunk_count": valid_text_chunk_count,
        "embedding_coverage_pct_valid_chunks": float(embedding_coverage_pct_valid_chunks),
        "unique_source_count": len(source_paths),
        "missing_logical_path_count": missing_logical_path,
        "missing_text_count": missing_text,
        "file_type_counts": dict(sorted(file_type_counts.items(), key=lambda kv: kv[0])),
        "char_count_stats": {
            "min": int(char_counts_sorted[0]) if char_counts_sorted else 0,
            "p50": _percentile(char_counts_sorted, 0.50),
            "p95": _percentile(char_counts_sorted, 0.95),
            "max": int(char_counts_sorted[-1]) if char_counts_sorted else 0,
            "mean": round(sum(char_counts) / len(char_counts), 6) if char_counts else 0.0,
        },
        "token_estimate_stats": {
            "min": int(token_sorted[0]) if token_sorted else 0,
            "p50": _percentile(token_sorted, 0.50),
            "p95": _percentile(token_sorted, 0.95),
            "max": int(token_sorted[-1]) if token_sorted else 0,
            "mean": round(sum(token_estimates) / len(token_estimates), 6) if token_estimates else 0.0,
        },
        "integrity_counters": {
            "invalid_chunk_collection_json_count": invalid_chunk_collection_json_count,
            "ignored_non_chunk_collection_count": ignored_non_chunk_collection_count,
            "invalid_chunk_collection_missing_chunks_count": invalid_chunk_collection_missing_chunks_count,
            "invalid_chunk_item_non_dict_count": invalid_chunk_item_non_dict_count,
            "invalid_chunk_missing_chunk_id_count": invalid_chunk_missing_chunk_id_count,
            "invalid_chunk_missing_text_count": invalid_chunk_missing_text_count,
            "invalid_chunk_missing_token_estimate_count": invalid_chunk_missing_token_estimate_count,
            "empty_text_chunk_count": empty_text_chunk_count,
            "duplicate_chunk_id_key_count": int(duplicate_chunk_id_key_count),
            "duplicate_chunk_id_count": int(duplicate_chunk_id_count),
            "duplicate_chunk_id_examples": duplicate_chunk_id_examples,
            "invalid_embedding_json_count": invalid_embedding_json_count,
            "invalid_embedding_missing_chunk_id_count": invalid_embedding_missing_chunk_id_count,
        },
    }

    out_dir = dataset_root / "corpus_stats"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "corpus_stats.json"

    # Use validated artifact writer for consistent formatting + future schema enforcement.
    write_validated_artifact(out_path, artifact)

    return out_path


def main():
    dataset_root = (PROJECT_ROOT / "artifacts" / "test_source_mid").resolve()
    out = generate_corpus_stats(dataset_root)
    print("CORPUS STATS WRITTEN:", out)


if __name__ == "__main__":
    main()
