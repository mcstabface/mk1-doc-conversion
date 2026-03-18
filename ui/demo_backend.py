from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any


def _latest_json(path: Path) -> dict[str, Any]:
    files = sorted(path.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return {}
    return json.loads(files[0].read_text(encoding="utf-8"))


def load_demo_summary(dataset_name: str) -> dict[str, Any]:
    artifact_root = Path("artifacts") / dataset_name
    stats_dir = artifact_root / "corpus_stats"
    data = _latest_json(stats_dir)

    if data:
        return {
            "dataset_name": dataset_name,
            "document_count": data.get("document_count", 0),
            "chunk_count": data.get("chunk_count", 0),
            "embedding_count": data.get("embedding_count", 0),
            "artifact_types": data.get("artifact_types", []),
            "last_updated": data.get("created_at") or data.get("generated_at") or "unknown",
        }

    search_context_dir = artifact_root / "search_context"
    chunk_dir = artifact_root / "search_context_chunks"
    embedding_dir = artifact_root / "embeddings"

    document_count = len(list(search_context_dir.glob("*.json"))) if search_context_dir.exists() else 0
    chunk_count = len(list(chunk_dir.glob("*.json"))) if chunk_dir.exists() else 0
    embedding_count = len(list(embedding_dir.glob("*.json"))) if embedding_dir.exists() else 0

    artifact_types = []
    if search_context_dir.exists():
        artifact_types.append("search_context_document")
    if chunk_dir.exists():
        artifact_types.append("search_context_chunks")
    if embedding_dir.exists():
        artifact_types.append("embeddings")

    last_updated = "unknown"
    existing_dirs = [p for p in [search_context_dir, chunk_dir, embedding_dir] if p.exists()]
    if existing_dirs:
        latest_mtime = max(p.stat().st_mtime for p in existing_dirs)
        last_updated = str(latest_mtime)

    return {
        "dataset_name": dataset_name,
        "document_count": document_count,
        "chunk_count": chunk_count,
        "embedding_count": embedding_count,
        "artifact_types": artifact_types,
        "last_updated": last_updated,
    }


def run_demo_query(dataset_name: str, query_text: str) -> dict[str, Any]:
    chunk_root = Path("artifacts") / dataset_name / "search_context_chunks"

    cmd = [
        sys.executable,
        "query_search_context.py",
        "--query",
        query_text,
        "--chunk-root",
        str(chunk_root),
        "--artifact-root",
        str(Path("artifacts") / dataset_name),
        "--ranker",
        "hybrid",
    ]
    print("RUNNING COMMAND:", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"Query script failed: {result.stderr}")

    artifact_root = Path("artifacts") / dataset_name
    answer = _latest_json(artifact_root / "query_answer")
    diagnostics = _latest_json(artifact_root / "query_diagnostics")

    raw_results = diagnostics.get("results", [])

    ranked = []
    for item in raw_results:
        if isinstance(item, dict):
            ranked.append(
                {
                    "source": item.get("logical_path") or item.get("source") or item.get("chunk_id") or "source",
                    "score": item.get("score", ""),
                    "text": item.get("text") or item.get("snippet") or item.get("content") or str(item),
                }
            )
    evidence = answer.get("evidence", []) or answer.get("sources", [])

    return {
        "answer_text": answer.get("answer_text", "No answer returned."),
        "ranked_results": ranked,
        "evidence": evidence,
        "diagnostics_summary": diagnostics,
        "repeatability_signature": {
            "query_text": query_text,
            "top_result_count": len(ranked),
        },
        "run_id": diagnostics.get("run_id"),
    }