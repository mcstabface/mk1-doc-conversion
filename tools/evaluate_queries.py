from __future__ import annotations

import json
import time
from pathlib import Path
import sys
import hashlib

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mk1_io.artifact_writer import write_validated_artifact


def _safe_query_stem(query_text: str, ranker: str) -> str:
    safe_query = "_".join(str(query_text).lower().split())[:80]
    return f"{safe_query}.{ranker}"


def _vector_deps_available() -> tuple[bool, str | None]:
    """Return (available, reason_if_unavailable).

    Deterministic check: only inspects local Python import availability.
    """
    try:
        import faiss  # noqa: F401
    except Exception as e:
        return False, f"faiss_import_failed: {type(e).__name__}: {e}"

    return True, None


QUERY_SET_PATH = Path("configs/query_eval_set.json")

def _compute_query_metrics(
    expected_sources: list[str],
    returned_sources: list[str],
    top_k: int,
) -> dict:
    expected_set = set(expected_sources)
    returned_top_k = list(returned_sources[:top_k])

    matched_sources = [src for src in returned_top_k if src in expected_set]

    precision_at_k = (
        len(matched_sources) / top_k if top_k > 0 else 0.0
    )

    recall_at_k = (
        len(set(matched_sources)) / len(expected_set)
        if expected_set
        else 0.0
    )

    reciprocal_rank = 0.0
    for idx, src in enumerate(returned_top_k, start=1):
        if src in expected_set:
            reciprocal_rank = 1.0 / idx
            break

    return {
        "matched_sources": matched_sources,
        "precision_at_k": round(precision_at_k, 6),
        "recall_at_k": round(recall_at_k, 6),
        "reciprocal_rank": round(reciprocal_rank, 6),
    }

def _extract_returned_sources(results: list[dict]) -> list[str]:
    returned = []

    for item in results:
        source = (
            item.get("source_path")
            or item.get("logical_path")
            or item.get("document_path")
            or item.get("doc_id")
            or item.get("source")
        )

        if source is not None:
            returned.append(str(source))

    return returned

def main():

    query_set_bytes = QUERY_SET_PATH.read_bytes()
    config = json.loads(query_set_bytes)
    queries = config.get("queries", [])

    dataset_name = config.get("dataset", "test_source_mid")

    ranker_name = config.get("ranker", "hybrid")
    top_k = int(config.get("top_k", 10))

    vector_available, vector_unavailable_reason = _vector_deps_available()

    eval_environment = {
        "dataset": dataset_name,
        "ranker": ranker_name,
        "top_k": top_k,
        "vector_deps_available": vector_available,
        "vector_deps_unavailable_reason": vector_unavailable_reason,
    }

    results = []

    for entry in queries:
        if isinstance(entry, str):
            query_text = entry
            expected_sources = []
        else:
            query_text = entry["query"]
            expected_sources = entry.get("expected_sources", [])

        query_status = "COMPLETE"
        query_status_reason = None
        effective_ranker = ranker_name

        if ranker_name == "hybrid" and not vector_available:
            query_status = "FALLBACK_BM25_VECTOR_DEPS_UNAVAILABLE"
            query_status_reason = vector_unavailable_reason
            effective_ranker = "bm25"

        dataset_root = Path(f"artifacts/{dataset_name}").resolve()
        diagnostics_dir = dataset_root / "query_diagnostics"
        diagnostics_dir.mkdir(parents=True, exist_ok=True)

        artifact_stem = _safe_query_stem(query_text, effective_ranker)
        diagnostics_path = diagnostics_dir / f"{artifact_stem}.diagnostics.json"

        if effective_ranker == "bm25":
            from experts.llm_search.search_context_query_expert import SearchContextQueryExpert
            from experts.llm_search.search_context_bm25_rank_expert import SearchContextBm25RankExpert

            query_expert = SearchContextQueryExpert()
            bm25_rank_expert = SearchContextBm25RankExpert()

            query_payload = {
                "query_text": query_text,
                "chunk_artifact_root": f"artifacts/{dataset_name}/search_context_chunks",
                "top_k": top_k,
                "allowed_file_types": [],
            }

            query_result = query_expert.run(query_payload)

            rank_payload = {
                "query_text": query_text,
                "results": query_result["results"],
                "top_k": top_k,
            }

            rank_result = bm25_rank_expert.run(rank_payload)

            returned_results = rank_result.get("results", [])
            diag = {
                "artifact_type": "query_diagnostics",
                "schema_version": "query_diagnostics_v1",
                "producer_expert": "query_eval_tool",
                "run_id": None,
                "status": "COMPLETE",
                "created_utc": int(time.time()),
                "query_text": query_text,
                "ranker": effective_ranker,
                "expanded_queries": [query_text],
                "retrieval_config": {
                    "chunk_root": f"artifacts/{dataset_name}/search_context_chunks",
                    "top_k": top_k,
                    "ranker_effective": effective_ranker,
                    "ranker_requested": ranker_name,
                },
                "candidate_count": query_result.get(
                    "candidate_count",
                    len(query_result.get("results", [])),
                ),
                "ranked_count": len(returned_results),
                "final_returned_count": len(returned_results),
                "context_used_count": 0,
                "corpus_chunk_count": query_result.get("total_chunks_seen"),
                "results": returned_results,
                "retrieval_trace": [
                    {
                        "final_rank": idx,
                        "logical_path": r.get("logical_path"),
                        "chunk_index": r.get("chunk_index"),
                        "chunk_id": r.get("chunk_id"),
                        "lexical_score": r.get("score"),
                        "final_score": r.get("score"),
                    }
                    for idx, r in enumerate(returned_results, start=1)
                ],
            }

            write_validated_artifact(diagnostics_path, diag)
        else:
            from query_search_context import run_query_pipeline

            result = run_query_pipeline(
                query=query_text,
                chunk_root=f"artifacts/{dataset_name}/search_context_chunks",
                ranker=effective_ranker,
                max_chunks_per_source=1,
            )

            diag = result["diagnostics"]
            returned_results = result["result"].get("results", [])

        source_best = {}

        for idx, r in enumerate(returned_results, start=1):
            source = r.get("logical_path")
            if not source:
                continue

            score = float(r.get("score", 0.0))

            existing = source_best.get(source)
            if existing is None or score > existing["score"]:
                source_best[source] = {
                    "source": source,
                    "best_rank": idx,
                    "score": score,
                }

        returned_source_details = sorted(
            source_best.values(),
            key=lambda x: x["best_rank"],
        )

        returned_sources = [item["source"] for item in returned_source_details]

        expected_set = set(expected_sources)
        returned_set = set(returned_sources)

        hit_count = len(expected_set & returned_set)

        if returned_sources:
            precision_over_returned = hit_count / len(returned_sources)
        else:
            precision_over_returned = 0.0

        if top_k:
            precision_at_k_true = hit_count / top_k
        else:
            precision_at_k_true = 0.0



        recall_at_k = (
            hit_count / len(expected_sources)
            if expected_sources
            else 0.0
        )

        reciprocal_rank = 0.0
        for idx, source in enumerate(returned_sources, start=1):
            if source in expected_set:
                reciprocal_rank = 1.0 / idx
                break

        matched_sources = [
            source for source in returned_sources[:top_k]
            if source in expected_set
        ]

        top_k_trace = []
        diagnostics_read_status = "MISSING"
        diagnostics_read_error = None

        if diagnostics_path.exists():
            try:
                diag_art = json.loads(diagnostics_path.read_text(encoding="utf-8"))
                top_k_trace = list(diag_art.get("retrieval_trace", []))[:top_k]
                diagnostics_read_status = "OK"
            except Exception as e:
                top_k_trace = []
                diagnostics_read_status = "ERROR"
                diagnostics_read_error = f"{type(e).__name__}: {e}"

        retrieval_config = {
            "chunk_root": f"artifacts/{dataset_name}/search_context_chunks",
            "top_k": top_k,
            "ranker_requested": ranker_name,
            "ranker_effective": effective_ranker,
        }

        retrieval_diagnostics_summary = {
            "candidate_count": diag.get("candidate_count"),
            "ranked_count": diag.get("ranked_count"),
            "final_returned_count": diag.get("final_returned_count"),
            "context_used_count": diag.get("context_used_count"),
            "lexical_candidate_count": diag.get("lexical_candidate_count"),
            "vector_candidate_count": diag.get("vector_candidate_count"),
            "corpus_chunk_count": diag.get("corpus_chunk_count"),
            "corpus_document_count": diag.get("corpus_document_count"),
            "hybrid_fusion_present": bool(diag.get("hybrid_fusion")),
            "top_k_trace": [
                {
                    "final_rank": row.get("final_rank"),
                    "logical_path": row.get("logical_path"),
                    "chunk_index": row.get("chunk_index"),
                    "lexical_score": row.get("lexical_score"),
                    "vector_score": row.get("vector_score"),
                    "fusion_score": row.get("fusion_score"),
                    "final_score": row.get("final_score"),
                }
                for row in (top_k_trace or [])
            ],
        }

        results.append(
            {
                "query": query_text,
                "query_text": query_text,
                "ranker": ranker_name,
                "effective_ranker": effective_ranker,
                "top_k": top_k,
                "retrieval_config": retrieval_config,
                "status": query_status,
                "status_reason": query_status_reason,
                "diagnostics_artifact_path": str(diagnostics_path),
                "diagnostics_read_status": diagnostics_read_status,
                "diagnostics_read_error": diagnostics_read_error,
                "top_k_trace": top_k_trace,
                "expected_sources": expected_sources,
                "returned_sources": returned_sources[:top_k],
                "returned_source_details": returned_source_details[:top_k],
                "matched_sources": matched_sources,
                "hit_count": hit_count,
                "precision_at_k": precision_at_k_true,
                "precision_over_returned": precision_over_returned,
                "recall_at_k": recall_at_k,
                "reciprocal_rank": reciprocal_rank,
                "candidate_count": diag.get("candidate_count"),
                "ranked_count": diag.get("ranked_count"),
                "returned_count": diag.get("final_returned_count"),
                "context_used_count": diag.get("context_used_count"),
                "corpus_chunk_count": diag.get("corpus_chunk_count"),
                "retrieval_diagnostics_summary": retrieval_diagnostics_summary,
            }
        )

    diagnostics_status_counts = {
        "OK": 0,
        "MISSING": 0,
        "ERROR": 0,
    }

    diagnostics_error_queries = []

    for row in results:
        status = row.get("diagnostics_read_status")
        if status in diagnostics_status_counts:
            diagnostics_status_counts[status] += 1
        else:
            diagnostics_status_counts["ERROR"] += 1
            diagnostics_error_queries.append({
                "query_text": row.get("query_text"),
                "diagnostics_artifact_path": row.get("diagnostics_artifact_path"),
                "diagnostics_read_error": f"unexpected_status: {status}",
            })

        if status == "ERROR":
            diagnostics_error_queries.append({
                "query_text": row.get("query_text"),
                "diagnostics_artifact_path": row.get("diagnostics_artifact_path"),
                "diagnostics_read_error": row.get("diagnostics_read_error"),
            })

    artifact = {
        "artifact_type": "query_eval",
        "schema_version": "query_eval_v1",
        "created_utc": int(time.time()),
        "producer_expert": "query_eval_tool",
        "run_id": None,
        "status": "COMPLETE",
        "eval_environment": eval_environment,
        "query_count": len(queries),
        "diagnostics_health_summary": {
            "status_counts": diagnostics_status_counts,
            "error_queries": diagnostics_error_queries,
        },
        "mean_precision_at_k": (
            sum(r["precision_at_k"] for r in results) / len(results)
            if results else 0.0
        ),
        "mean_recall_at_k": (
            sum(r["recall_at_k"] for r in results) / len(results)
            if results else 0.0
        ),
        "mrr": (
            sum(r["reciprocal_rank"] for r in results) / len(results)
            if results else 0.0
        ),
        "results": results,
    }

    artifact_root = Path(f"artifacts/{dataset_name}").resolve()

    output = artifact_root / "query_eval" / "query_eval.hybrid.json"
    output.parent.mkdir(parents=True, exist_ok=True)

    write_validated_artifact(output, artifact)

    manifest = {
        "artifact_type": "query_eval_run_manifest",
        "schema_version": "query_eval_run_manifest_v1",
        "created_utc": int(time.time()),
        "producer_expert": "query_eval_tool",
        "run_id": None,
        "status": "COMPLETE",
        "dataset": dataset_name,
        "query_set_path": str(QUERY_SET_PATH),
        "query_set_sha256": hashlib.sha256(query_set_bytes).hexdigest(),
        "chunk_root": f"artifacts/{dataset_name}/search_context_chunks",
        "ranker_requested": ranker_name,
        "top_k": top_k,
        "vector_deps_available": vector_available,
        "vector_deps_unavailable_reason": vector_unavailable_reason,
    }

    manifest_path = artifact_root / "query_eval" / f"query_eval_run_manifest.{ranker_name}.json"
    write_validated_artifact(manifest_path, manifest)

    print("QUERY EVAL WRITTEN:", output)
    print("QUERY EVAL MANIFEST WRITTEN:", manifest_path)



if __name__ == "__main__":
    main()