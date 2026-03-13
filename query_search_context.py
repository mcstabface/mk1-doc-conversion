from __future__ import annotations

import argparse

from pathlib import Path

from mk1_io.artifact_writer import write_validated_artifact

from datetime import datetime, timezone

from experts.llm_search.search_context_assemble_expert import SearchContextAssembleExpert
from experts.llm_search.search_context_query_expert import SearchContextQueryExpert
from experts.llm_search.search_context_rank_expert import SearchContextRankExpert
from experts.llm_search.search_context_answer_expert import SearchContextAnswerExpert
from experts.llm_search.search_context_bm25_rank_expert import SearchContextBm25RankExpert
from experts.llm_search.score_gap_filter import ScoreGapFilter, ScoreGapFilterConfig
from experts.query.query_expansion_expert import QueryExpansionExpert
from experts.llm_search.query_embedding_expert import QueryEmbeddingExpert
from experts.llm_search.vector_search_expert import VectorSearchExpert
from experts.llm_search.hybrid_fusion_expert import HybridFusionExpert
from experts.llm_search.mmr_diversity_ranker import (
    MMRDiversityRanker,
    MMRDiversityRankerConfig,
)

DEFAULT_TOP_K = 5

def run_query_pipeline(
    query: str,
    chunk_root: str,
    ranker: str = "overlap",
    max_chunks_per_source: int = 1,
    allowed_file_types: list[str] | None = None,
):
    query_expert = SearchContextQueryExpert()
    rank_expert = SearchContextRankExpert()
    answer_expert = SearchContextAnswerExpert()
    assemble_expert = SearchContextAssembleExpert()
    bm25_rank_expert = SearchContextBm25RankExpert()
    query_expansion_expert = QueryExpansionExpert()
    query_embedding_expert = QueryEmbeddingExpert()
    vector_search_expert = VectorSearchExpert()
    hybrid_fusion_expert = HybridFusionExpert()

    chunk_root_path = Path(chunk_root).resolve()

    # artifacts/<dataset>/search_context_chunks
    dataset_root = chunk_root_path.parent

    embedding_root = dataset_root / "embeddings"

    query_payload = {
        "query_text": query,
        "chunk_artifact_root": chunk_root,
        "top_k": DEFAULT_TOP_K,
        "allowed_file_types": allowed_file_types or [],
    }

    query_result = query_expert.run(query_payload)

    expansion_result = query_expansion_expert.expand(query)
    expanded_queries = expansion_result["expanded_queries"]

    diagnostics = {
        "query_text": query,
        "ranker": ranker,
        "expanded_queries": expanded_queries,
        "candidate_count": query_result.get("candidate_count", len(query_result.get("results", []))),
        "ranked_count": 0,
        "lexical_candidate_count": 0,
        "vector_candidate_count": 0,
        "allowed_file_types": allowed_file_types or [],
        "total_chunks_seen": query_result.get("total_chunks_seen", 0),
        "filtered_out_by_file_type": query_result.get("filtered_out_by_file_type", 0),
        "post_file_filter_candidate_count": query_result.get(
            "candidate_count",
            len(query_result.get("results", [])),
        ),
    }

    all_ranked_results = []

    for expanded_query in expanded_queries:
        rank_payload = {
            "query_text": expanded_query,
            "results": query_result["results"],
            "top_k": DEFAULT_TOP_K,
        }

        if ranker == "bm25":
            expanded_result = bm25_rank_expert.run(rank_payload)

        elif ranker == "hybrid":
            rank_result = bm25_rank_expert.run(rank_payload)

            lexical_results = sorted(
                rank_result.get("results", []),
                key=lambda x: float(x.get("score", 0.0)),
                reverse=True,
            )[:DEFAULT_TOP_K]

            diagnostics["lexical_candidate_count"] = len(lexical_results)

            query_embedding = query_embedding_expert.run({
                "query_text": query,
                "embedding_model": "nomic-embed-text",
                "endpoint": "http://localhost:11434/api/embeddings",
            })

            vector_result = vector_search_expert.run({
                "query_vector": query_embedding["vector"],
                "embedding_root": str(embedding_root),
                "top_k": DEFAULT_TOP_K,
            })

            diagnostics["vector_candidate_count"] = len(vector_result.get("results", []))

            fused = hybrid_fusion_expert.run({
                "lexical_results": lexical_results,
                "vector_results": vector_result.get("results", []),
                "vector_bonus_weight": 0.10,
                "vector_only_score_floor": 0.60,
                "top_k": DEFAULT_TOP_K,
            })

            # Preserve per-expanded-query fusion observability.
            diagnostics.setdefault("hybrid_fusion", {
                "config": fused.get("fusion_config"),
                "per_expanded_query": [],
            })
            diagnostics["hybrid_fusion"]["per_expanded_query"].append({
                "expanded_query": expanded_query,
                "lexical_input_count": len(lexical_results),
                "vector_input_count": len(vector_result.get("results", [])),
                "fused_count": len(fused.get("results", [])),
                "top_items": [
                    {
                        "logical_path": r.get("logical_path"),
                        "chunk_index": r.get("chunk_index"),
                        "lexical_score": r.get("lexical_score"),
                        "vector_score": r.get("vector_score"),
                        "fusion_bonus": r.get("fusion_bonus"),
                        "fusion_doc_boost": r.get("fusion_doc_boost"),
                        "fusion_score": r.get("fusion_score"),
                    }
                    for r in fused.get("results", [])
                ],
            })

            expanded_result = {
                "query_text": query,
                "results": fused["results"],
            }

        else:
            expanded_result = rank_expert.run(rank_payload)

        original_token_count = len(query.split())
        expanded_token_count = len(expanded_query.split())

        if expanded_query == query:
            expansion_weight = 1.00
        elif expanded_token_count > original_token_count:
            expansion_weight = 0.75
        else:
            expansion_weight = 0.90

        for item in expanded_result["results"]:
            weighted_item = dict(item)
            raw_score = float(weighted_item.get("score", 0.0))
            weighted_item["raw_score"] = raw_score
            weighted_item["score"] = raw_score * expansion_weight
            weighted_item["matched_query"] = expanded_query
            weighted_item["expansion_weight"] = expansion_weight
            all_ranked_results.append(weighted_item)

    dedup = {}
    for item in all_ranked_results:
        key = (
            item.get("source_name") or item.get("logical_path") or item.get("source_path"),
            item.get("chunk_index", item.get("chunk_id")),
        )
        if key not in dedup or float(item.get("score", 0.0)) > float(dedup[key].get("score", 0.0)):
            dedup[key] = item

    sorted_results = sorted(
        dedup.values(),
        key=lambda x: float(x.get("score", 0.0)),
        reverse=True,
    )

    result = {
        "query_text": query,
        "expanded_queries": expanded_queries,
        "candidate_count": query_result.get("candidate_count", len(query_result.get("results", []))),
        "ranked_count": len(sorted_results),
        "results": sorted_results,
    }

    diagnostics["ranked_count"] = result["ranked_count"]

    score_gap_filter = ScoreGapFilter(
        ScoreGapFilterConfig(
            relative_score_floor=0.35,
            min_results=3,
        )
    )

    result["results"] = score_gap_filter.filter_candidates(result["results"])
    result["returned_count"] = len(result["results"])

    mmr_ranker = MMRDiversityRanker(
        MMRDiversityRankerConfig(
            lambda_weight=0.5,
            max_results=5,
        )
    )

    result["results"] = mmr_ranker.rerank(result["results"])
    result["returned_count"] = len(result["results"])



    diagnostics["final_returned_count"] = len(result["results"])
    diagnostics["results"] = []
    diagnostics["retrieval_trace"] = []

    source_seen_counts = {}
    diversified_results = []

    for item in result["results"]:
        source_key = item.get("logical_path") or item.get("source_path") or "unknown"
        seen = source_seen_counts.get(source_key, 0)

        diversified_item = dict(item)

        if seen == 0:
            source_diversity_weight = 1.00
        elif seen == 1:
            source_diversity_weight = 0.92
        else:
            source_diversity_weight = 0.80

        diversified_item["pre_diversity_score"] = float(diversified_item.get("score", 0.0))
        diversified_item["source_diversity_weight"] = source_diversity_weight
        diversified_item["score"] = diversified_item["pre_diversity_score"] * source_diversity_weight

        diversified_results.append(diversified_item)
        source_seen_counts[source_key] = seen + 1

    result["results"] = sorted(
        diversified_results,
        key=lambda x: float(x.get("score", 0.0)),
        reverse=True,
    )

    for final_rank, r in enumerate(result["results"], start=1):
        trace_row = {
            "final_rank": final_rank,
            "logical_path": r.get("logical_path"),
            "chunk_index": r.get("chunk_index"),
            "chunk_id": r.get("chunk_id"),
            "seen_in_lexical": r.get("seen_in_lexical"),
            "seen_in_vector": r.get("seen_in_vector"),
            "lexical_score": r.get("lexical_score"),
            "vector_score": r.get("vector_score"),
            "fusion_score": r.get("fusion_score", r.get("score")),
            "final_score": r.get("score"),
            "raw_score": r.get("raw_score"),
            "expansion_weight": r.get("expansion_weight"),
            "matched_query": r.get("matched_query"),
            "pre_diversity_score": r.get("pre_diversity_score"),
            "source_diversity_weight": r.get("source_diversity_weight"),
        }
        diagnostics["retrieval_trace"].append(trace_row)

    diagnostics["results"] = result["results"]

    assemble_payload = {
        "query_text": query,
        "results": result["results"],
        "expanded_queries": result.get("expanded_queries", [query]),
        "max_context_chars": 6000,
        "max_chunks_per_source": max_chunks_per_source,
    }

    assembled = assemble_expert.run(assemble_payload)
    assembled["ranker"] = ranker
    assembled["top_k"] = 5

    diagnostics["context_used_count"] = assembled.get("used_count", 0)
    diagnostics["included_results"] = assembled.get("included_results", [])
    diagnostics["excluded_results"] = assembled.get("excluded_results", [])

    # Corpus scaling / size metrics for observability.
    try:
        chunk_files = list((dataset_root / "search_context_chunks").glob("*.search_context_chunks.json"))
        diagnostics["corpus_document_count"] = len(chunk_files)
    except Exception:
        diagnostics["corpus_document_count"] = None
    diagnostics["artifact_type"] = "query_diagnostics"
    diagnostics["schema_version"] = "query_diagnostics_v1"
    diagnostics["producer_expert"] = "query_search_context"
    diagnostics["run_id"] = None
    diagnostics["status"] = "COMPLETE"

    answer_payload = {
        "query_text": assembled.get("query_text"),
        "context_text": assembled.get("context_text"),
        "sources": assembled.get("sources", []),
    }

    answer_result = answer_expert.run(answer_payload)
    answer_result["ranker"] = ranker
    answer_result["top_k"] = 5

    return {
        "result": result,
        "assembled": assembled,
        "answer_result": answer_result,
        "diagnostics": diagnostics,
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--query",
        required=True,
        help="Search query text"
    )
    parser.add_argument(
        "--chunk-root",
        required=True,
        help="Path to search_context_chunks directory"
    )

    parser.add_argument(
        "--ranker",
        choices=["overlap", "bm25", "hybrid"],
        default="overlap",
        help="Ranking strategy to use"
    )

    parser.add_argument(
        "--max-chunks-per-source",
        type=int,
        default=1,
        help="Maximum number of chunks to include from any single source in assembled context"
    )

    parser.add_argument(
        "--allowed-file-types",
        nargs="*",
        default=None,
        help="Inclusive allow-list of source file types to search, e.g. pdf docx doc",
    )

    parser.add_argument(
        "--artifact-root",
        default="./artifacts",
        help="Root directory for query artifacts"
    )

    args = parser.parse_args()

    pipeline = run_query_pipeline(
        query=args.query,
        chunk_root=args.chunk_root,
        ranker=args.ranker,
        max_chunks_per_source=args.max_chunks_per_source,
        allowed_file_types=args.allowed_file_types,
    )

    artifact_root = Path(args.artifact_root).resolve()
    chunk_root_path = Path(args.chunk_root).resolve()
    dataset_root = chunk_root_path.parent
    artifact_root = dataset_root

    result = pipeline["result"]
    assembled = pipeline["assembled"]
    answer_result = pipeline["answer_result"]
    diagnostics = pipeline["diagnostics"]

    artifact_root = Path(args.artifact_root).resolve()

    query_artifact_dir = artifact_root / "query_context"
    query_artifact_dir.mkdir(parents=True, exist_ok=True)

    answer_artifact_dir = artifact_root / "query_answer"
    answer_artifact_dir.mkdir(parents=True, exist_ok=True)

    diagnostics_dir = artifact_root / "query_diagnostics"
    diagnostics_dir.mkdir(parents=True, exist_ok=True)

    safe_query = "_".join(args.query.lower().split())[:80]
    artifact_stem = f"{safe_query}.{args.ranker}"

    query_artifact_path = query_artifact_dir / f"{artifact_stem}.query_context.json"
    answer_artifact_path = answer_artifact_dir / f"{artifact_stem}.query_answer.json"
    diagnostics_path = diagnostics_dir / f"{artifact_stem}.diagnostics.json"

    print("\nQUERY:", result["query_text"])
    print("CANDIDATE CHUNKS:", result["candidate_count"])
    print("RANKED CHUNKS:", result["ranked_count"])
    print("RETURNED CHUNKS:", result["returned_count"])
    print("CONTEXT CHUNKS USED:", assembled["used_count"])
    print("QUERY CONTEXT ARTIFACT:", query_artifact_path)
    print("RANKER:", args.ranker)
    print("ALLOWED FILE TYPES:", args.allowed_file_types or "ALL")

    print("\nTOP 5 RESULTS\n")

    if not result["results"]:
        print("No matching chunks found.")
    else:
        for r in result["results"][:5]:
            print("SCORE:", r["score"])
            print(f"RAW SCORE: {r.get('raw_score', r.get('score'))}")
            print(f"EXPANSION WEIGHT: {r.get('expansion_weight', 1.0)}")
            print(f"MATCHED QUERY: {r.get('matched_query', args.query)}")
            print("PHRASE BONUS:", r.get("phrase_bonus", 0))
            print("MATCHED TERMS:", ", ".join(r["matched_terms"]))
            print("SOURCE:", r["logical_path"])
            print("CHUNK:", r["chunk_index"])
            print(r["text"][:200].replace("\n", " "))
            print("-" * 60)

    print("\nASSEMBLED CONTEXT\n")
    print(assembled["context_text"])

    query_artifact_dir = artifact_root / "query_context"
    query_artifact_dir.mkdir(parents=True, exist_ok=True)

    safe_query = "_".join(args.query.lower().split())[:80]
    artifact_stem = f"{safe_query}.{args.ranker}"
    query_artifact_path = query_artifact_dir / f"{artifact_stem}.query_context.json"

    answer_artifact_dir = artifact_root / "query_answer"
    answer_artifact_dir.mkdir(parents=True, exist_ok=True)

    answer_artifact_path = answer_artifact_dir / f"{artifact_stem}.query_answer.json"

    now_utc = int(datetime.now(timezone.utc).timestamp())

    diagnostics["created_utc"] = now_utc

    answer_result["artifact_type"] = "query_answer"
    answer_result["schema_version"] = "query_answer_v1"
    answer_result["created_utc"] = now_utc
    answer_result["producer_expert"] = "query_search_context"
    answer_result["run_id"] = None
    answer_result["status"] = "COMPLETE"
    answer_result["source_count"] = len(answer_result.get("sources", []))

    assembled["artifact_type"] = "query_context"
    assembled["schema_version"] = "query_context_v1"
    assembled["created_utc"] = now_utc
    assembled["producer_expert"] = "query_search_context"
    assembled["run_id"] = None
    assembled["status"] = "COMPLETE"

    write_validated_artifact(answer_artifact_path, answer_result)
    write_validated_artifact(query_artifact_path, assembled)
    write_validated_artifact(diagnostics_path, diagnostics)


if __name__ == "__main__":
    main()