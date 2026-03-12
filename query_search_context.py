from __future__ import annotations

import argparse

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

    args = parser.parse_args()

    query_expert = SearchContextQueryExpert()
    rank_expert = SearchContextRankExpert()
    answer_expert = SearchContextAnswerExpert()
    assemble_expert = SearchContextAssembleExpert()
    rank_expert = SearchContextRankExpert()
    bm25_rank_expert = SearchContextBm25RankExpert()
    query_expansion_expert = QueryExpansionExpert()
    query_embedding_expert = QueryEmbeddingExpert()
    vector_search_expert = VectorSearchExpert()
    hybrid_fusion_expert = HybridFusionExpert()

    query_payload = {
        "query_text": args.query,
        "chunk_artifact_root": args.chunk_root,
        "top_k": DEFAULT_TOP_K,
    }

    query_result = query_expert.run(query_payload)

    expansion_result = query_expansion_expert.expand(args.query)
    expanded_queries = expansion_result["expanded_queries"]

    all_ranked_results = []

    for expanded_query in expanded_queries:

        rank_payload = {
            "query_text": expanded_query,
            "results": query_result["results"],
            "top_k": DEFAULT_TOP_K,
        }

        if args.ranker == "bm25":
            expanded_result = bm25_rank_expert.run(rank_payload)

        elif args.ranker == "hybrid":

            # Step 1: lexical backbone
            rank_result = bm25_rank_expert.run(rank_payload)

            lexical_results = sorted(
                rank_result.get("results", []),
                key=lambda x: float(x.get("score", 0.0)),
                reverse=True,
            )[:DEFAULT_TOP_K]

            # Step 2: embed query
            query_embedding = query_embedding_expert.run({
                "query_text": args.query,
                "embedding_model": "nomic-embed-text",
                "endpoint": "http://localhost:11434/api/embeddings",
            })

            # Step 3: vector search
            vector_result = vector_search_expert.run({
                "query_vector": query_embedding["vector"],
                "embedding_root": "artifacts/embeddings",
                "top_k": DEFAULT_TOP_K,
            })

            # Step 4: hybrid fusion
            fused = hybrid_fusion_expert.run({
                "lexical_results": lexical_results,
                "vector_results": vector_result.get("results", []),
                "vector_bonus_weight": 0.10,
                "vector_only_score_floor": 0.60,
                "top_k": DEFAULT_TOP_K,
            })

            expanded_result = {
                "query_text": args.query,
                "results": fused["results"],
            }

        else:
            expanded_result = rank_expert.run(rank_payload)

        original_token_count = len(args.query.split())
        expanded_token_count = len(expanded_query.split())

        if expanded_query == args.query:
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

    result = {
        "query_text": args.query,
        "expanded_queries": expanded_queries,
        "candidate_count": query_result.get("candidate_count", len(query_result.get("results", []))),
        "ranked_count": len(dedup),
        "results": sorted(
            dedup.values(),
            key=lambda x: float(x.get("score", 0.0)),
            reverse=True,
        ),
    }

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

    assemble_payload = {
        "query_text": args.query,
        "results": result["results"],
        "expanded_queries": result.get("expanded_queries", [args.query]),
        "max_context_chars": 6000,
        "max_chunks_per_source": args.max_chunks_per_source,
    }

    assembled = assemble_expert.run(assemble_payload)

    assembled["ranker"] = args.ranker
    assembled["top_k"] = 5

    answer_payload = {
        "query_text": assembled.get("query_text"),
        "context_text": assembled.get("context_text"),
        "sources": assembled.get("sources", []),
    }

    answer_result = answer_expert.run(answer_payload)

    answer_result["ranker"] = args.ranker
    answer_result["top_k"] = 5

    from pathlib import Path
    import json

    query_artifact_dir = Path("artifacts/query_context")
    query_artifact_dir.mkdir(parents=True, exist_ok=True)

    safe_query = "_".join(args.query.lower().split())[:80]
    artifact_stem = f"{safe_query}.{args.ranker}"
    query_artifact_path = query_artifact_dir / f"{artifact_stem}.query_context.json"

    answer_artifact_dir = Path("artifacts/query_answer")
    answer_artifact_dir.mkdir(parents=True, exist_ok=True)

    answer_artifact_path = answer_artifact_dir / f"{artifact_stem}.query_answer.json"

    with open(answer_artifact_path, "w", encoding="utf-8") as f:
        json.dump(answer_result, f, indent=2, ensure_ascii=False)

    print("QUERY ANSWER ARTIFACT:", answer_artifact_path)

    print("\nANSWER\n")
    print(answer_result["answer_text"])

    with open(query_artifact_path, "w", encoding="utf-8") as f:
        json.dump(assembled, f, indent=2, ensure_ascii=False)

    print("\nQUERY:", result["query_text"])
    print("CANDIDATE CHUNKS:", result["candidate_count"])
    print("RANKED CHUNKS:", result["ranked_count"])
    print("RETURNED CHUNKS:", result["returned_count"])
    print("CONTEXT CHUNKS USED:", assembled["used_count"])
    print("QUERY CONTEXT ARTIFACT:", query_artifact_path)
    print("RANKER:", args.ranker)
    print("MAX CHUNKS PER SOURCE:", args.max_chunks_per_source)

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


if __name__ == "__main__":
    main()