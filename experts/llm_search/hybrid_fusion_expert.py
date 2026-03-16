from __future__ import annotations


class HybridFusionExpert:
    def run(self, payload: dict) -> dict:
        lexical_results = payload.get("lexical_results", [])
        vector_results = payload.get("vector_results", [])
        top_k = int(payload.get("top_k", 5))

        # Normalized-score fusion experiment (deterministic).
        # Keep weights simple and visible.
        lexical_weight = float(payload.get("lexical_weight", 0.70))
        vector_weight = float(payload.get("vector_weight", 0.30))

        # Back-compat: existing hybrid callers supply vector_bonus_weight.
        # We continue to accept it but do not use it as the primary fusion path.
        vector_bonus_weight = float(payload.get("vector_bonus_weight", 0.10))
        vector_only_score_floor = float(payload.get("vector_only_score_floor", 0.60))

        lexical_scores = [float(item.get("score", 0.0)) for item in lexical_results]
        vector_scores = [float(item.get("score", 0.0)) for item in vector_results]

        lex_min = min(lexical_scores) if lexical_scores else 0.0
        lex_max = max(lexical_scores) if lexical_scores else 0.0
        vec_min = min(vector_scores) if vector_scores else 0.0
        vec_max = max(vector_scores) if vector_scores else 0.0

        lex_range = lex_max - lex_min
        vec_range = vec_max - vec_min

        def _norm(score: float, min_v: float, range_v: float) -> float:
            if range_v <= 0.0:
                return 0.0
            return (score - min_v) / range_v

        vector_lookup = {
            (item.get("logical_path"), item.get("chunk_index")): item
            for item in vector_results
        }

        lexical_rank_lookup = {
            (item.get("logical_path"), item.get("chunk_index")): idx
            for idx, item in enumerate(lexical_results, start=1)
        }

        vector_rank_lookup = {
            (item.get("logical_path"), item.get("chunk_index")): idx
            for idx, item in enumerate(vector_results, start=1)
        }

        fused_results = []

        doc_counts = {}

        lexical_keys = {
            (item.get("logical_path"), item.get("chunk_index"))
            for item in lexical_results
        }

        for item in lexical_results:
            fused_item = dict(item)
            fused_item["lexical_score"] = float(item.get("score", 0.0))
            fused_item["lexical_score_normalized"] = _norm(fused_item["lexical_score"], lex_min, lex_range)
            key = (item.get("logical_path"), item.get("chunk_index"))

            fused_item["present_in_lexical"] = True
            fused_item["present_in_vector"] = False
            fused_item["lexical_rank_before_fusion"] = lexical_rank_lookup.get(key)
            fused_item["vector_rank_before_fusion"] = vector_rank_lookup.get(key)

            vector_match = vector_lookup.get(key)
            vector_score = 0.0

            if vector_match is not None:
                vector_score = float(vector_match.get("score", 0.0))
                fused_item["seen_in_vector"] = True
                fused_item["present_in_vector"] = True
            else:
                fused_item["seen_in_vector"] = False
                fused_item["present_in_vector"] = False

            fused_item["vector_score"] = vector_score
            fused_item["vector_score_normalized"] = _norm(vector_score, vec_min, vec_range)

            fused_item["seen_in_lexical"] = True

            if fused_item.get("present_in_vector"):
                fused_item["match_origin"] = "both"
            else:
                fused_item["match_origin"] = "lexical_only"

            fused_item["fusion_lexical_weight"] = lexical_weight
            fused_item["fusion_vector_weight"] = vector_weight
            fused_item["fusion_score_pre_doc_boost"] = (
                lexical_weight * fused_item["lexical_score_normalized"]
                + vector_weight * fused_item["vector_score_normalized"]
            )
            fused_item["fusion_doc_boost"] = 0.0
            fused_item["fusion_score"] = fused_item["fusion_score_pre_doc_boost"]
            fused_item["fusion_score_post_doc_boost"] = fused_item["fusion_score"]

            doc = fused_item.get("logical_path")
            doc_counts[doc] = doc_counts.get(doc, 0) + 1
            fused_results.append(fused_item)

        for item in vector_results:
            key = (item.get("logical_path"), item.get("chunk_index"))
            if key in lexical_keys:
                continue

            if float(item.get("score", 0.0)) >= vector_only_score_floor:
                vector_only_item = dict(item)
                vector_only_item["vector_score"] = float(item.get("score", 0.0))
                vector_only_item["present_in_lexical"] = False
                vector_only_item["present_in_vector"] = True
                vector_only_item["lexical_rank_before_fusion"] = None
                vector_only_item["vector_rank_before_fusion"] = vector_rank_lookup.get(key)
                vector_only_item["match_origin"] = "vector_only"
                vector_only_item["vector_score_normalized"] = _norm(vector_only_item["vector_score"], vec_min, vec_range)
                vector_only_item["lexical_score"] = 0.0
                vector_only_item["lexical_score_normalized"] = 0.0
                vector_only_item["seen_in_lexical"] = False
                vector_only_item["seen_in_vector"] = True

                vector_only_item["fusion_lexical_weight"] = lexical_weight
                vector_only_item["fusion_vector_weight"] = vector_weight
                vector_only_item["fusion_score_pre_doc_boost"] = (
                    lexical_weight * vector_only_item["lexical_score_normalized"]
                    + vector_weight * vector_only_item["vector_score_normalized"]
                )
                vector_only_item["fusion_doc_boost"] = 0.0
                vector_only_item["fusion_score"] = vector_only_item["fusion_score_pre_doc_boost"]
                vector_only_item["fusion_score_post_doc_boost"] = vector_only_item["fusion_score"]

                doc = vector_only_item.get("logical_path")
                doc_counts[doc] = doc_counts.get(doc, 0) + 1
                fused_results.append(vector_only_item)

        for item in fused_results:
            doc = item.get("logical_path")
            count = doc_counts.get(doc, 1)

            if count > 1:
                doc_boost = 0
                item["fusion_doc_boost"] = doc_boost
                item["fusion_score"] = float(item.get("fusion_score_pre_doc_boost", item.get("fusion_score", 0.0))) + doc_boost
                item["fusion_score_post_doc_boost"] = item["fusion_score"]

        fused_results.sort(
            key=lambda x: (
                float(x.get("fusion_score", 0.0)),
                str(x.get("logical_path") or ""),
                int(x.get("chunk_index") or 0),
            ),
            reverse=True,
        )
        fused_results = fused_results[:top_k]

        return {
            "artifact_type": "hybrid_fusion_result",
            "result_count": len(fused_results),
            "results": fused_results,
            "fusion_config": {
                "top_k": top_k,
                "lexical_weight": lexical_weight,
                "vector_weight": vector_weight,
                "vector_bonus_weight": vector_bonus_weight,
                "vector_only_score_floor": vector_only_score_floor,
                "normalization": {
                    "lexical_min": lex_min,
                    "lexical_max": lex_max,
                    "vector_min": vec_min,
                    "vector_max": vec_max,
                },
            },
            "fusion_summary": {
                "lexical_input_count": len(lexical_results),
                "vector_input_count": len(vector_results),
                "deduped_input_count": len(fused_results),
            },
        }