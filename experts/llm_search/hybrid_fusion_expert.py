from __future__ import annotations


class HybridFusionExpert:
    def run(self, payload: dict) -> dict:
        lexical_results = payload.get("lexical_results", [])
        vector_results = payload.get("vector_results", [])
        top_k = int(payload.get("top_k", 5))
        vector_bonus_weight = float(payload.get("vector_bonus_weight", 0.10))
        vector_only_score_floor = float(payload.get("vector_only_score_floor", 0.60))

        vector_lookup = {
            (item.get("logical_path"), item.get("chunk_index")): item
            for item in vector_results
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
            key = (item.get("logical_path"), item.get("chunk_index"))

            vector_match = vector_lookup.get(key)
            vector_bonus = 0.0
            vector_score = 0.0

            if vector_match is not None:
                vector_score = float(vector_match.get("score", 0.0))
                vector_bonus = vector_bonus_weight * vector_score
                fused_item["seen_in_vector"] = True
            else:
                fused_item["seen_in_vector"] = False

            fused_item["vector_score"] = vector_score

            fused_item["seen_in_lexical"] = True
            fused_item["fusion_bonus"] = vector_bonus
            fused_item["fusion_score_pre_doc_boost"] = float(item.get("score", 0.0)) + vector_bonus
            fused_item["fusion_doc_boost"] = 0.0
            fused_item["fusion_score"] = fused_item["fusion_score_pre_doc_boost"]

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
                vector_only_item["lexical_score"] = 0.0
                vector_only_item["seen_in_lexical"] = False
                vector_only_item["seen_in_vector"] = True

                # Keep vector-only items deterministic but clearly marked as vector-only.
                vector_only_item["fusion_bonus"] = 0.0
                vector_only_item["fusion_score_pre_doc_boost"] = 0.01 * float(item.get("score", 0.0))
                vector_only_item["fusion_doc_boost"] = 0.0
                vector_only_item["fusion_score"] = vector_only_item["fusion_score_pre_doc_boost"]

                doc = vector_only_item.get("logical_path")
                doc_counts[doc] = doc_counts.get(doc, 0) + 1
                fused_results.append(vector_only_item)

        for item in fused_results:
            doc = item.get("logical_path")
            count = doc_counts.get(doc, 1)

            if count > 1:
                doc_boost = 0.20 * (count - 1)
                item["fusion_doc_boost"] = doc_boost
                item["fusion_score"] = float(item.get("fusion_score_pre_doc_boost", item.get("fusion_score", 0.0))) + doc_boost

        fused_results.sort(key=lambda x: x["fusion_score"], reverse=True)
        fused_results = fused_results[:top_k]

        return {
            "artifact_type": "hybrid_fusion_result",
            "result_count": len(fused_results),
            "results": fused_results,
            "fusion_config": {
                "top_k": top_k,
                "vector_bonus_weight": vector_bonus_weight,
                "vector_only_score_floor": vector_only_score_floor,
            },
            "fusion_summary": {
                "lexical_input_count": len(lexical_results),
                "vector_input_count": len(vector_results),
                "deduped_input_count": len(fused_results),
            },
        }