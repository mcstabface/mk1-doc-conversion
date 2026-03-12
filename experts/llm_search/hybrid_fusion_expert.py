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

        lexical_keys = {
            (item.get("logical_path"), item.get("chunk_index"))
            for item in lexical_results
        }

        for item in lexical_results:
            fused_item = dict(item)
            key = (item.get("logical_path"), item.get("chunk_index"))

            vector_match = vector_lookup.get(key)
            vector_bonus = 0.0

            if vector_match is not None:
                vector_bonus = vector_bonus_weight * float(vector_match.get("score", 0.0))
                fused_item["seen_in_vector"] = True
            else:
                fused_item["seen_in_vector"] = False

            fused_item["seen_in_lexical"] = True
            fused_item["fusion_score"] = float(item.get("score", 0.0)) + vector_bonus
            fused_results.append(fused_item)

        for item in vector_results:
            key = (item.get("logical_path"), item.get("chunk_index"))
            if key in lexical_keys:
                continue

            if float(item.get("score", 0.0)) >= vector_only_score_floor:
                vector_only_item = dict(item)
                vector_only_item["seen_in_lexical"] = False
                vector_only_item["seen_in_vector"] = True
                vector_only_item["fusion_score"] = 0.01 * float(item.get("score", 0.0))
                fused_results.append(vector_only_item)

        fused_results.sort(key=lambda x: x["fusion_score"], reverse=True)
        fused_results = fused_results[:top_k]

        return {
            "artifact_type": "hybrid_fusion_result",
            "result_count": len(fused_results),
            "results": fused_results,
        }