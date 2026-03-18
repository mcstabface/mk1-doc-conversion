from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mk1_io.artifact_writer import write_validated_artifact


METRIC_KEYS = [
    "mean_precision_at_k",
    "mean_recall_at_k",
    "mrr",
    "query_count",
]


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _infer_dataset_name(before_artifact: dict, after_artifact: dict) -> str:
    for art in (after_artifact, before_artifact):
        env = art.get("eval_environment") or {}
        dataset = env.get("dataset")
        if isinstance(dataset, str) and dataset:
            return dataset

    # Fallback: infer from path layout: artifacts/<dataset>/query_eval/...
    # Keep deterministic: purely string parsing.
    for art_path in (after_artifact.get("_artifact_path"), before_artifact.get("_artifact_path")):
        if not art_path:
            continue
        parts = Path(art_path).parts
        try:
            idx = parts.index("artifacts")
            if idx + 1 < len(parts):
                return parts[idx + 1]
        except ValueError:
            pass

    return "unknown_dataset"


def _index_by_query_text(results: list[dict]) -> dict[str, dict]:
    indexed: dict[str, dict] = {}
    for row in results:
        qt = row.get("query_text") or row.get("query")
        if qt is None:
            continue
        qt_str = str(qt)
        # Deterministic behavior if duplicates exist: first wins.
        if qt_str not in indexed:
            indexed[qt_str] = row
    return indexed


def _as_sorted_list(values) -> list:
    if values is None:
        return []
    if isinstance(values, list):
        return sorted([str(v) for v in values])
    return sorted([str(values)])


def _float_or_none(x):
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _diff_metrics(before: dict, after: dict) -> dict:
    out = {}
    for k in METRIC_KEYS:
        out[k] = {
            "before": before.get(k),
            "after": after.get(k),
        }
        try:
            if out[k]["before"] is not None and out[k]["after"] is not None:
                out[k]["delta"] = float(out[k]["after"]) - float(out[k]["before"])
            else:
                out[k]["delta"] = None
        except Exception:
            out[k]["delta"] = None
    return out


def _compare_query_rows(query_text: str, before_row: dict | None, after_row: dict | None) -> dict:
    before_row = before_row or {}
    after_row = after_row or {}

    before_precision_at_k = _float_or_none(before_row.get("precision_at_k"))
    after_precision_at_k = _float_or_none(after_row.get("precision_at_k"))

    # Back-compat: older artifacts may have precision_at_k_true.
    before_precision_at_k_true = _float_or_none(
        before_row.get("precision_at_k_true", before_row.get("precision_at_k"))
    )
    after_precision_at_k_true = _float_or_none(
        after_row.get("precision_at_k_true", after_row.get("precision_at_k"))
    )

    before_recall_at_k = _float_or_none(before_row.get("recall_at_k"))
    after_recall_at_k = _float_or_none(after_row.get("recall_at_k"))

    before_rr = _float_or_none(before_row.get("reciprocal_rank"))
    after_rr = _float_or_none(after_row.get("reciprocal_rank"))

    before_returned_sources = _as_sorted_list(before_row.get("returned_sources"))
    after_returned_sources = _as_sorted_list(after_row.get("returned_sources"))

    before_matched_sources = _as_sorted_list(before_row.get("matched_sources"))
    after_matched_sources = _as_sorted_list(after_row.get("matched_sources"))

    def _delta(a, b):
        if a is None or b is None:
            return None
        return b - a

    return {
        "query_text": query_text,
        "status": {
            "before": before_row.get("status"),
            "after": after_row.get("status"),
        },
        "metrics": {
            "precision_at_k": {
                "before": before_precision_at_k,
                "after": after_precision_at_k,
                "delta": _delta(before_precision_at_k, after_precision_at_k),
            },
            "precision_at_k_true": {
                "before": before_precision_at_k_true,
                "after": after_precision_at_k_true,
                "delta": _delta(before_precision_at_k_true, after_precision_at_k_true),
            },
            "recall_at_k": {
                "before": before_recall_at_k,
                "after": after_recall_at_k,
                "delta": _delta(before_recall_at_k, after_recall_at_k),
            },
            "reciprocal_rank": {
                "before": before_rr,
                "after": after_rr,
                "delta": _delta(before_rr, after_rr),
            },
        },
        "returned_sources": {
            "before": before_returned_sources,
            "after": after_returned_sources,
            "added": sorted(list(set(after_returned_sources) - set(before_returned_sources))),
            "removed": sorted(list(set(before_returned_sources) - set(after_returned_sources))),
        },
        "matched_sources": {
            "before": before_matched_sources,
            "after": after_matched_sources,
            "added": sorted(list(set(after_matched_sources) - set(before_matched_sources))),
            "removed": sorted(list(set(before_matched_sources) - set(after_matched_sources))),
        },
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--before", required=True, help="Path to older query_eval artifact JSON")
    parser.add_argument("--after", required=True, help="Path to newer query_eval artifact JSON")
    args = parser.parse_args()

    before_path = Path(args.before).resolve()
    after_path = Path(args.after).resolve()

    before_art = _load_json(before_path)
    after_art = _load_json(after_path)

    # Keep paths for deterministic dataset inference fallback.
    before_art["_artifact_path"] = str(before_path)
    after_art["_artifact_path"] = str(after_path)

    dataset_name = _infer_dataset_name(before_art, after_art)

    before_results = list(before_art.get("results") or [])
    after_results = list(after_art.get("results") or [])

    before_by_query = _index_by_query_text(before_results)
    after_by_query = _index_by_query_text(after_results)

    all_queries = sorted(set(before_by_query.keys()) | set(after_by_query.keys()))

    per_query = []
    improved = 0
    regressed = 0
    unchanged = 0

    for q in all_queries:
        delta = _compare_query_rows(q, before_by_query.get(q), after_by_query.get(q))

        # Determine improvement using precision_at_k_true delta if available, else precision_at_k.
        p_true_delta = delta["metrics"]["precision_at_k_true"]["delta"]
        p_delta = delta["metrics"]["precision_at_k"]["delta"]

        score_delta = p_true_delta if p_true_delta is not None else p_delta

        if score_delta is None or score_delta == 0:
            unchanged += 1
            delta["change_class"] = "UNCHANGED"
        elif score_delta > 0:
            improved += 1
            delta["change_class"] = "IMPROVED"
        else:
            regressed += 1
            delta["change_class"] = "REGRESSED"

        per_query.append(delta)

    comparison = {
        "artifact_type": "query_eval_compare",
        "schema_version": "query_eval_compare_v1",
        "created_utc": int(time.time()),
        "producer_expert": "compare_query_eval_tool",
        "run_id": None,
        "status": "COMPLETE",
        "dataset": dataset_name,
        "inputs": {
            "before_path": str(before_path),
            "after_path": str(after_path),
        },
        "top_level_metrics": {
            "before": {k: before_art.get(k) for k in METRIC_KEYS},
            "after": {k: after_art.get(k) for k in METRIC_KEYS},
            "delta": _diff_metrics(before_art, after_art),
        },
        "summary": {
            "improved_query_count": improved,
            "regressed_query_count": regressed,
            "unchanged_query_count": unchanged,
            "total_compared_query_count": len(all_queries),
        },
        "per_query_deltas": per_query,
    }

    artifact_root = Path("artifacts") / dataset_name / "query_eval_compare"
    artifact_root.mkdir(parents=True, exist_ok=True)

    stem = f"compare.{before_path.stem}_to_{after_path.stem}"
    output_path = artifact_root / f"{stem}.query_eval_compare.json"

    write_validated_artifact(output_path, comparison)

    print("QUERY EVAL COMPARE WRITTEN:", output_path)


if __name__ == "__main__":
    main()
