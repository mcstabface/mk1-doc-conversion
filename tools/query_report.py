from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--eval-artifact", required=True, help="Path to query_eval artifact JSON")
    args = parser.parse_args()

    artifact_path = Path(args.eval_artifact)
    data = json.loads(artifact_path.read_text(encoding="utf-8"))

    print(f"EVAL ARTIFACT: {artifact_path}")
    print(f"QUERY COUNT: {data.get('query_count', 0)}")
    print(f"MEAN PRECISION@K: {data.get('mean_precision_at_k', 0.0):.2f}")
    print(f"MEAN RECALL@K: {data.get('mean_recall_at_k', 0.0):.2f}")
    print(f"MRR: {data.get('mrr', 0.0):.2f}")

    for result in data.get("results", []):
        query = result.get("query", "")
        expected = set(result.get("expected_sources", []))
        returned = result.get("returned_sources", [])

        print("\n" + "=" * 72)
        print(f"QUERY: {query}")
        print(f"precision@k={result.get('precision_at_k', 0.0):.2f}  "
              f"recall@k={result.get('recall_at_k', 0.0):.2f}  "
              f"rr={result.get('reciprocal_rank', 0.0):.2f}")
        print(f"candidate={result.get('candidate_count', 0)}  "
              f"ranked={result.get('ranked_count', 0)}  "
              f"returned={result.get('returned_count', 0)}  "
              f"context_used={result.get('context_used_count', 0)}")

        print("EXPECTED:")
        for src in sorted(expected):
            print(f"  - {src}")

        print("RETURNED:")
        if not returned:
            print("  (none)")
            continue

        for idx, src in enumerate(returned, start=1):
            marker = "✔" if src in expected else "✘"
            print(f"  {idx}. {marker} {src}")


if __name__ == "__main__":
    main()