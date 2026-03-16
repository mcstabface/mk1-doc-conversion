import json
import subprocess
from pathlib import Path

EVAL_FILE = "artifacts/enron_full_v2/query_eval_set.json"
CHUNK_ROOT = "artifacts/enron_full_v2/search_context_chunks"

def pattern_hits(output: str, patterns: list[str]) -> list[str]:
    lowered = output.lower()
    return [p for p in patterns if p.lower() in lowered]

def extract_top_results_section(output: str) -> str:
    lines = output.splitlines()
    capture = False
    captured = []

    for line in lines:
        if line.startswith("TOP 5 RESULTS"):
            capture = True
        if capture:
            captured.append(line)
        if line.startswith("ASSEMBLED CONTEXT"):
            break

    return "\n".join(captured)

def run_query(query):
    cmd = [
        ".venv/bin/python",
        "query_search_context.py",
        "--query",
        query,
        "--chunk-root",
        CHUNK_ROOT,
        "--ranker",
        "hybrid",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout


def main():
    with open(EVAL_FILE) as f:
        data = json.load(f)

    queries = data["queries"]

    for q in queries:
        print("=" * 80)
        print("QUERY:", q["user_query"])
        print("=" * 80)

        output = run_query(q["user_query"])
        # extract rewrite line
        for line in output.splitlines():
            if line.startswith("QUERY REWRITE:"):
                print(line)
                break

        print("EXPECTED TOP1:", q.get("top1_expected", "unknown"))
        print("EXPECTED TOP3:", q.get("top3_expected", "unknown"))
        print("EXPECTED THEME:", q.get("expected_theme", "unknown"))

        top1_expected = q.get("top1_expected", "unknown")
        top3_expected = q.get("top3_expected", "unknown")

        rewrite_used = "used_fallback': False" in output and "rewritten_query': '" in output
        rewrite_changed = False
        for line in output.splitlines():
            if line.startswith("QUERY REWRITE:"):
                rewrite_changed = "used_fallback': False" in line and "No improvement possible." not in line
                break

        print(f"VERDICT TARGET: top1={top1_expected} | top3={top3_expected} | rewrite_changed={rewrite_changed}")

        top_results_text = extract_top_results_section(output)
        good_hits = pattern_hits(top_results_text, q.get("good_result_patterns", []))
        bad_hits = pattern_hits(top_results_text, q.get("bad_result_patterns", []))

        print(f"HEURISTIC: good_hits={len(good_hits)} bad_hits={len(bad_hits)}")
        if good_hits:
            print("GOOD PATTERNS:", ", ".join(good_hits))
        if bad_hits:
            print("BAD PATTERNS:", ", ".join(bad_hits))
        print()

        # only print top results section
        capture = False
        for line in output.splitlines():
            if line.startswith("TOP 5 RESULTS"):
                capture = True
            if capture:
                print(line)
            if line.startswith("ASSEMBLED CONTEXT"):
                break


if __name__ == "__main__":
    main()