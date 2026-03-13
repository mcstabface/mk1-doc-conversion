from __future__ import annotations

from pathlib import Path

import argparse
import json

from experts.llm_search.embedding_chunk_expert import EmbeddingChunkExpert


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--chunk-root",
        default="artifacts/test_source_mid/search_context_chunks",
        help="Directory containing search_context_chunks artifacts",
    )
    parser.add_argument(
        "--output-root",
        default="artifacts/test_source_mid/embeddings",
        help="Directory to write embedding artifacts",
    )
    parser.add_argument(
        "--source-contains",
        default=None,
        help="Optional substring filter for source_path, e.g. test_source_mid",
    )
    args = parser.parse_args()

    chunk_root = Path(args.chunk_root)
    output_dir = args.output_root
    source_contains = args.source_contains
    expert = EmbeddingChunkExpert()

    chunk_files = sorted(chunk_root.glob("*.search_context_chunks.json"))
    if not chunk_files:
        print("No chunk artifacts found.")
        return

    total_written = 0
    failed_files = []

    for chunk_file in chunk_files:
        try:
            artifact_data = json.loads(chunk_file.read_text(encoding="utf-8"))
            source_path = artifact_data.get("source", {}).get("source_path", "")

            if source_contains and source_contains not in source_path:
                print(f"{chunk_file.name} -> SKIPPED: source filter mismatch")
                continue

            result = expert.run(
                {
                    "chunk_artifact_path": str(chunk_file),
                    "output_dir": output_dir,
                    "embedding_model": "nomic-embed-text",
                    "endpoint": "http://localhost:11434/api/embeddings",
                }
            )
            total_written += result["written_count"]
            written = result["written_count"]
            skipped_valid = result.get("skipped_valid_count", 0)

            if written == 0 and skipped_valid > 0:
                print(f"{chunk_file.name} -> SKIPPED: {skipped_valid} embeddings already valid")
            elif written > 0 and skipped_valid > 0:
                print(f"{chunk_file.name} -> {written} embeddings written, {skipped_valid} skipped valid")
            elif written > 0:
                print(f"{chunk_file.name} -> {written} embeddings written")
            else:
                print(f"{chunk_file.name} -> SKIPPED: no embeddable chunks")
        except Exception as exc:
            failed_files.append((chunk_file.name, str(exc)))
            print(f"{chunk_file.name} -> FAILED: {exc}")

    print(f"\nDONE: wrote {total_written} embedding artifacts.")
    print(f"FAILED FILES: {len(failed_files)}")

    for name, error in failed_files:
        print(f"- {name}: {error}")

if __name__ == "__main__":
    main()