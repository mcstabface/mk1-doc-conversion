from __future__ import annotations

from pathlib import Path

from experts.llm_search.embedding_chunk_expert import EmbeddingChunkExpert


def main() -> None:
    chunk_root = Path("artifacts/search_context_chunks")
    output_dir = "artifacts/embeddings"
    expert = EmbeddingChunkExpert()

    chunk_files = sorted(chunk_root.glob("*.search_context_chunks.json"))
    if not chunk_files:
        print("No chunk artifacts found.")
        return

    total_written = 0
    failed_files = []

    for chunk_file in chunk_files:
        try:
            import json

            artifact_data = json.loads(chunk_file.read_text(encoding="utf-8"))
            source_path = artifact_data.get("source", {}).get("source_path", "")

            if "/test_source_real/" not in source_path:
                print(f"{chunk_file.name} -> SKIPPED: not in active corpus")
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
            print(f"{chunk_file.name} -> {result['written_count']} embeddings")
        except Exception as exc:
            failed_files.append((chunk_file.name, str(exc)))
            print(f"{chunk_file.name} -> FAILED: {exc}")

    print(f"\nDONE: wrote {total_written} embedding artifacts.")
    print(f"FAILED FILES: {len(failed_files)}")

    for name, error in failed_files:
        print(f"- {name}: {error}")

if __name__ == "__main__":
    main()