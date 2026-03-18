from pathlib import Path
import json, re
from collections import defaultdict

TOKEN_RE = re.compile(r"[a-zA-Z0-9_]+")
MIN_LEN = 3

chunk_root = Path("artifacts/enron_full_v2/search_context_chunks")
out_path = Path("artifacts/enron_full_v2/lexical_index.json")

index: dict[str, list[list]] = defaultdict(list)
doc_count = 0
chunk_count = 0

for chunk_file in sorted(chunk_root.glob("*.search_context_chunks.json")):
    artifact = json.loads(chunk_file.read_text(encoding="utf-8"))
    source = artifact.get("source", {})
    logical_path = source.get("logical_path")
    for chunk in artifact.get("chunks", []):
        text = (chunk.get("content", {}).get("text") or chunk.get("text") or "").lower()
        terms = {
            t for t in TOKEN_RE.findall(text)
            if len(t) >= MIN_LEN
        }
        ref = [logical_path, chunk.get("chunk_index"), chunk.get("chunk_id")]
        for term in terms:
            index[term].append(ref)
        chunk_count += 1
    doc_count += 1

payload = {
    "artifact_type": "lexical_inverted_index",
    "schema_version": "lexical_inverted_index_v1",
    "doc_count": doc_count,
    "chunk_count": chunk_count,
    "term_count": len(index),
    "index": dict(index),
}

out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
print(f"wrote: {out_path}")
print(f"docs: {doc_count}")
print(f"chunks: {chunk_count}")
print(f"terms: {len(index)}")