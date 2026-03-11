import json, re, subprocess, tempfile
from pathlib import Path
from experts.base_expert import BaseExpert

class DocToSearchContextExpert(BaseExpert):
    def run(self, payload):
        src = Path(payload["physical_path"]).resolve()
        logical = payload["logical_path"]
        source_hash = payload["source_hash"]
        run_id = payload["run_id"]
        target_chars = int(payload.get("target_chars", 2000))
        overlap_chars = int(payload.get("overlap_chars", 200))
        out_dir = Path(payload.get("artifact_dir", "artifacts/search_context"))
        out_dir.mkdir(parents=True, exist_ok=True)

        text = self._extract_text(src)
        text = self._normalize(text)
        chunks = self._chunk(text, logical, source_hash, target_chars, overlap_chars)

        name = logical.replace("/", "__").replace("\\", "__")
        artifact_path = out_dir / f"{name}__{source_hash[:12]}.json"
        artifact = {
            "artifact_type": "search_context_document",
            "schema_version": "v1",
            "run_id": run_id,
            "source": {
                "source_path": str(src),
                "logical_path": logical,
                "source_type": src.suffix.lstrip(".").lower(),
                "source_hash": source_hash,
                "size_bytes": src.stat().st_size,
                "modified_utc": int(src.stat().st_mtime),
            },
            "extraction": {"extractor": "libreoffice_text", "extractor_version": "v1", "status": "SUCCESS"},
            "chunking": {"strategy": "fixed_chars_with_overlap", "target_chars": target_chars, "overlap_chars": overlap_chars},
            "chunks": chunks,
        }
        artifact_path.write_text(json.dumps(artifact, indent=2, ensure_ascii=False), encoding="utf-8")
        return {"artifact_path": str(artifact_path), "chunk_count": len(chunks), "artifact_type": "search_context_document"}

    def _extract_text(self, src):
        with tempfile.TemporaryDirectory() as td:
            subprocess.run(["soffice", "--headless", "--convert-to", "txt:Text", "--outdir", td, str(src)],
                           check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            txt = Path(td) / f"{src.stem}.txt"
            return txt.read_text(encoding="utf-8", errors="replace")

    def _normalize(self, text):
        return re.sub(r"\n{3,}", "\n\n", re.sub(r"[ \t]+", " ", text.replace("\r\n", "\n").replace("\r", "\n"))).strip()

    def _chunk(self, text, logical, source_hash, target, overlap):
        if not text: return []
        step, out, i = max(1, target - overlap), [], 0
        for start in range(0, len(text), step):
            end = min(len(text), start + target)
            chunk = text[start:end]
            out.append({"chunk_id": f"{logical}::{source_hash}::{i:04d}", "chunk_index": i,
                        "chunk_count": 0, "content": {"text": chunk, "char_count": len(chunk),
                        "token_estimate": max(1, len(chunk) // 4)}, "position": {"start_char": start, "end_char": end}})
            if end >= len(text): break
            i += 1
        for n in out: n["chunk_count"] = len(out)
        return out