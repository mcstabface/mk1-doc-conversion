import json
from typing import Optional
import re
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from experts.base_expert import BaseExpert
from mk1_io.artifact_writer import write_validated_artifact


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

        now_utc = int(datetime.now(timezone.utc).timestamp())

        artifact = {
            "artifact_type": "search_context_document",
            "schema_version": "search_context_document_v1",
            "created_utc": now_utc,
            "producer_expert": "DocToSearchContextExpert",
            "run_id": run_id,
            "status": "COMPLETE",
            "source_path": str(src),
            "logical_path": logical,
            "document_hash": source_hash,
            "text_content": text,
            "source": {
                "source_path": str(src),
                "logical_path": logical,
                "source_hash": source_hash,
                "source_type": src.suffix.lstrip(".").lower(),
                "size_bytes": src.stat().st_size,
                "modified_utc": int(src.stat().st_mtime),
            },
            "metadata": {
                "extraction": {
                    "extractor": self._extractor_name(src),
                    "extractor_version": "v1",
                    "status": "SUCCESS",
                },
                "chunking": {
                    "strategy": "fixed_chars_with_overlap",
                    "target_chars": target_chars,
                    "overlap_chars": overlap_chars,
                },
                "chunk_count": len(chunks),
            },
            "chunks": chunks,
        }

        write_validated_artifact(artifact_path, artifact)

        return {
            "artifact_path": str(artifact_path),
            "chunk_count": len(chunks),
            "artifact_type": "search_context_document",
        }

    def _extract_text(self, src):
        suffix = src.suffix.lower()

        if suffix == ".pdf":
            text = self._extract_pdf_text(src)
            if text is None:
                raise RuntimeError(f"PDF text extraction failed: {src}")
            return text

        return self._extract_via_libreoffice(src)

    def _extractor_name(self, src):
        if src.suffix.lower() == ".pdf":
            return "pdf_text"
        return "libreoffice_text"

    def _extract_via_libreoffice(self, src):
        with tempfile.TemporaryDirectory() as td:
            subprocess.run(
                ["soffice", "--headless", "--convert-to", "txt:Text", "--outdir", td, str(src)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            txt = Path(td) / f"{src.stem}.txt"
            return txt.read_text(encoding="utf-8", errors="replace")

    def _extract_pdf_text(self, src) -> Optional[str]:
        # 1) PyMuPDF
        try:
            import fitz  # type: ignore

            doc = fitz.open(str(src))
            try:
                parts = []
                for page in doc:
                    parts.append(page.get_text("text") or "")
                return "\n".join(parts)
            finally:
                doc.close()
        except Exception:
            pass

        # 2) pypdf
        try:
            from pypdf import PdfReader  # type: ignore

            reader = PdfReader(str(src))
            parts = []
            for page in reader.pages:
                parts.append(page.extract_text() or "")
            return "\n".join(parts)
        except Exception:
            return None

    def _normalize(self, text):
        return re.sub(
            r"\n{3,}",
            "\n\n",
            re.sub(r"[ \t]+", " ", text.replace("\r\n", "\n").replace("\r", "\n")),
        ).strip()

    def _chunk(self, text, logical, source_hash, target, overlap):
        if not text:
            return []

        step = max(1, target - overlap)
        out = []
        i = 0

        for start in range(0, len(text), step):
            end = min(len(text), start + target)
            chunk = text[start:end]
            out.append(
                {
                    "chunk_id": f"{logical}::{source_hash}::{i:04d}",
                    "chunk_index": i,
                    "chunk_count": 0,
                    "content": {
                        "text": chunk,
                        "char_count": len(chunk),
                        "token_estimate": max(1, len(chunk) // 4),
                    },
                    "position": {"start_char": start, "end_char": end},
                }
            )
            if end >= len(text):
                break
            i += 1

        for n in out:
            n["chunk_count"] = len(out)

        return out