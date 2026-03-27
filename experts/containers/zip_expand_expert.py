from pathlib import Path
from typing import Dict, List
import hashlib
import zipfile
import os
import re


SUPPORTED_MEMBER_EXTENSIONS = {
    ".docx",
    ".doc",
    ".rtf",
}


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def _safe_member_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)

def expand_zip_artifacts(zip_artifact: Dict, extract_root: Path) -> List[Dict]:
    zip_path = Path(zip_artifact["physical_path"])
    results: List[Dict] = []

    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue

            member_path = Path(info.filename)
            ext = member_path.suffix.lower()
            if ext not in SUPPORTED_MEMBER_EXTENSIONS:
                continue

            try:
                member_bytes = zf.read(info.filename)
            except (zipfile.BadZipFile, RuntimeError, OSError) as e:
                results.append(
                    {
                        "physical_path": str(zip_path),
                        "container_path": str(zip_path),
                        "logical_path": f'{zip_artifact["logical_path"]}::{info.filename}',
                        "source_type": ext.lstrip("."),
                        "size_bytes": 0,
                        "modified_utc": None,
                        "sha256": None,
                        "expansion_status": "FAILED",
                        "error_message": str(e),
                    }
                )
                continue

            zip_key = hashlib.sha256(str(zip_path).encode("utf-8")).hexdigest()[:12]
            member_key = hashlib.sha256(info.filename.encode("utf-8")).hexdigest()[:12]

            member_stage_dir = extract_root / zip_key
            member_stage_dir.mkdir(parents=True, exist_ok=True)

            staged_name = f"{Path(info.filename).stem}__{member_key}{ext}"
            staged_path = member_stage_dir / _safe_member_name(staged_name)

            with open(staged_path, "wb") as f:
                f.write(member_bytes)

            results.append(
                {
                    "physical_path": str(staged_path),
                    "container_path": str(zip_path),
                    "logical_path": f'{zip_artifact["logical_path"]}::{info.filename}',
                    "source_type": ext.lstrip("."),
                    "size_bytes": len(member_bytes),
                    "modified_utc": None,
                    "sha256": sha256_bytes(member_bytes),
                    "expansion_status": "SUCCESS",
                    "error_message": None,
                }
            )

    return results