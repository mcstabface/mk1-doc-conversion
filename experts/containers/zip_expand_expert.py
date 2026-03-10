from pathlib import Path
from typing import Dict, List
import hashlib
import zipfile


SUPPORTED_MEMBER_EXTENSIONS = {
    ".docx",
    ".doc",
    ".rtf",
}


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def expand_zip_artifacts(zip_artifact: Dict) -> List[Dict]:
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

            member_bytes = zf.read(info.filename)

            results.append(
                {
                    "physical_path": str(zip_path),
                    "container_path": str(zip_path),
                    "logical_path": f'{zip_artifact["logical_path"]}::{info.filename}',
                    "source_type": ext.lstrip("."),
                    "size_bytes": len(member_bytes),
                    "modified_utc": None,
                    "sha256": sha256_bytes(member_bytes),
                }
            )

    return results