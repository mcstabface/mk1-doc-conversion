from pathlib import Path
from typing import Dict, List
import hashlib
import time


SUPPORTED_EXTENSIONS = {
    ".docx",
    ".doc",
    ".rtf",
    ".zip",
    ".pdf",
    ".",
}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def run_inventory(source_root: Path) -> List[Dict]:
    artifacts: List[Dict] = []

    for path in source_root.rglob("*"):
        if not path.is_file():
            continue

        ext = path.suffix.lower()
        if ext not in SUPPORTED_EXTENSIONS:
            continue

        stat = path.stat()

        artifact = {
            "physical_path": str(path),
            "container_path": None,
            "logical_path": str(path.relative_to(source_root)),
            "source_type": ext.lstrip("."),
            "size_bytes": stat.st_size,
            "modified_utc": int(stat.st_mtime),
            "sha256": sha256_file(path),
        }

        artifacts.append(artifact)

    return artifacts