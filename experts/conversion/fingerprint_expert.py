import hashlib
import os
from typing import Dict, Any

from experts.base_expert import BaseExpert


class FingerprintExpert(BaseExpert):
    """
    Deterministic source fingerprinting for conversion planning.

    Input:
        payload["file_inventory"] or payload["file_manifest"]

    Output:
        {
            "fingerprints": {
                path: {
                    "source_hash": str,
                    "size_bytes": int
                }
            }
        }
    """

    def __init__(self, chunk_size: int = 1024 * 1024):
        self.chunk_size = chunk_size

    def _sha256_file(self, path: str) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while True:
                chunk = f.read(self.chunk_size)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()

    def run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        file_inventory = payload.get("file_inventory") or payload.get("file_manifest") or []
        fingerprints: Dict[str, Dict[str, Any]] = {}

        for path in sorted(file_inventory):
            if not os.path.isfile(path):
                continue

            fingerprints[path] = {
                "source_hash": self._sha256_file(path),
                "size_bytes": os.path.getsize(path),
            }

        return {"fingerprints": fingerprints}