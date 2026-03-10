from pathlib import Path
from typing import Dict
import json


def emit_run_manifest(manifest_dir: Path, result: Dict) -> Path:
    manifest_dir.mkdir(parents=True, exist_ok=True)

    run_id = result["run_id"]
    manifest_path = manifest_dir / f"run_{run_id}.json"

    manifest_path.write_text(
        json.dumps(result, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    return manifest_path