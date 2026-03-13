from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from validation.artifact_validator import validate_artifact


def write_validated_artifact(path: str | Path, artifact: Dict[str, Any]) -> None:
    validate_artifact(artifact)

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(artifact, f, indent=2, ensure_ascii=False)