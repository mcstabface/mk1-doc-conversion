from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AppConfig:
    app_title: str
    artifact_root: Path
    db_path: Path
    default_source_root: Path
    enabled_capabilities: tuple[str, ...]


def load_app_config() -> AppConfig:
    repo_root = Path(__file__).resolve().parent.parent
    artifact_root = (repo_root / "artifacts").resolve()
    db_path = (artifact_root / "db" / "conversion_memory.db").resolve()
    default_source_root = (repo_root / "test_source").resolve()

    return AppConfig(
        app_title="MK1 Operator Console",
        artifact_root=artifact_root,
        db_path=db_path,
        default_source_root=default_source_root,
        enabled_capabilities=("ingestion", "redaction", "audit_history"),
    )