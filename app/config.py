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
    ...