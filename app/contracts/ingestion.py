from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class IngestionRunRequest:
    source_root: Path
    artifact_root: Path
    db_path: Path
    mode: str  # "pdf" | "context"


@dataclass(frozen=True)
class RunSummary:
    run_id: int
    status: str
    source_root: str
    inventory_count: int
    expanded_count: int
    planned_total_count: int
    planned_convert_count: int
    planned_skip_count: int
    converted_count: int
    failed_count: int
    artifact_output: str


@dataclass(frozen=True)
class IngestionRunResult:
    summary: RunSummary
    raw_result: dict