from __future__ import annotations

from pathlib import Path

import sqlite3
from pathlib import Path

from director.conversion_director import ConversionDirector
from experts.conversion.docx_to_pdf_expert import ensure_libreoffice_available

from app.contracts.ingestion import (
    IngestionRunRequest,
    IngestionRunResult,
    RunSummary,
)
from app.repositories.run_repository import RunRepository


class IngestionService:
    def __init__(self) -> None:
        pass

    def _ensure_db(self, db_path: Path) -> None:
        repo_root = Path(__file__).resolve().parent.parent.parent
        schema_path = repo_root / "artifacts" / "db" / "schema.sql"

        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        db_path.parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(db_path) as conn:
            schema_sql = schema_path.read_text(encoding="utf-8")
            conn.executescript(schema_sql)
            conn.commit()

    def run_ingestion(self, request: IngestionRunRequest) -> IngestionRunResult:
        if request.mode not in ("pdf", "context"):
            raise ValueError(f"Unsupported mode: {request.mode}")

        if request.mode in ("pdf", "context"):
            ensure_libreoffice_available()

        pdf_output = (
            request.artifact_root / "pdfs"
            if request.mode == "context"
            else request.artifact_root / "pdfs"
        )
        manifest_dir = request.artifact_root / "manifests"

        pdf_output.mkdir(parents=True, exist_ok=True)
        manifest_dir.mkdir(parents=True, exist_ok=True)
        request.db_path.parent.mkdir(parents=True, exist_ok=True)

        director = ConversionDirector(
            db_path=str(request.db_path),
            pdf_output=pdf_output,
            manifest_dir=manifest_dir,
            mode=request.mode,
        )

        result = director.run(request.source_root)

        summary = RunSummary(
            run_id=result["run_id"],
            status=result["status"],
            source_root=str(request.source_root),
            inventory_count=result.get("inventory_count", 0),
            expanded_count=result.get("expanded_count", 0),
            planned_total_count=result.get("planned_total_count", 0),
            planned_convert_count=result.get("planned_convert_count", 0),
            planned_skip_count=result.get("planned_skip_count", 0),
            converted_count=result.get("converted_count", 0),
            failed_count=result.get("failed_count", 0),
            artifact_output=str(result.get("artifact_output", pdf_output)),
        )

        return IngestionRunResult(summary=summary, raw_result=result)

    def list_recent_runs(self, db_path: Path, limit: int = 10) -> list[dict]:
        repo = RunRepository(str(db_path))
        return repo.list_recent_runs(limit=limit)