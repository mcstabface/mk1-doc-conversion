from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from director.conversion_director import ConversionDirector
from experts.conversion.docx_to_pdf_expert import ensure_libreoffice_available
from experts.llm_search.search_context_chunk_expert import SearchContextChunkExpert
from experts.llm_search.embedding_chunk_expert import EmbeddingChunkExpert
from mk1_io.artifact_writer import write_validated_artifact

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

        self._ensure_db(request.db_path)

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

    def rechunk_search_context_artifact(
        self,
        *,
        artifact_path: Path,
        output_path: Path,
    ) -> dict:
        artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
        result = SearchContextChunkExpert().run(
            {"search_context_document": artifact}
        )["search_context_chunks"]

        output_path.parent.mkdir(parents=True, exist_ok=True)
        write_validated_artifact(output_path, result)

        return {
            "status": "COMPLETE",
            "source_artifact_path": str(artifact_path),
            "chunk_artifact_path": str(output_path),
            "chunk_count": len(result["chunks"]),
            "chunk_source_mode": result.get("chunking", {}).get("chunk_source_mode"),
            "redaction_present": bool(result.get("redaction")),
        }

    def generate_embeddings_for_chunk_artifact(
        self,
        *,
        chunk_artifact_path: Path,
        output_dir: Path,
        embedding_model: str = "nomic-embed-text",
        endpoint: str = "http://localhost:11434/api/embeddings",
        batch_size: int = 64,
    ) -> dict:
        output_dir.mkdir(parents=True, exist_ok=True)

        return EmbeddingChunkExpert().run(
            {
                "chunk_artifact_path": str(chunk_artifact_path),
                "output_dir": str(output_dir),
                "embedding_model": embedding_model,
                "endpoint": endpoint,
                "batch_size": batch_size,
            }
        )