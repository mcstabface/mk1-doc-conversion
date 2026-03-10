from pathlib import Path
import sqlite3
import zipfile

from experts.inventory.inventory_expert import run_inventory
from experts.containers.zip_expand_expert import expand_zip_artifacts
from experts.delta.delta_expert import detect_delta
from experts.storage.storage_expert import create_run, persist_source_artifacts
from experts.storage.conversion_receipt_expert import persist_conversion_receipt
from experts.query.run_query import fetch_recent_runs
from experts.query.convertible_query import select_convertible_artifacts
from experts.query.artifact_query import find_artifact_id
from experts.conversion.docx_to_pdf_expert import convert_docx_to_pdf

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "artifacts" / "db" / "conversion_memory.db"
SCHEMA_PATH = ROOT / "artifacts" / "db" / "schema.sql"
PDF_OUTPUT = ROOT / "artifacts" / "pdfs"


def ensure_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
        conn.executescript(schema_sql)
        conn.commit()


def debug_zip_members(zip_path: Path) -> None:
    print(f"\nZIP DEBUG: {zip_path.name}")
    with zipfile.ZipFile(zip_path, "r") as zf:
        members = zf.infolist()
        print(f"Total members: {len(members)}")
        for info in members[:50]:
            kind = "dir" if info.is_dir() else "file"
            print(f"  {kind:4} | {info.filename}")


def main() -> None:
    ensure_db()

    source_root = ROOT / "test_source"
    inventory = run_inventory(source_root)

    expanded = []
    for artifact in inventory:
        expanded.append(artifact)
        if artifact["source_type"] == "zip":
            debug_zip_members(Path(artifact["physical_path"]))
            expanded.extend(expand_zip_artifacts(artifact))

    new_artifacts, changed_artifacts, unchanged_artifacts = detect_delta(
        db_path=str(DB_PATH),
        current_artifacts=expanded,
    )

    delta_artifacts = new_artifacts + changed_artifacts
    convertible = select_convertible_artifacts(delta_artifacts)

    run_id = create_run(
        db_path=str(DB_PATH),
        source_root=str(source_root),
        files_discovered=len(expanded),
        files_eligible=len(expanded),
        files_converted=len(convertible),
        files_skipped=len(unchanged_artifacts),
        files_failed=0,
        status="CONVERSION_RUN",
        notes=f"new={len(new_artifacts)} changed={len(changed_artifacts)} unchanged={len(unchanged_artifacts)}",
    )

    if delta_artifacts:
        persist_source_artifacts(
            db_path=str(DB_PATH),
            run_id=run_id,
            artifacts=delta_artifacts,
        )

    print("\nDELTA REPORT")
    print(f"run_id: {run_id}")
    print(f"new: {len(new_artifacts)}")
    print(f"changed: {len(changed_artifacts)}")
    print(f"unchanged: {len(unchanged_artifacts)}")

    print("\nCONVERTIBLE DELTA")
    print(f"eligible_for_conversion: {len(convertible)}")

    for artifact in convertible:
        artifact_id = find_artifact_id(
            db_path=str(DB_PATH),
            logical_path=artifact["logical_path"],
            sha256=artifact["sha256"],
        )

        try:
            pdf_path = convert_docx_to_pdf(artifact, PDF_OUTPUT)

            persist_conversion_receipt(
                db_path=str(DB_PATH),
                artifact_id=artifact_id,
                run_id=run_id,
                output_pdf_path=str(pdf_path),
                converter_used="libreoffice-headless",
                conversion_status="SUCCESS",
                error_message=None,
            )

            print(f"converted → {pdf_path}")

        except Exception as e:

            persist_conversion_receipt(
                db_path=str(DB_PATH),
                artifact_id=artifact_id,
                run_id=run_id,
                output_pdf_path="",
                converter_used="libreoffice-headless",
                conversion_status="FAILED",
                error_message=str(e),
            )

            print(f"conversion_failed → {artifact['logical_path']} | {e}")

    print("\nRECENT RUNS")
    for row in fetch_recent_runs(str(DB_PATH), limit=10):
        print(row)


if __name__ == "__main__":
    main()