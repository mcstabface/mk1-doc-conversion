from pathlib import Path
import argparse
import sqlite3
import sys

from pathlib import Path
from director.conversion_director import ConversionDirector
from experts.query.run_query import fetch_recent_runs
from experts.conversion.docx_to_pdf_expert import ensure_libreoffice_available


def get_runtime_root() -> Path:
    """
    Return the directory where bundled read-only assets live.

    When running from a PyInstaller one-file executable,
    assets are extracted to sys._MEIPASS.
    """

    if getattr(sys, "frozen", False):
        if hasattr(sys, "_MEIPASS"):
            return Path(sys._MEIPASS)

        # fallback (older behavior)
        return Path(sys.executable).parent

    return Path(__file__).resolve().parent

runtime_root = get_runtime_root()

bundle_root = get_runtime_root()

if getattr(sys, "frozen", False):
    work_root = Path(sys.executable).parent
else:
    work_root = bundle_root

DEFAULT_SCHEMA_PATH = bundle_root / "artifacts" / "db" / "schema.sql"
DEFAULT_DB_PATH = work_root / "artifacts" / "db" / "conversion_memory.db"
DEFAULT_PDF_OUTPUT = work_root / "artifacts" / "pdfs"
DEFAULT_MANIFEST_DIR = work_root / "artifacts" / "manifests"
DEFAULT_SOURCE_ROOT = work_root / "test_source"




def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deterministic delta-aware document conversion pipeline."
    )
    parser.add_argument(
        "--source",
        dest="source_root",
        default=str(DEFAULT_SOURCE_ROOT),
        help="Source directory to scan recursively.",
    )
    parser.add_argument(
        "--pdf-output",
        dest="pdf_output",
        default=str(DEFAULT_PDF_OUTPUT),
        help="Directory where converted PDFs will be written.",
    )
    parser.add_argument(
        "--db-path",
        dest="db_path",
        default=str(DEFAULT_DB_PATH),
        help="SQLite database path for pipeline state.",
    )
    parser.add_argument(
        "--recent-runs",
        dest="recent_runs",
        type=int,
        default=10,
        help="Number of recent runs to display.",
    )
    parser.add_argument(
        "--mode",
        choices=["pdf", "context"],
        default="pdf",
        help="Conversion mode: pdf (default) or context",
    )
    parser.add_argument(
        "--artifact-root",
        default="./artifacts",
        help="Root directory for generated artifacts",
    )

    return parser.parse_args()

    mode = args.mode

def ensure_db(db_path: Path, schema_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        schema_sql = schema_path.read_text(encoding="utf-8")
        conn.executescript(schema_sql)
        conn.commit()


def main() -> None:
    args = parse_args()

    db_path = Path(args.db_path).resolve()
    source_root = Path(args.source_root).resolve()
    artifact_root = Path(args.artifact_root).resolve()
    schema_path = DEFAULT_SCHEMA_PATH
    mode = args.mode

    ensure_libreoffice_available()

    if mode == "context":
        pdf_output = artifact_root / "pdfs"
    else:
        pdf_output = Path(args.pdf_output).resolve()

    pdf_output.mkdir(parents=True, exist_ok=True)

    manifest_dir = artifact_root / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)

    ensure_db(db_path, schema_path)

    director = ConversionDirector(
        db_path=db_path,
        pdf_output=pdf_output,
        manifest_dir=manifest_dir,
        mode=mode,
    )

    result = director.run(source_root)

    print("\nRUN RESULT")
    print(f"run_id: {result['run_id']}")
    print(f"status: {result['status']}")
    print(f"source_root: {source_root}")
    print(f"artifact_output: {result.get('artifact_output', pdf_output)}")
    print(f"inventory_count: {result['inventory_count']}")
    print(f"expanded_count: {result['expanded_count']}")
    print(f"planned_total_count: {result.get('planned_total_count', 0)}")
    print(f"planned_convert_count: {result.get('planned_convert_count', 0)}")
    print(f"planned_skip_count: {result.get('planned_skip_count', 0)}")
    print(f"converted_count: {result.get('converted_count', 0)}")
    print(f"failed_count: {result.get('failed_count', 0)}")

    print("\nCONVERSIONS")
    if result["conversions"]:
        for conversion in result["conversions"]:
            if conversion["status"] == "SUCCESS":
                print(
                    f"SUCCESS | {conversion['logical_path']} | "
                    f"{conversion.get('artifact_path', conversion.get('output_pdf_path', ''))}"
                )
            else:
                print(f"FAILED | {conversion['logical_path']} | {conversion['error']}")
    else:
        print("No conversions executed.")

    print("\nSKIPPED")
    skipped = result.get("skipped", [])
    if skipped:
        for item in skipped:
            print(
                f"SKIP | {item['logical_path']} | "
                f"reason={item.get('skip_reason', 'unknown')} | "
                f"prior_artifact={item.get('artifact_path', '')} | "
                f"artifact_type={item.get('artifact_type', '')}"
            )
    else:
        print("No skipped items.")

    print("\nRECENT RUNS (AUDIT)")
    for row in fetch_recent_runs(str(db_path), limit=args.recent_runs):
        print(row)


if __name__ == "__main__":
    main()