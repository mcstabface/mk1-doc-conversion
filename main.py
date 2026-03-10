from pathlib import Path
import argparse
import sqlite3

from director.conversion_director import ConversionDirector
from experts.query.run_query import fetch_recent_runs

ROOT = Path(__file__).resolve().parent
DEFAULT_DB_PATH = ROOT / "artifacts" / "db" / "conversion_memory.db"
DEFAULT_SCHEMA_PATH = ROOT / "artifacts" / "db" / "schema.sql"
DEFAULT_PDF_OUTPUT = ROOT / "artifacts" / "pdfs"
DEFAULT_SOURCE_ROOT = ROOT / "test_source"
DEFAULT_MANIFEST_DIR = ROOT / "artifacts" / "manifests"


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
    return parser.parse_args()


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
    pdf_output = Path(args.pdf_output).resolve()
    schema_path = DEFAULT_SCHEMA_PATH

    ensure_db(db_path, schema_path)

    director = ConversionDirector(
        db_path=str(db_path),
        pdf_output=pdf_output,
        manifest_dir=DEFAULT_MANIFEST_DIR,
    )

    result = director.run(source_root)

    print("\nRUN RESULT")
    print(f"run_id: {result['run_id']}")
    print(f"source_root: {source_root}")
    print(f"pdf_output: {pdf_output}")
    print(f"inventory_count: {result['inventory_count']}")
    print(f"expanded_count: {result['expanded_count']}")
    print(f"convert_count: {result['convert_count']}")
    print(f"skip_count: {result['skip_count']}")
    print(f"planned_convert_count: {result.get('planned_convert_count', 0)}")

    print("\nCONVERSIONS")
    if result["conversions"]:
        for conversion in result["conversions"]:
            if conversion["status"] == "SUCCESS":
                print(f"SUCCESS | {conversion['logical_path']} | {conversion['output_pdf_path']}")
            else:
                print(f"FAILED | {conversion['logical_path']} | {conversion['error']}")
    else:
        print("No conversions executed.")

    print("\nRECENT RUNS")
    for row in fetch_recent_runs(str(db_path), limit=args.recent_runs):
        print(row)


if __name__ == "__main__":
    main()