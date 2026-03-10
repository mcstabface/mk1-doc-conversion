from pathlib import Path
import sqlite3

from director.conversion_director import ConversionDirector
from experts.query.run_query import fetch_recent_runs

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


def main() -> None:
    ensure_db()

    director = ConversionDirector(
        db_path=str(DB_PATH),
        pdf_output=PDF_OUTPUT,
    )

    source_root = ROOT / "test_source"
    result = director.run(source_root)

    print("\nRUN RESULT")
    print(f"run_id: {result['run_id']}")
    print(f"inventory_count: {result['inventory_count']}")
    print(f"expanded_count: {result['expanded_count']}")
    print(f"new_count: {result['new_count']}")
    print(f"changed_count: {result['changed_count']}")
    print(f"unchanged_count: {result['unchanged_count']}")
    print(f"convertible_count: {result['convertible_count']}")

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
    for row in fetch_recent_runs(str(DB_PATH), limit=10):
        print(row)


if __name__ == "__main__":
    main()