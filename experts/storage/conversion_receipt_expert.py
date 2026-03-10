from typing import Optional
import sqlite3
import time


def persist_conversion_receipt(
    db_path: str,
    artifact_id: int,
    run_id: int,
    output_pdf_path: str,
    converter_used: str,
    conversion_status: str,
    error_message: Optional[str] = None,
) -> None:
    created_utc = int(time.time())

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO conversions (
                artifact_id,
                run_id,
                output_pdf_path,
                converter_used,
                conversion_status,
                error_message,
                created_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                artifact_id,
                run_id,
                output_pdf_path,
                converter_used,
                conversion_status,
                error_message,
                created_utc,
            ),
        )
        conn.commit()