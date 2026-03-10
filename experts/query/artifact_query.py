from typing import Optional
import sqlite3


def find_artifact_id(db_path: str, logical_path: str, sha256: str) -> Optional[int]:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT artifact_id
            FROM source_artifacts
            WHERE logical_path = ?
              AND sha256 = ?
            ORDER BY artifact_id DESC
            LIMIT 1
            """,
            (logical_path, sha256),
        )
        row = cursor.fetchone()

    return row[0] if row else None