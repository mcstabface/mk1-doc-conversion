from typing import List, Tuple
import sqlite3


def fetch_recent_runs(db_path: str, limit: int = 10) -> List[Tuple]:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                run_id,
                started_utc,
                status,
                files_discovered,
                files_skipped,
                notes
            FROM runs
            ORDER BY run_id DESC
            LIMIT ?
            """,
            (limit,),
        )
        return cursor.fetchall()