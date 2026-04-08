from __future__ import annotations

import sqlite3
from typing import Any


class RunRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def list_recent_runs(self, limit: int = 10) -> list[dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT
                    run_id,
                    source_root,
                    status,
                    files_discovered,
                    files_converted,
                    files_skipped,
                    files_failed,
                    notes,
                    started_utc,
                    finished_utc
                FROM runs
                ORDER BY run_id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]