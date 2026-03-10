from typing import Dict, List
import sqlite3
import time


def create_run(
    db_path: str,
    source_root: str,
    files_discovered: int,
    files_eligible: int,
    files_converted: int = 0,
    files_skipped: int = 0,
    files_failed: int = 0,
    status: str = "DISCOVERED",
    notes: str = "",
) -> int:
    now_utc = int(time.time())

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO runs (
                started_utc,
                finished_utc,
                source_root,
                status,
                files_discovered,
                files_eligible,
                files_converted,
                files_skipped,
                files_failed,
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now_utc,
                now_utc,
                source_root,
                status,
                files_discovered,
                files_eligible,
                files_converted,
                files_skipped,
                files_failed,
                notes,
            ),
        )
        conn.commit()
        return cursor.lastrowid


def persist_source_artifacts(
    db_path: str,
    run_id: int,
    artifacts: List[Dict],
) -> None:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        for artifact in artifacts:
            cursor.execute(
                """
                INSERT INTO source_artifacts (
                    physical_path,
                    container_path,
                    logical_path,
                    source_type,
                    size_bytes,
                    modified_utc,
                    sha256,
                    first_seen_run_id,
                    last_seen_run_id,
                    is_active
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    artifact["physical_path"],
                    artifact["container_path"],
                    artifact["logical_path"],
                    artifact["source_type"],
                    artifact["size_bytes"],
                    artifact["modified_utc"],
                    artifact["sha256"],
                    run_id,
                    run_id,
                    1,
                ),
            )

        conn.commit()