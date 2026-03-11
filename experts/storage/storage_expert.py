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
                ON CONFLICT(logical_path, sha256) DO UPDATE SET
                    physical_path = excluded.physical_path,
                    container_path = excluded.container_path,
                    source_type = excluded.source_type,
                    size_bytes = excluded.size_bytes,
                    modified_utc = excluded.modified_utc,
                    last_seen_run_id = excluded.last_seen_run_id,
                    is_active = excluded.is_active
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

def finalize_run(
    db_path: str,
    run_id: int,
    files_converted: int,
    files_failed: int,
    status: str,
    notes: str,
) -> None:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE runs
            SET finished_utc = ?,
                files_converted = ?,
                files_failed = ?,
                status = ?,
                notes = ?
            WHERE run_id = ?
            """,
            (
                int(time.time()),
                files_converted,
                files_failed,
                status,
                notes,
                run_id,
            ),
        )
        conn.commit()