from typing import Dict, List, Tuple
import sqlite3


def load_latest_artifact_state(db_path: str) -> Dict[str, str]:
    """
    Returns:
        {logical_path: sha256} for the most recently seen active version
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT sa.logical_path, sa.sha256
            FROM source_artifacts sa
            INNER JOIN (
                SELECT logical_path, MAX(last_seen_run_id) AS max_run_id
                FROM source_artifacts
                WHERE is_active = 1
                GROUP BY logical_path
            ) latest
                ON sa.logical_path = latest.logical_path
               AND sa.last_seen_run_id = latest.max_run_id
            WHERE sa.is_active = 1
            """
        )
        rows = cursor.fetchall()

    return {logical_path: sha256 for logical_path, sha256 in rows}


def detect_delta(
    db_path: str,
    current_artifacts: List[Dict],
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Returns:
        new_artifacts, changed_artifacts, unchanged_artifacts
    """
    previous = load_latest_artifact_state(db_path)

    new_artifacts: List[Dict] = []
    changed_artifacts: List[Dict] = []
    unchanged_artifacts: List[Dict] = []

    for artifact in current_artifacts:
        logical_path = artifact["logical_path"]
        sha256 = artifact["sha256"]

        if logical_path not in previous:
            new_artifacts.append(artifact)
            continue

        if previous[logical_path] != sha256:
            changed_artifacts.append(artifact)
            continue

        unchanged_artifacts.append(artifact)

    return new_artifacts, changed_artifacts, unchanged_artifacts