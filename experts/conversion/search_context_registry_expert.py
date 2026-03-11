import sqlite3


class SearchContextRegistryExpert:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def run(self, payload: dict) -> dict:
        file_inventory = payload["file_inventory"]
        fingerprints = payload["fingerprints"]

        plan = {
            "convert": [],
            "skip": [],
        }

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            for source_path in file_inventory:
                fp = fingerprints.get(source_path, {})
                source_hash = fp.get("source_hash")

                row = conn.execute(
                    """
                    SELECT
                        source_path,
                        source_hash,
                        artifact_hash,
                        artifact_path,
                        artifact_type,
                        run_id
                    FROM search_context_registry
                    WHERE source_path = ?
                    """,
                    (source_path,),
                ).fetchone()

                if row is not None and row["source_hash"] == source_hash:
                    plan["skip"].append(
                        {
                            "source_path": source_path,
                            "reason": "unchanged_already_contextualized",
                            "artifact_path": row["artifact_path"],
                            "artifact_hash": row["artifact_hash"],
                            "artifact_type": row["artifact_type"],
                            "run_id": row["run_id"],
                        }
                    )
                else:
                    plan["convert"].append(
                        {
                            "source_path": source_path,
                            "reason": "new_or_changed_source",
                        }
                    )
        finally:
            conn.close()

        return {
            "conversion_plan": plan,
        }