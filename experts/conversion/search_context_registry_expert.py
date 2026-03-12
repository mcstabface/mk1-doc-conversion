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

                rows = conn.execute(
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
                ).fetchall()

                matching_rows = [r for r in rows if r["source_hash"] == source_hash]

                artifact_types = {r["artifact_type"] for r in matching_rows}

                has_search_context = "search_context_document" in artifact_types
                has_search_chunks = "search_context_chunks" in artifact_types

                if has_search_context and has_search_chunks:
                    context_row = next(
                        r for r in matching_rows
                        if r["artifact_type"] == "search_context_document"
                    )
                    plan["skip"].append(
                        {
                            "source_path": source_path,
                            "reason": "unchanged_already_contextualized",
                            "artifact_path": context_row["artifact_path"],
                            "artifact_hash": context_row["artifact_hash"],
                            "artifact_type": context_row["artifact_type"],
                            "run_id": context_row["run_id"],
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