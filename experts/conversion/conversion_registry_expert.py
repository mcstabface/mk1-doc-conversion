import sqlite3
from typing import Dict, Any, List

from experts.base_expert import BaseExpert


class ConversionRegistryExpert(BaseExpert):
    """
    Read-only planner for document conversion.

    Input:
        payload["file_inventory"] or payload["file_manifest"]
        payload["fingerprints"] = {
            "/abs/path/file.docx": {"source_hash": "..."},
            ...
        }
        payload["db_path"] (optional)

    Output:
        {
            "conversion_plan": {
                "convert": [...],
                "skip": [...]
            },
            "conversion_plan_counts": {
                "convert_count": int,
                "skip_count": int
            }
        }
    """

    def __init__(self, db_path: str = "artifacts/system_memory.db"):
        self.db_path = db_path

    def _ensure_registry_table(self, conn: sqlite3.Connection) -> None:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS doc_conversion_registry (
                source_path TEXT NOT NULL,
                source_hash TEXT NOT NULL,
                pdf_hash TEXT,
                output_pdf TEXT NOT NULL,
                created_utc INTEGER NOT NULL,
                run_id INTEGER,
                PRIMARY KEY (source_path, source_hash)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_doc_conversion_registry_run_id
            ON doc_conversion_registry(run_id)
        """)
        conn.commit()

    def run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        db_path = payload.get("db_path", self.db_path)
        file_inventory = payload.get("file_inventory") or payload.get("file_manifest") or []
        fingerprints = payload.get("fingerprints", {})

        convert: List[Dict[str, Any]] = []
        skip: List[Dict[str, Any]] = []

        conn = sqlite3.connect(db_path)
        try:
            self._ensure_registry_table(conn)

            for path in sorted(file_inventory):
                fp = fingerprints.get(path, {})
                source_hash = fp.get("source_hash")

                if not source_hash:
                    convert.append({
                        "source_path": path,
                        "reason": "missing_source_hash",
                    })
                    continue

                row = conn.execute(
                    """
                    SELECT output_pdf, pdf_hash, run_id
                    FROM doc_conversion_registry
                    WHERE source_path = ? AND source_hash = ?
                    LIMIT 1
                    """,
                    (path, source_hash),
                ).fetchone()

                if row:
                    output_pdf, pdf_hash, run_id = row
                    skip.append({
                        "source_path": path,
                        "source_hash": source_hash,
                        "output_pdf": output_pdf,
                        "pdf_hash": pdf_hash,
                        "run_id": run_id,
                        "reason": "unchanged_already_converted",
                    })
                else:
                    convert.append({
                        "source_path": path,
                        "source_hash": source_hash,
                        "reason": "new_or_changed",
                    })

        finally:
            conn.close()

        return {
            "conversion_plan": {
                "convert": convert,
                "skip": skip,
            },
            "conversion_plan_counts": {
                "convert_count": len(convert),
                "skip_count": len(skip),
            },
        }