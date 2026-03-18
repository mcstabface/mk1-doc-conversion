from __future__ import annotations

from pathlib import Path
from email import policy
from email.parser import BytesParser
from datetime import datetime, timezone

from mk1_io.artifact_writer import write_validated_artifact


class EmailToSearchContextExpert:

    def run(self, payload: dict):

        physical_path = payload["physical_path"]
        logical_path = payload["logical_path"]
        artifact_dir = Path(payload["artifact_dir"])
        source_hash = payload["source_hash"]
        run_id = payload["run_id"]

        with open(physical_path, "rb") as f:
            msg = BytesParser(policy=policy.default).parse(f)

        subject = msg.get("subject", "")
        sender = msg.get("from", "")
        to = msg.get("to", "")
        date = msg.get("date", "")

        body = ""

        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    try:
                        body += part.get_content()
                    except Exception:
                        pass
        else:
            try:
                body = msg.get_content()
            except Exception:
                pass

        combined_text = f"""
Subject: {subject}
From: {sender}
To: {to}
Date: {date}

{body}
""".strip()

        artifact = {
            "artifact_type": "search_context_document",
            "schema_version": "search_context_document_v1",
            "producer_expert": "EmailToSearchContextExpert",
            "run_id": run_id,
            "logical_path": logical_path,
            "source_path": physical_path,
            "source_hash": source_hash,
            "document_hash": source_hash,
            "created_utc": int(datetime.now(timezone.utc).timestamp()),
            "status": "COMPLETE",
            "text_content": combined_text,
            "source": {
                "source_path": physical_path,
                "logical_path": logical_path,
                "source_hash": source_hash,
            },
            "chunks": [
                {
                    "text": combined_text,
                    "position": {
                        "start_char": 0,
                        "end_char": len(combined_text),
                    },
                }
            ],
        }

        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = artifact_dir / f"{source_hash}.search_context_document.json"

        write_validated_artifact(artifact_path, artifact)

        return {
            "artifact_path": str(artifact_path),
            "artifact_type": "search_context_document",
            "logical_path": logical_path,
            "run_id": run_id,
            "chunk_count": len(artifact["chunks"]),
        }