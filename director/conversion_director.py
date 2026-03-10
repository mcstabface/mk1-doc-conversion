from typing import Dict, List
import zipfile
from pathlib import Path

from experts.inventory.inventory_expert import run_inventory
from experts.containers.zip_expand_expert import expand_zip_artifacts
from experts.delta.delta_expert import detect_delta
from experts.query.convertible_query import select_convertible_artifacts
from experts.storage.storage_expert import create_run, persist_source_artifacts, finalize_run
from experts.storage.conversion_receipt_expert import persist_conversion_receipt
from experts.query.artifact_query import find_artifact_id
from experts.conversion.docx_to_pdf_expert import convert_docx_to_pdf
from experts.storage.run_manifest_expert import emit_run_manifest
from concurrent.futures import ThreadPoolExecutor, as_completed
from experts.conversion.fingerprint_expert import FingerprintExpert
from experts.conversion.conversion_registry_expert import ConversionRegistryExpert

import hashlib
import sqlite3
import time


class ConversionDirector:
    def __init__(self, db_path: str, pdf_output: Path, manifest_dir: Path):
        self.db_path = db_path
        self.pdf_output = pdf_output
        self.manifest_dir = manifest_dir

    def _expand_inventory(self, inventory: List[Dict]) -> List[Dict]:
        expanded: List[Dict] = []

        for artifact in inventory:
            expanded.append(artifact)
            if artifact["source_type"] == "zip":
                expanded.extend(expand_zip_artifacts(artifact))

        return expanded

    def _sha256_file(self, path: str, chunk_size: int = 1024 * 1024) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()

    def record_conversion_decision(
        self,
        conn,
        *,
        run_id: int,
        artifact_id: int,
        decision_type: str,
        reason: str | None = None,
        source_hash: str | None = None,
        output_pdf_path: str | None = None,
        pdf_hash: str | None = None,
        registry_run_id: int | None = None,
        created_utc: int,
    ) -> int:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO conversion_decisions (
                run_id,
                artifact_id,
                decision_type,
                reason,
                source_hash,
                output_pdf_path,
                pdf_hash,
                registry_run_id,
                created_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                artifact_id,
                decision_type,
                reason,
                source_hash,
                output_pdf_path,
                pdf_hash,
                registry_run_id,
                created_utc,
            ),
        )
        return cursor.lastrowid
    def _persist_conversion_registry_row(
        self,
        *,
        source_path: str,
        source_hash: str,
        output_pdf: str,
        run_id: int,
        ) -> None:
        allowed_suffixes = {".doc", ".docx", ".odt", ".rtf"}
        if Path(source_path).suffix.lower() not in allowed_suffixes:
            raise ValueError(f"Refusing to persist non-convertible source type: {source_path}")

        pdf_hash = self._sha256_file(output_pdf)
        created_utc = int(time.time())

        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO doc_conversion_registry (
                    source_path,
                    source_hash,
                    pdf_hash,
                    output_pdf,
                    created_utc,
                    run_id
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    source_path,
                    source_hash,
                    pdf_hash,
                    output_pdf,
                    created_utc,
                    run_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def run(self, source_root: Path) -> Dict:
        inventory = run_inventory(source_root)
        expanded = self._expand_inventory(inventory)
        fingerprint_expert = FingerprintExpert()
        registry_expert = ConversionRegistryExpert(db_path=self.db_path)
        convertible_inventory = [
            a["physical_path"]
            for a in expanded
            if "physical_path" in a
            and str(a["physical_path"]).lower().endswith((".doc", ".docx", ".odt", ".rtf"))
        ]

        fingerprint_payload = {
            "file_inventory": convertible_inventory,
        }
        fingerprint_result = fingerprint_expert.run(fingerprint_payload)

        registry_payload = {
            "file_inventory": convertible_inventory,
            "fingerprints": fingerprint_result["fingerprints"],
            "db_path": self.db_path,
        }
        registry_result = registry_expert.run(registry_payload)
        artifact_by_path = {
            a["physical_path"]: a
            for a in expanded
            if "physical_path" in a
        }

        plan_convert_paths = [
            item["source_path"]
            for item in registry_result["conversion_plan"]["convert"]
        ]

        planned_convert_artifacts = []
        for path in sorted(plan_convert_paths):
            artifact = artifact_by_path.get(path)
            if artifact is None:
                raise ValueError(f"Planned conversion path not found in expanded inventory: {path}")

            fp = fingerprint_result["fingerprints"].get(path, {})
            artifact_copy = dict(artifact)
            artifact_copy["source_hash"] = fp.get("source_hash")
            artifact_copy["size_bytes"] = fp.get("size_bytes")
            planned_convert_artifacts.append(artifact_copy)
        
        planned_skip_artifacts = []
        for item in sorted(
            registry_result["conversion_plan"]["skip"],
            key=lambda x: x["source_path"],
        ):
            path = item["source_path"]
            artifact = artifact_by_path.get(path)
            if artifact is None:
                raise ValueError(f"Planned skip path not found in expanded inventory: {path}")

            fp = fingerprint_result["fingerprints"].get(path, {})
            artifact_copy = dict(artifact)
            artifact_copy["source_hash"] = fp.get("source_hash")
            artifact_copy["size_bytes"] = fp.get("size_bytes")
            artifact_copy["skip_reason"] = item.get("reason")
            artifact_copy["output_pdf"] = item.get("output_pdf")
            artifact_copy["pdf_hash"] = item.get("pdf_hash")
            artifact_copy["registry_run_id"] = item.get("run_id")
            planned_skip_artifacts.append(artifact_copy)
        planned_convert_count = len(planned_convert_artifacts)
        planned_skip_count = len(planned_skip_artifacts)
        planned_total_count = len(convertible_inventory)

        convertible = list(planned_convert_artifacts)

        run_id = create_run(
            db_path=self.db_path,
            source_root=str(source_root),
            files_discovered=planned_total_count,
            files_eligible=planned_total_count,
            files_converted=planned_convert_count,
            files_skipped=planned_skip_count,
            files_failed=0,
            status="CONVERSION_RUN",
            notes=(
                f"convertible_total={planned_total_count} "
                f"planned_convert={planned_convert_count} "
                f"planned_skip={planned_skip_count}"
            ),
        )

        skip_decision_count = 0
        created_utc = int(time.time())

        with sqlite3.connect(self.db_path) as conn:
            for artifact in planned_skip_artifacts:
                artifact_id = find_artifact_id(
                    db_path=self.db_path,
                    logical_path=artifact["logical_path"],
                    sha256=artifact["sha256"],
                )

                if artifact_id is None:
                    raise ValueError(
                        f"Could not resolve artifact_id for planned skip: {artifact.get('physical_path')}"
                    )

                self.record_conversion_decision(
                    conn,
                    run_id=run_id,
                    artifact_id=artifact_id,
                    decision_type="SKIP",
                    reason=artifact.get("skip_reason"),
                    source_hash=artifact.get("source_hash"),
                    output_pdf_path=artifact.get("output_pdf"),
                    pdf_hash=artifact.get("pdf_hash"),
                    registry_run_id=artifact.get("registry_run_id"),
                    created_utc=created_utc,
                )
                skip_decision_count += 1

            conn.commit()
        conversions: List[Dict] = []

        def _convert_one(artifact: Dict) -> Dict:
            artifact_id = find_artifact_id(
                db_path=self.db_path,
                logical_path=artifact["logical_path"],
                sha256=artifact["sha256"],
            )

            try:
                pdf_path = convert_docx_to_pdf(artifact, self.pdf_output)

                persist_conversion_receipt(
                    db_path=self.db_path,
                    artifact_id=artifact_id,
                    run_id=run_id,
                    output_pdf_path=str(pdf_path),
                    converter_used="libreoffice-headless",
                    conversion_status="SUCCESS",
                    error_message=None,
                )

                self._persist_conversion_registry_row(
                    source_path=artifact["physical_path"],
                    source_hash=artifact["source_hash"],
                    output_pdf=str(pdf_path),
                    run_id=run_id,
                )

                return {
                    "logical_path": artifact["logical_path"],
                    "physical_path": artifact["physical_path"],
                    "source_hash": artifact["source_hash"],
                    "status": "SUCCESS",
                    "output_pdf_path": str(pdf_path),
                }

            except Exception as e:
                persist_conversion_receipt(
                    db_path=self.db_path,
                    artifact_id=artifact_id,
                    run_id=run_id,
                    output_pdf_path="",
                    converter_used="libreoffice-headless",
                    conversion_status="FAILED",
                    error_message=str(e),
                )

                return {
                    "logical_path": artifact["logical_path"],
                    "status": "FAILED",
                    "error": str(e),
                }

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(_convert_one, artifact) for artifact in convertible]

            for future in as_completed(futures):
                conversions.append(future.result())
            
            conversions.sort(key=lambda c: c["logical_path"])
        success_count = sum(1 for c in conversions if c["status"] == "SUCCESS")
        failed_count = sum(1 for c in conversions if c["status"] == "FAILED")

        finalize_run(
            db_path=self.db_path,
            run_id=run_id,
            files_converted=success_count,
            files_failed=failed_count,
            status="CONVERSION_RUN_COMPLETE",
            notes=(
                f"convertible_total={planned_total_count} "
                f"planned_convert={planned_convert_count} "
                f"planned_skip={planned_skip_count} "
                f"converted={success_count} "
                f"failed={failed_count}"
            ),
        )

        skipped = [
            {
                "logical_path": artifact["logical_path"],
                "physical_path": artifact["physical_path"],
                "source_hash": artifact["source_hash"],
                "skip_reason": artifact.get("skip_reason"),
                "output_pdf": artifact.get("output_pdf"),
                "pdf_hash": artifact.get("pdf_hash"),
                "registry_run_id": artifact.get("registry_run_id"),
            }
            for artifact in planned_skip_artifacts
        ]

        result = {
            "run_id": run_id,
            "status": "CONVERSION_RUN_COMPLETE",
            "failed_count": failed_count,
            "source_root": str(source_root),
            "pdf_output": str(self.pdf_output),
            "inventory_count": len(inventory),
            "expanded_count": len(expanded),
            "planned_total_count": planned_total_count,
            "planned_convert_count": planned_convert_count,
            "planned_skip_count": planned_skip_count,
            "convert_count": planned_convert_count,
            "skip_count": planned_skip_count,
            "converted_count": sum(1 for c in conversions if c["status"] == "SUCCESS"),
            "converted_paths": [c["logical_path"] for c in conversions if c["status"] == "SUCCESS"],
            "conversions": conversions,
            "skipped": skipped,
        }

        emit_run_manifest(self.manifest_dir, result)

        return result