from pathlib import Path
from typing import Dict, List
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

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

    def run(self, source_root: Path) -> Dict:
        inventory = run_inventory(source_root)
        expanded = self._expand_inventory(inventory)

        new_artifacts, changed_artifacts, unchanged_artifacts = detect_delta(
            db_path=self.db_path,
            current_artifacts=expanded,
        )

        delta_artifacts = new_artifacts + changed_artifacts
        convertible = select_convertible_artifacts(delta_artifacts)

        run_id = create_run(
            db_path=self.db_path,
            source_root=str(source_root),
            files_discovered=len(expanded),
            files_eligible=len(expanded),
            files_converted=len(convertible),
            files_skipped=len(unchanged_artifacts),
            files_failed=0,
            status="CONVERSION_RUN",
            notes=f"new={len(new_artifacts)} changed={len(changed_artifacts)} unchanged={len(unchanged_artifacts)}",
        )

        if delta_artifacts:
            persist_source_artifacts(
                db_path=self.db_path,
                run_id=run_id,
                artifacts=delta_artifacts,
            )

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

                return {
                    "logical_path": artifact["logical_path"],
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
        success_count = sum(1 for c in conversions if c["status"] == "SUCCESS")
        failed_count = sum(1 for c in conversions if c["status"] == "FAILED")

        finalize_run(
            db_path=self.db_path,
            run_id=run_id,
            files_converted=success_count,
            files_failed=failed_count,
            status="CONVERSION_RUN_COMPLETE",
            notes=(
                f"new={len(new_artifacts)} "
                f"changed={len(changed_artifacts)} "
                f"unchanged={len(unchanged_artifacts)} "
                f"converted={success_count} "
                f"failed={failed_count}"
            ),
        )

        result = {
            "run_id": run_id,
            "status": "CONVERSION_RUN_COMPLETE",
            "failed_count": failed_count,
            "source_root": str(source_root),
            "pdf_output": str(self.pdf_output),
            "inventory_count": len(inventory),
            "expanded_count": len(expanded),
            "new_count": len(new_artifacts),
            "changed_count": len(changed_artifacts),
            "unchanged_count": len(unchanged_artifacts),
            "convertible_count": len(convertible),
            "converted_count": sum(1 for c in conversions if c["status"] == "SUCCESS"),
            "converted_paths": [c["logical_path"] for c in conversions if c["status"] == "SUCCESS"],
            "conversions": conversions,
        }

        emit_run_manifest(self.manifest_dir, result)

        return result