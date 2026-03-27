from typing import Dict, List
import zipfile
from pathlib import Path
import json

from mk1_io.artifact_writer import write_validated_artifact

from experts.inventory.inventory_expert import run_inventory
from experts.containers.zip_expand_expert import expand_zip_artifacts
from experts.delta.delta_expert import detect_delta
from experts.query.convertible_query import select_convertible_artifacts
from experts.storage.storage_expert import create_run, persist_source_artifacts, finalize_run
from experts.storage.conversion_receipt_expert import persist_conversion_receipt
from experts.query.artifact_query import find_artifact_id
from experts.conversion.doc_to_search_context_expert import DocToSearchContextExpert
from experts.conversion.docx_to_pdf_expert import convert_docx_to_pdf
from experts.storage.run_manifest_expert import emit_run_manifest
from concurrent.futures import ThreadPoolExecutor, as_completed
from experts.conversion.fingerprint_expert import FingerprintExpert
from experts.conversion.search_context_registry_expert import SearchContextRegistryExpert
from experts.conversion.conversion_registry_expert import ConversionRegistryExpert
from experts.conversion.search_context_registry_expert import SearchContextRegistryExpert
from experts.llm_search.search_context_chunk_expert import SearchContextChunkExpert
from experts.conversion.email_to_search_context_expert import EmailToSearchContextExpert


import hashlib
import sqlite3
import time

def _is_maildir_email(path_str: str) -> bool:
    from pathlib import Path

    path = Path(path_str)

    parent_names = {p.name.lower() for p in path.parents}

    maildir_markers = {
        "maildir",
        "inbox",
        "sent",
        "sent_items",
        "_sent_mail",
        "deleted_items",
        "discussion_threads",
        "all_documents",
    }

    return bool(parent_names & maildir_markers) and path.suffix in {"", "."}

class ConversionDirector:
    def __init__(self, db_path: str, pdf_output: Path, manifest_dir: Path, mode: str = "pdf"):
        self.db_path = db_path
        self.pdf_output = pdf_output
        self.manifest_dir = manifest_dir
        self.mode = mode

    def _expand_inventory(self, inventory: List[Dict]) -> tuple[List[Dict], List[Dict]]:
        expanded: List[Dict] = []
        expansion_failures: List[Dict] = []

        for artifact in inventory:
            expanded.append(artifact)
            if artifact["source_type"] == "zip":
                extract_root = self.pdf_output.parent / "staging"
                members = expand_zip_artifacts(artifact, extract_root=extract_root)

                for m in members:
                    if m.get("expansion_status") == "FAILED":
                        expansion_failures.append({
                            "logical_path": m.get("logical_path"),
                            "reason": m.get("error_message"),
                            "container_path": m.get("container_path"),
                        })
                        print(
                            f"[ZIP-EXPAND:ERROR] Skipping corrupt member: "
                            f"{m['logical_path']} reason={m.get('error_message')}"
                        )
                        continue

                    expanded.append(m)

        return expanded, expansion_failures

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

    def _persist_search_context_registry_row(
        self,
        *,
        source_path: str,
        source_hash: str,
        artifact_path: str,
        artifact_type: str,
        run_id: int,
    ) -> None:
        artifact_hash = self._sha256_file(artifact_path)
        created_utc = int(time.time())

        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO search_context_registry (
                    source_path,
                    source_hash,
                    artifact_hash,
                    artifact_path,
                    artifact_type,
                    created_utc,
                    run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_path,
                    source_hash,
                    artifact_hash,
                    artifact_path,
                    artifact_type,
                    created_utc,
                    run_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def run(self, source_root: Path) -> Dict:
        inventory = run_inventory(source_root)
        expanded, expansion_failures = self._expand_inventory(inventory)

        fingerprint_expert = FingerprintExpert()
        email_expert = EmailToSearchContextExpert()

        if self.mode == "context":
            registry_expert = SearchContextRegistryExpert(db_path=self.db_path)
        elif self.mode == "pdf":
            registry_expert = ConversionRegistryExpert(db_path=self.db_path)
        else:
            raise ValueError(f"Unsupported mode: {self.mode}")

        if self.mode == "context":
            allowed_ext = (".doc", ".docx", ".odt", ".rtf", ".pdf")
        else:
            allowed_ext = (".doc", ".docx", ".odt", ".rtf")

        convertible_inventory = [
            a["physical_path"]
            for a in expanded
            if "physical_path" in a
            and (
                str(a["physical_path"]).lower().endswith(allowed_ext)
                or (self.mode == "context" and _is_maildir_email(a["physical_path"]))
            )
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

        search_context_expert = None
        chunk_expert = None
        if self.mode == "context":
            search_context_expert = DocToSearchContextExpert()
            chunk_expert = SearchContextChunkExpert()

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
            artifact_copy["artifact_path"] = item.get("artifact_path")
            artifact_copy["artifact_hash"] = item.get("artifact_hash")
            artifact_copy["artifact_type"] = item.get("artifact_type")
            artifact_copy["registry_run_id"] = item.get("run_id")
            planned_skip_artifacts.append(artifact_copy)
        planned_convert_count = len(planned_convert_artifacts)
        planned_skip_count = len(planned_skip_artifacts)
        planned_total_count = len(convertible_inventory)

        convertible = list(planned_convert_artifacts)

        run_id = None

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
        try:
            artifacts_to_persist = []
            for artifact in planned_convert_artifacts + planned_skip_artifacts:
                artifacts_to_persist.append(
                    {
                        "physical_path": artifact["physical_path"],
                        "container_path": artifact.get("container_path"),
                        "logical_path": artifact["logical_path"],
                        "source_type": artifact["source_type"],
                        "size_bytes": artifact["size_bytes"],
                        "modified_utc": artifact.get("modified_utc"),
                        "sha256": artifact["source_hash"],
                    }
                )

            persist_source_artifacts(
                db_path=self.db_path,
                run_id=run_id,
                artifacts=artifacts_to_persist,
            )

            skip_decision_count = 0
            created_utc = int(time.time())

            with sqlite3.connect(self.db_path) as conn:
                for artifact in planned_skip_artifacts:
                    artifact_id = find_artifact_id(
                    db_path=self.db_path,
                    logical_path=artifact["logical_path"],
                    sha256=artifact["source_hash"],
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
                        output_pdf_path=artifact.get("artifact_path"),
                        pdf_hash=artifact.get("artifact_hash"),
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
                    sha256=artifact["source_hash"],
                )

                if artifact_id is None:
                    raise RuntimeError(
                        f"Artifact lookup failed for {artifact['logical_path']} hash={artifact['source_hash']}"
                    )

                try:
                    if self.mode == "context":
                        is_maildir_email = _is_maildir_email(artifact["physical_path"])

                        expert_payload = {
                            "physical_path": artifact["physical_path"],
                            "logical_path": artifact["logical_path"],
                            "source_hash": artifact["source_hash"],
                            "run_id": run_id,
                            "artifact_dir": str(self.pdf_output.parent / "search_context"),
                        }

                        if is_maildir_email:
                            expert_result = email_expert.run(expert_payload)
                            converter_used = "email-to-search-context-v1"
                        else:
                            expert_result = search_context_expert.run(expert_payload)
                            converter_used = "doc-to-search-context-v1"

                        self._persist_search_context_registry_row(
                            source_path=artifact["physical_path"],
                            source_hash=artifact["source_hash"],
                            artifact_path=str(expert_result["artifact_path"]),
                            artifact_type=expert_result.get("artifact_type", "search_context_document"),
                            run_id=run_id,
                        )

                        with open(expert_result["artifact_path"], "r", encoding="utf-8") as f:
                            chunk_input_document = json.load(f)

                        chunk_payload = {
                            "search_context_document": chunk_input_document,
                        }

                        chunk_result = chunk_expert.run(chunk_payload)
                        chunk_artifact = chunk_result["search_context_chunks"]

                        chunk_artifact_dir = self.pdf_output.parent / "search_context_chunks"
                        chunk_artifact_dir.mkdir(parents=True, exist_ok=True)

                        chunk_artifact_path = (
                            chunk_artifact_dir
                            / f"{artifact['source_hash']}.search_context_chunks.json"
                        )

                        write_validated_artifact(chunk_artifact_path, chunk_artifact)

                        self._persist_search_context_registry_row(
                            source_path=artifact["physical_path"],
                            source_hash=artifact["source_hash"],
                            artifact_path=str(chunk_artifact_path),
                            artifact_type="search_context_chunks",
                            run_id=run_id,
                        )

                        persist_conversion_receipt(
                            db_path=self.db_path,
                            artifact_id=artifact_id,
                            run_id=run_id,
                            output_pdf_path=str(expert_result["artifact_path"]),
                            converter_used=converter_used,
                            conversion_status="SUCCESS",
                            error_message=None,
                        )

                        return {
                            "logical_path": artifact["logical_path"],
                            "physical_path": artifact["physical_path"],
                            "source_hash": artifact["source_hash"],
                            "status": "SUCCESS",
                            "artifact_path": str(expert_result["artifact_path"]),
                            "artifact_type": expert_result.get("artifact_type"),
                            "chunk_count": expert_result.get("chunk_count", 0),
                            "search_chunk_count": len(
                                chunk_result["search_context_chunks"].get("chunks", [])
                            ),
                            "search_chunk_artifact_type": chunk_result["search_context_chunks"].get("artifact_type"),
                            "search_chunk_artifact_path": str(chunk_artifact_path),
                        }
                    elif self.mode == "pdf":
                        source_path = artifact["physical_path"]

                        output_pdf_path = convert_docx_to_pdf(
                            artifact=artifact,
                            output_dir=self.pdf_output,
                        )

                        persist_conversion_receipt(
                            db_path=self.db_path,
                            artifact_id=artifact_id,
                            run_id=run_id,
                            output_pdf_path=str(output_pdf_path),
                            converter_used="docx-to-pdf",
                            conversion_status="SUCCESS",
                            error_message=None,
                        )

                        return {
                            "logical_path": artifact["logical_path"],
                            "physical_path": artifact["physical_path"],
                            "source_hash": artifact["source_hash"],
                            "status": "SUCCESS",
                            "output_pdf_path": str(output_pdf_path),
                            "converter_used": "docx-to-pdf",
                        }
                except Exception as e:
                    persist_conversion_receipt(
                        db_path=self.db_path,
                        artifact_id=artifact_id,
                        run_id=run_id,
                        output_pdf_path="",
                        converter_used=(
                            "email-to-search-context-v1"
                            if (self.mode == "context" and _is_maildir_email(artifact["physical_path"]))
                            else "doc-to-search-context-v1"
                            if self.mode == "context"
                            else "docx-to-pdf"
                        ),
                        conversion_status="FAILED",
                        error_message=str(e),
                    )

                    return {
                        "logical_path": artifact["logical_path"],
                        "status": "FAILED",
                        "error": str(e),
                    }
            all_maildir_email = all(
                _is_maildir_email(artifact["physical_path"])
                for artifact in planned_convert_artifacts
            )

            if self.mode == "context" and all_maildir_email:
                future_to_path = {}

                with ThreadPoolExecutor(max_workers=4) as executor:
                    for artifact in planned_convert_artifacts:
                        future = executor.submit(_convert_one, artifact)
                        future_to_path[future] = artifact["logical_path"]

                    completed = []
                    for future in as_completed(future_to_path):
                        completed.append(future.result())

                conversions.extend(sorted(completed, key=lambda x: x["logical_path"]))
            else:
                for artifact in planned_convert_artifacts:
                    result = _convert_one(artifact)
                    conversions.append(result)

            failures = [c for c in conversions if c["status"] == "FAILED"]
            total_failures = len(failures) + len(expansion_failures)

            finalize_run(
                db_path=self.db_path,
                run_id=run_id,
                files_converted=len([c for c in conversions if c["status"] == "SUCCESS"]),
                files_failed=total_failures,
                status="SUCCESS" if total_failures == 0 else "FAILED",
                notes=(
                    f"artifact_mode=search_context "
                    f"planned_convert={planned_convert_count} "
                    f"planned_skip={planned_skip_count} "
                    f"completed_success={len([c for c in conversions if c['status'] == 'SUCCESS'])} "
                    f"completed_failed={len(failures)} "
                    f"expansion_failures={len(expansion_failures)}"
                ),
            )

            total_failures = len(failures) + len(expansion_failures)

            skipped = [
                {
                    "logical_path": artifact["logical_path"],
                    "physical_path": artifact["physical_path"],
                    "source_hash": artifact["source_hash"],
                    "skip_reason": artifact.get("skip_reason"),
                    "artifact_path": artifact.get("artifact_path"),
                    "artifact_hash": artifact.get("artifact_hash"),
                    "artifact_type": artifact.get("artifact_type"),
                    "registry_run_id": artifact.get("registry_run_id"),
                }
                for artifact in planned_skip_artifacts
            ]

            result = {
                "run_id": run_id,
                "status": "FAILED",
                "failed_count": total_failures,
                "expansion_failures_count": len(expansion_failures),
                "source_root": str(source_root),
                "artifact_output": str(self.pdf_output.parent / "search_context"),
                "inventory_count": len(inventory),
                "expanded_count": len(expanded),
                "planned_total_count": planned_total_count,
                "planned_convert_count": planned_convert_count,
                "planned_skip_count": planned_skip_count,
                "convert_count": planned_convert_count,
                "skip_count": planned_skip_count,
                "converted_count": len([c for c in conversions if c["status"] == "SUCCESS"]),
                "converted_paths": [
                    c["logical_path"] for c in conversions if c["status"] == "SUCCESS"
                ],
                "conversions": conversions,
                "failed": failures,
                "skipped": skipped,
                "expansion_failures": expansion_failures,
            }

            emit_run_manifest(self.manifest_dir, result)

            return result

        except Exception:
            finalize_run(
                db_path=self.db_path,
                run_id=run_id,
                files_converted=0,
                files_failed=1,
                status="FAILED",
                notes="artifact_mode=search_context director_exception=1",
            )
            skipped = [
                {
                    "logical_path": artifact["logical_path"],
                    "physical_path": artifact["physical_path"],
                    "source_hash": artifact["source_hash"],
                    "skip_reason": artifact.get("skip_reason"),
                    "artifact_path": artifact.get("artifact_path"),
                    "artifact_hash": artifact.get("artifact_hash"),
                    "artifact_type": artifact.get("artifact_type"),
                    "registry_run_id": artifact.get("registry_run_id"),
                }
                for artifact in planned_skip_artifacts
            ]

            result = {
                "run_id": run_id,
                "status": "CONVERSION_RUN_COMPLETE",
                "failed_count": len(failures),
                "source_root": str(source_root),
                "artifact_output": str(self.pdf_output.parent / "search_context"),
                "inventory_count": len(inventory),
                "expanded_count": len(expanded),
                "planned_total_count": planned_total_count,
                "planned_convert_count": planned_convert_count,
                "planned_skip_count": planned_skip_count,
                "convert_count": planned_convert_count,
                "skip_count": planned_skip_count,
                "converted_count": len([c for c in conversions if c["status"] == "SUCCESS"]),
                "converted_paths": [
                    c["logical_path"] for c in conversions if c["status"] == "SUCCESS"
                ],
                "conversions": conversions,
                "skipped": skipped,
            }

            emit_run_manifest(self.manifest_dir, result)


            return result

        except Exception as e:
            if run_id is not None:
                finalize_run(
                    db_path=self.db_path,
                    run_id=run_id,
                    files_converted=0,
                    files_failed=0,
                    status="CONVERSION_RUN_ABORTED",
                    notes=(
                        f"convertible_total={planned_total_count} "
                        f"planned_convert={planned_convert_count} "
                        f"planned_skip={planned_skip_count} "
                        f"aborted_error={str(e)}"
                    ),
                )
            raise