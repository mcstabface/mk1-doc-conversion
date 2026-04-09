from __future__ import annotations

from pathlib import Path
from urllib.request import Request, urlopen
import json

import streamlit as st

from app.config import AppConfig
from app.contracts.ingestion import IngestionRunRequest
from app.services.ingestion_service import IngestionService


def _list_recent_search_context_files(artifact_root: Path, limit: int = 25) -> list[str]:
    search_context_dir = artifact_root / "search_context"
    if not search_context_dir.exists():
        return []

    files = [p for p in search_context_dir.glob("*.json") if p.is_file()]
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return [str(p.resolve()) for p in files[:limit]]


def _list_recent_chunk_files(artifact_root: Path, limit: int = 25) -> list[str]:
    chunk_dir = artifact_root / "chunks"
    if not chunk_dir.exists():
        return []

    files = [p for p in chunk_dir.glob("*.json") if p.is_file()]
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return [str(p.resolve()) for p in files[:limit]]


def _run_embedding_health_check(endpoint: str, model: str) -> dict:
    payload = {"model": model, "input": ["hello world"]}

    req = Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )

    with urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    embeddings = data.get("embeddings")
    if not isinstance(embeddings, list) or len(embeddings) != 1:
        raise ValueError("Health check expected exactly 1 embedding in 'embeddings'.")

    vector = embeddings[0]
    if not isinstance(vector, list) or not vector:
        raise ValueError("Health check returned an empty embedding vector.")

    if not all(isinstance(x, (int, float)) for x in vector):
        raise ValueError("Health check returned a non-numeric embedding vector.")

    return {
        "embedding_count": len(embeddings),
        "embedding_dim": len(vector),
    }


def render(config: AppConfig) -> None:
    st.subheader("Deterministic Ingestion")

    service = IngestionService()

    if "ingestion_last_result" not in st.session_state:
        st.session_state["ingestion_last_result"] = None
    if "ingestion_rechunk_summary" not in st.session_state:
        st.session_state["ingestion_rechunk_summary"] = None
    if "ingestion_rechunk_batch_results" not in st.session_state:
        st.session_state["ingestion_rechunk_batch_results"] = None
    if "ingestion_embedding_summary" not in st.session_state:
        st.session_state["ingestion_embedding_summary"] = None
    if "ingestion_embedding_health_check" not in st.session_state:
        st.session_state["ingestion_embedding_health_check"] = None
    if "ingestion_workflow_stage" not in st.session_state:
        st.session_state["ingestion_workflow_stage"] = "ingest"
    if "ingestion_selected_search_context_path" not in st.session_state:
        st.session_state["ingestion_selected_search_context_path"] = ""
    if "ingestion_selected_search_context_paths" not in st.session_state:
        st.session_state["ingestion_selected_search_context_paths"] = []
    if "ingestion_selected_chunk_path" not in st.session_state:
        st.session_state["ingestion_selected_chunk_path"] = ""
    if "ingestion_selected_chunk_paths" not in st.session_state:
        st.session_state["ingestion_selected_chunk_paths"] = []

    source_root = st.text_input("Source root", value=str(config.default_source_root))
    artifact_root = st.text_input("Artifact root", value=str(config.artifact_root))
    db_path = st.text_input("DB path", value=str(config.db_path))
    mode = st.selectbox("Mode", ["context", "pdf"], index=0)

    artifact_root_path = Path(artifact_root).resolve()
    db_path_path = Path(db_path).resolve()

    with st.expander("Operator glide path", expanded=False):
        st.markdown(
            """
            **Without redaction**
            1. Run ingestion in `context` mode
            2. Review the ingestion result
            3. Chunk one or more produced search-context artifacts
            4. Review the chunking result
            5. Generate embeddings
            6. Review the embedding result

            **With redaction**
            1. Run ingestion in `context` mode
            2. Review the ingestion result
            3. Continue to `PII Redaction`
            4. Create and approve a plan
            5. Preview if needed
            6. Commit redaction
            7. Rechunk active truth
            8. Generate embeddings
            """
        )

    st.markdown("### Step 1 · Ingest")

    if st.button("Run ingestion", width="stretch"):
        request = IngestionRunRequest(
            source_root=Path(source_root).resolve(),
            artifact_root=artifact_root_path,
            db_path=db_path_path,
            mode=mode,
        )

        try:
            result = service.run_ingestion(request)
            st.session_state["ingestion_last_result"] = result
            st.session_state["ingestion_rechunk_summary"] = None
            st.session_state["ingestion_rechunk_batch_results"] = None
            st.session_state["ingestion_embedding_summary"] = None
            st.session_state["ingestion_embedding_health_check"] = None
            st.session_state["ingestion_workflow_stage"] = "ingestion_result"

            recent_search_context = _list_recent_search_context_files(artifact_root_path, limit=25)
            st.session_state["ingestion_selected_search_context_path"] = (
                recent_search_context[0] if recent_search_context else ""
            )
            st.session_state["ingestion_selected_search_context_paths"] = recent_search_context[:5]
            st.session_state["ingestion_selected_chunk_path"] = ""
            st.session_state["ingestion_selected_chunk_paths"] = []

        except Exception as exc:
            st.error(f"{type(exc).__name__}: {exc}")

    result = st.session_state["ingestion_last_result"]
    if result is not None:
        summary = result.summary
        raw = result.raw_result

        st.markdown("### Step 2 · Ingestion result")
        if summary.status == "SUCCESS":
            st.success(f"Ingestion complete: run {summary.run_id}")
        else:
            st.warning(f"Ingestion finished with status: {summary.status}")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Run ID", str(summary.run_id))
        c2.metric("Status", summary.status)
        c3.metric("Mode", mode)
        c4.metric("Converted", str(summary.converted_count))

        c5, c6, c7, c8 = st.columns(4)
        c5.metric("Inventory", str(summary.inventory_count))
        c6.metric("Expanded", str(summary.expanded_count))
        c7.metric("Planned Convert", str(summary.planned_convert_count))
        c8.metric("Failed", str(summary.failed_count))

        st.write(f"Artifact output: `{summary.artifact_output}`")

        skipped = raw.get("skipped", [])
        failures = raw.get("failed", [])

        with st.expander("Skipped", expanded=False):
            if skipped:
                st.dataframe(
                    [
                        {
                            "logical_path": item.get("logical_path"),
                            "skip_reason": item.get("skip_reason"),
                            "artifact_type": item.get("artifact_type"),
                            "artifact_path": item.get("artifact_path"),
                        }
                        for item in skipped
                    ],
                    width="stretch",
                )
            else:
                st.write("No skipped items.")

        with st.expander("Failures", expanded=False):
            if failures:
                st.dataframe(
                    [
                        {
                            "logical_path": item.get("logical_path"),
                            "error": item.get("error"),
                        }
                        for item in failures
                    ],
                    width="stretch",
                )
            else:
                st.write("No failed items.")

        with st.expander("Recent runs", expanded=False):
            recent_runs = service.list_recent_runs(db_path_path, limit=10)
            if recent_runs:
                st.dataframe(recent_runs, width="stretch")
            else:
                st.write("No recent runs found.")

        if summary.status == "SUCCESS":
            st.markdown("### Next step")
            next_col_1, next_col_2 = st.columns(2)

            with next_col_1:
                if st.button("Continue to chunking", width="stretch"):
                    st.session_state["ingestion_workflow_stage"] = "chunk"

            with next_col_2:
                if st.button("Go to PII Redaction next", width="stretch"):
                    st.session_state["selected_capability_label"] = "PII Redaction"
                    st.session_state["redaction_prefill_run_id"] = summary.run_id
                    st.rerun()

    if (
        result is not None
        and result.summary.status == "SUCCESS"
        and st.session_state["ingestion_workflow_stage"] in {"chunk", "chunk_result", "embed", "embed_result"}
    ):
        st.markdown("### Step 3 · Chunk artifacts")

        recent_search_context_files = _list_recent_search_context_files(artifact_root_path, limit=25)

        select_all_recent = st.checkbox("Select all recent search-context artifacts", value=False)

        default_selected_paths = st.session_state["ingestion_selected_search_context_paths"]
        if select_all_recent:
            default_selected_paths = recent_search_context_files

        selected_recent_search_contexts = st.multiselect(
            "Recent search-context artifacts",
            recent_search_context_files,
            default=default_selected_paths,
        )

        manual_search_context_path = st.text_input(
            "Additional search-context artifact path",
            value="",
            placeholder="Optional: paste one extra search_context_document JSON path here",
        )

        effective_search_context_paths = list(selected_recent_search_contexts)
        if manual_search_context_path.strip():
            manual_path = str(Path(manual_search_context_path.strip()).resolve())
            if manual_path not in effective_search_context_paths:
                effective_search_context_paths.append(manual_path)

        st.session_state["ingestion_selected_search_context_paths"] = effective_search_context_paths
        st.session_state["ingestion_selected_search_context_path"] = (
            effective_search_context_paths[0] if effective_search_context_paths else ""
        )

        if effective_search_context_paths:
            st.caption(f"Artifacts selected for chunking: {len(effective_search_context_paths)}")
            st.dataframe(
                [{"search_context_artifact_path": p} for p in effective_search_context_paths[:25]],
                width="stretch",
            )
            if len(effective_search_context_paths) > 25:
                st.caption("Showing first 25 selected search-context artifacts.")
        else:
            st.write("No search-context artifacts selected.")

        chunk_output_dir = st.text_input(
            "Chunk artifact output directory",
            value=str(artifact_root_path / "chunks"),
        )

        if st.button("Chunk selected artifacts", width="stretch"):
            if not effective_search_context_paths:
                st.error("Select at least one search-context artifact.")
            else:
                batch_results = []

                for artifact_path_str in effective_search_context_paths:
                    try:
                        artifact_path = Path(artifact_path_str).resolve()
                        output_path = (
                            Path(chunk_output_dir).resolve()
                            / f"{artifact_path.stem}.chunks.json"
                        )

                        rechunk_summary = service.rechunk_search_context_artifact(
                            artifact_path=artifact_path,
                            output_path=output_path,
                        )

                        batch_results.append(
                            {
                                "search_context_artifact_path": artifact_path_str,
                                "status": rechunk_summary["status"],
                                "chunk_artifact_path": rechunk_summary["chunk_artifact_path"],
                                "chunk_count": rechunk_summary["chunk_count"],
                                "chunk_source_mode": rechunk_summary["chunk_source_mode"],
                                "error": "",
                            }
                        )
                    except Exception as exc:
                        batch_results.append(
                            {
                                "search_context_artifact_path": artifact_path_str,
                                "status": "FAILED",
                                "chunk_artifact_path": "",
                                "chunk_count": 0,
                                "chunk_source_mode": "",
                                "error": f"{type(exc).__name__}: {exc}",
                            }
                        )

                st.session_state["ingestion_rechunk_batch_results"] = batch_results

                successful_chunk_paths = [
                    item["chunk_artifact_path"]
                    for item in batch_results
                    if item["status"] == "COMPLETE" and item["chunk_artifact_path"]
                ]
                st.session_state["ingestion_selected_chunk_paths"] = successful_chunk_paths
                st.session_state["ingestion_selected_chunk_path"] = (
                    successful_chunk_paths[0] if successful_chunk_paths else ""
                )

                if successful_chunk_paths:
                    st.session_state["ingestion_rechunk_summary"] = {
                        "status": "COMPLETE",
                        "chunk_artifact_path": successful_chunk_paths[0],
                        "chunk_count": len(successful_chunk_paths),
                        "chunk_source_mode": "batch",
                    }
                else:
                    st.session_state["ingestion_rechunk_summary"] = None

                st.session_state["ingestion_embedding_summary"] = None
                st.session_state["ingestion_workflow_stage"] = "chunk_result"

    rechunk_batch_results = st.session_state["ingestion_rechunk_batch_results"]
    if rechunk_batch_results is not None:
        st.markdown("### Step 4 · Chunking result")

        success_count = len([r for r in rechunk_batch_results if r["status"] == "COMPLETE"])
        failure_count = len(rechunk_batch_results) - success_count

        st.success("Chunking step complete.")

        c1, c2, c3 = st.columns(3)
        c1.metric("Artifacts attempted", str(len(rechunk_batch_results)))
        c2.metric("Chunked", str(success_count))
        c3.metric("Failed", str(failure_count))

        st.dataframe(rechunk_batch_results, width="stretch")

        if success_count > 0:
            if st.button("Continue to embeddings", width="stretch"):
                st.session_state["ingestion_workflow_stage"] = "embed"

    if (
        rechunk_batch_results is not None
        and st.session_state["ingestion_workflow_stage"] in {"embed", "embed_result"}
    ):
        st.markdown("### Step 5 · Generate embeddings")

        recent_chunk_files = _list_recent_chunk_files(artifact_root_path, limit=25)

        select_all_recent_chunks = st.checkbox("Select all recent chunk files", value=False)

        default_chunk_paths = st.session_state["ingestion_selected_chunk_paths"]
        if select_all_recent_chunks:
            default_chunk_paths = recent_chunk_files

        selected_recent_chunks = st.multiselect(
            "Recent chunk files",
            recent_chunk_files,
            default=default_chunk_paths,
        )

        manual_chunk_path = st.text_input(
            "Additional chunk artifact path",
            value="",
            placeholder="Optional: paste one extra search_context_chunk_collection JSON path here",
        )

        effective_chunk_paths = list(selected_recent_chunks)
        if manual_chunk_path.strip():
            manual_path = str(Path(manual_chunk_path.strip()).resolve())
            if manual_path not in effective_chunk_paths:
                effective_chunk_paths.append(manual_path)

        st.session_state["ingestion_selected_chunk_paths"] = effective_chunk_paths
        st.session_state["ingestion_selected_chunk_path"] = (
            effective_chunk_paths[0] if effective_chunk_paths else ""
        )

        if effective_chunk_paths:
            st.caption(f"Chunk artifacts selected for embedding: {len(effective_chunk_paths)}")
            st.dataframe(
                [{"chunk_artifact_path": p} for p in effective_chunk_paths[:25]],
                width="stretch",
            )
            if len(effective_chunk_paths) > 25:
                st.caption("Showing first 25 selected chunk artifacts.")
        else:
            st.write("No chunk artifacts selected.")

        embedding_output_dir = st.text_input(
            "Embedding output directory",
            value=str(artifact_root_path / "embeddings"),
        )
        embedding_model = st.text_input(
            "Embedding model",
            value="nomic-embed-text",
        )
        embedding_endpoint = st.text_input(
            "Embedding endpoint",
            value="http://localhost:11434/api/embed",
        )
        embedding_batch_size = st.number_input(
            "Embedding batch size",
            min_value=1,
            value=64,
            step=1,
        )

        health_col_1, health_col_2 = st.columns([1, 2])
        with health_col_1:
            if st.button("Run embedding health check", width="stretch"):
                try:
                    st.session_state["ingestion_embedding_health_check"] = {
                        "status": "COMPLETE",
                        **_run_embedding_health_check(
                            endpoint=embedding_endpoint,
                            model=embedding_model,
                        ),
                    }
                except Exception as exc:
                    st.session_state["ingestion_embedding_health_check"] = {
                        "status": "FAILED",
                        "error": f"{type(exc).__name__}: {exc}",
                    }

        health_result = st.session_state["ingestion_embedding_health_check"]
        if health_result is not None:
            if health_result.get("status") == "COMPLETE":
                st.success(
                    f"Embedding health check passed. "
                    f"count={health_result['embedding_count']}, "
                    f"dim={health_result['embedding_dim']}"
                )
            else:
                st.error(f"Embedding health check failed: {health_result.get('error', '')}")

        if st.button("Generate embeddings", width="stretch"):
            if not effective_chunk_paths:
                st.error("Select at least one chunk artifact.")
            else:
                all_artifact_paths = []
                total_written = 0
                total_skipped_valid = 0
                failed = []

                for chunk_artifact_path_str in effective_chunk_paths:
                    try:
                        embedding_summary = service.generate_embeddings_for_chunk_artifact(
                            chunk_artifact_path=Path(chunk_artifact_path_str).resolve(),
                            output_dir=Path(embedding_output_dir).resolve(),
                            embedding_model=embedding_model,
                            endpoint=embedding_endpoint,
                            batch_size=int(embedding_batch_size),
                        )
                        total_written += int(embedding_summary.get("written_count", 0))
                        total_skipped_valid += int(embedding_summary.get("skipped_valid_count", 0))
                        all_artifact_paths.extend(embedding_summary.get("artifact_paths", []))
                    except Exception as exc:
                        failed.append(
                            {
                                "chunk_artifact_path": chunk_artifact_path_str,
                                "error": f"{type(exc).__name__}: {exc}",
                            }
                        )

                st.session_state["ingestion_embedding_summary"] = {
                    "status": "COMPLETE" if not failed else "PARTIAL",
                    "written_count": total_written,
                    "skipped_valid_count": total_skipped_valid,
                    "artifact_paths": all_artifact_paths,
                    "failed": failed,
                    "source_artifact_path": effective_chunk_paths[0] if effective_chunk_paths else "",
                }
                st.session_state["ingestion_workflow_stage"] = "embed_result"

    embedding_summary = st.session_state["ingestion_embedding_summary"]
    if embedding_summary is not None:
        st.markdown("### Step 6 · Embedding result")
        st.success("Embedding step complete.")

        c1, c2, c3 = st.columns(3)
        c1.metric("Embedding status", str(embedding_summary.get("status", "COMPLETE")))
        c2.metric("Written", str(embedding_summary.get("written_count", 0)))
        c3.metric("Skipped valid", str(embedding_summary.get("skipped_valid_count", 0)))

        st.write(
            f"Embedding source chunk artifact: "
            f"`{embedding_summary.get('source_artifact_path', '')}`"
        )

        failed = embedding_summary.get("failed", [])
        if failed:
            st.markdown("#### Embedding failures")
            st.dataframe(failed, width="stretch")

        artifact_paths = embedding_summary.get("artifact_paths", [])
        if artifact_paths:
            st.markdown("#### Written embedding artifacts")
            st.dataframe(
                [{"artifact_path": p} for p in artifact_paths[:25]],
                width="stretch",
            )
            if len(artifact_paths) > 25:
                st.caption("Showing first 25 embedding artifact paths.")