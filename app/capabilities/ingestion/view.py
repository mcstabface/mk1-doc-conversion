from __future__ import annotations

from pathlib import Path

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


def render(config: AppConfig) -> None:
    st.subheader("Deterministic Ingestion")

    service = IngestionService()

    if "ingestion_last_result" not in st.session_state:
        st.session_state["ingestion_last_result"] = None
    if "ingestion_rechunk_summary" not in st.session_state:
        st.session_state["ingestion_rechunk_summary"] = None
    if "ingestion_embedding_summary" not in st.session_state:
        st.session_state["ingestion_embedding_summary"] = None
    if "ingestion_workflow_stage" not in st.session_state:
        st.session_state["ingestion_workflow_stage"] = "ingest"
    if "ingestion_selected_search_context_path" not in st.session_state:
        st.session_state["ingestion_selected_search_context_path"] = ""
    if "ingestion_selected_chunk_path" not in st.session_state:
        st.session_state["ingestion_selected_chunk_path"] = ""

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
            3. Chunk the produced search-context artifact
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
            st.session_state["ingestion_embedding_summary"] = None
            st.session_state["ingestion_workflow_stage"] = "ingestion_result"

            recent_search_context = _list_recent_search_context_files(artifact_root_path, limit=1)
            st.session_state["ingestion_selected_search_context_path"] = (
                recent_search_context[0] if recent_search_context else ""
            )
            st.session_state["ingestion_selected_chunk_path"] = ""

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
                    st.info(
                        "Switch to the PII Redaction tab in the left navigation to continue "
                        "with plan, preview, and commit."
                    )

    if (
        result is not None
        and result.summary.status == "SUCCESS"
        and st.session_state["ingestion_workflow_stage"] in {"chunk", "chunk_result", "embed", "embed_result"}
    ):
        st.markdown("### Step 3 · Chunk artifacts")

        recent_search_context_files = _list_recent_search_context_files(artifact_root_path, limit=25)
        search_context_options = [""] + recent_search_context_files

        current_search_context_path = st.session_state["ingestion_selected_search_context_path"]
        search_context_index = 0
        if current_search_context_path in search_context_options:
            search_context_index = search_context_options.index(current_search_context_path)

        selected_recent_search_context = st.selectbox(
            "Recent search-context artifacts",
            search_context_options,
            index=search_context_index,
            format_func=lambda x: "Select a recent search-context artifact..." if x == "" else x,
        )

        search_context_artifact_path = st.text_input(
            "Search-context artifact path",
            value=current_search_context_path,
            placeholder="Paste a search_context_document JSON path here",
        )

        if selected_recent_search_context:
            search_context_artifact_path = selected_recent_search_context

        st.session_state["ingestion_selected_search_context_path"] = search_context_artifact_path

        chunk_output_default = (
            artifact_root_path / "chunks" / "manual_rechunk_output.chunks.json"
        )
        chunk_output_path = st.text_input(
            "Chunk artifact output path",
            value=str(chunk_output_default),
        )

        if st.button("Chunk artifact", width="stretch"):
            if not search_context_artifact_path.strip():
                st.error("Search-context artifact path is required.")
            else:
                try:
                    rechunk_summary = service.rechunk_search_context_artifact(
                        artifact_path=Path(search_context_artifact_path).resolve(),
                        output_path=Path(chunk_output_path).resolve(),
                    )
                    st.session_state["ingestion_rechunk_summary"] = rechunk_summary
                    st.session_state["ingestion_embedding_summary"] = None
                    st.session_state["ingestion_selected_chunk_path"] = rechunk_summary["chunk_artifact_path"]
                    st.session_state["ingestion_workflow_stage"] = "chunk_result"
                except Exception as exc:
                    st.error(f"{type(exc).__name__}: {exc}")

    rechunk_summary = st.session_state["ingestion_rechunk_summary"]
    if rechunk_summary is not None:
        st.markdown("### Step 4 · Chunking result")
        st.success("Chunking complete.")

        c1, c2, c3 = st.columns(3)
        c1.metric("Chunk status", str(rechunk_summary["status"]))
        c2.metric("Chunk count", str(rechunk_summary["chunk_count"]))
        c3.metric("Source mode", str(rechunk_summary["chunk_source_mode"]))

        st.write(f"Chunk artifact path: `{rechunk_summary['chunk_artifact_path']}`")

        if st.button("Continue to embeddings", width="stretch"):
            st.session_state["ingestion_workflow_stage"] = "embed"

    if (
        rechunk_summary is not None
        and st.session_state["ingestion_workflow_stage"] in {"embed", "embed_result"}
    ):
        st.markdown("### Step 5 · Generate embeddings")

        recent_chunk_files = _list_recent_chunk_files(artifact_root_path, limit=25)
        chunk_options = [""] + recent_chunk_files

        current_chunk_path = st.session_state["ingestion_selected_chunk_path"]
        chunk_index = 0
        if current_chunk_path in chunk_options:
            chunk_index = chunk_options.index(current_chunk_path)

        selected_recent_chunk = st.selectbox(
            "Recent chunk files",
            chunk_options,
            index=chunk_index,
            format_func=lambda x: "Select a recent chunk file..." if x == "" else x,
        )

        embedding_chunk_artifact_path = st.text_input(
            "Chunk artifact path",
            value=current_chunk_path,
            placeholder="Paste a search_context_chunk_collection JSON path here",
        )

        if selected_recent_chunk:
            embedding_chunk_artifact_path = selected_recent_chunk

        st.session_state["ingestion_selected_chunk_path"] = embedding_chunk_artifact_path

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
            value="http://localhost:11434/api/embeddings",
        )
        embedding_batch_size = st.number_input(
            "Embedding batch size",
            min_value=1,
            value=64,
            step=1,
        )

        if st.button("Generate embeddings", width="stretch"):
            effective_chunk_artifact_path = embedding_chunk_artifact_path.strip()

            if not effective_chunk_artifact_path:
                st.error("Chunk artifact path is required.")
            else:
                try:
                    embedding_summary = service.generate_embeddings_for_chunk_artifact(
                        chunk_artifact_path=Path(effective_chunk_artifact_path).resolve(),
                        output_dir=Path(embedding_output_dir).resolve(),
                        embedding_model=embedding_model,
                        endpoint=embedding_endpoint,
                        batch_size=int(embedding_batch_size),
                    )
                    st.session_state["ingestion_embedding_summary"] = embedding_summary
                    st.session_state["ingestion_workflow_stage"] = "embed_result"
                except Exception as exc:
                    st.error(f"{type(exc).__name__}: {exc}")

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

        artifact_paths = embedding_summary.get("artifact_paths", [])
        if artifact_paths:
            st.markdown("#### Written embedding artifacts")
            st.dataframe(
                [{"artifact_path": p} for p in artifact_paths[:25]],
                width="stretch",
            )
            if len(artifact_paths) > 25:
                st.caption("Showing first 25 embedding artifact paths.")