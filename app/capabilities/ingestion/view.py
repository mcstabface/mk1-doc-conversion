from __future__ import annotations

from pathlib import Path

import streamlit as st

from app.config import AppConfig
from app.contracts.ingestion import IngestionRunRequest
from app.services.ingestion_service import IngestionService


def render(config: AppConfig) -> None:
    st.subheader("Deterministic Ingestion")

    source_root = st.text_input("Source root", value=str(config.default_source_root))
    artifact_root = st.text_input("Artifact root", value=str(config.artifact_root))
    db_path = st.text_input("DB path", value=str(config.db_path))
    mode = st.selectbox("Mode", ["context", "pdf"], index=0)

    if "ingestion_last_result" not in st.session_state:
        st.session_state["ingestion_last_result"] = None
    if "ingestion_rechunk_summary" not in st.session_state:
        st.session_state["ingestion_rechunk_summary"] = None
    if "ingestion_embedding_summary" not in st.session_state:
        st.session_state["ingestion_embedding_summary"] = None

    with st.expander("Operator glide path", expanded=False):
        st.markdown(
            """
            **Without redaction**
            1. Run ingestion in `context` mode
            2. Rechunk a search-context document if needed
            3. Generate embeddings

            **With redaction**
            1. Run ingestion in `context` mode
            2. Go to `PII Redaction`
            3. Create and approve a plan
            4. Preview if needed
            5. Commit redaction
            6. Rechunk active truth
            7. Generate embeddings
            """
        )

    service = IngestionService()

    if st.button("Run ingestion", width="stretch"):
        request = IngestionRunRequest(
            source_root=Path(source_root).resolve(),
            artifact_root=Path(artifact_root).resolve(),
            db_path=Path(db_path).resolve(),
            mode=mode,
        )

        try:
            result = service.run_ingestion(request)
            st.session_state["ingestion_last_result"] = result
            st.session_state["ingestion_rechunk_summary"] = None
            st.session_state["ingestion_embedding_summary"] = None

        except Exception as exc:
            st.error(f"{type(exc).__name__}: {exc}")

    result = st.session_state["ingestion_last_result"]
    if result is not None:
        summary = result.summary
        raw = result.raw_result

        st.success(f"Run complete: {summary.run_id}")

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

        st.markdown("### Output")
        st.write(f"Artifact output: `{summary.artifact_output}`")

        if mode == "context":
            st.info(
                "Context-mode ingestion produces the search_context artifacts required "
                "for redaction, rechunking, and embedding generation."
            )

        skipped = raw.get("skipped", [])
        failures = raw.get("failed", [])

        st.markdown("### Skipped")
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

        st.markdown("### Failures")
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

        st.markdown("### Recent runs")
        recent_runs = service.list_recent_runs(Path(db_path).resolve(), limit=10)
        if recent_runs:
            st.dataframe(recent_runs, width="stretch")
        else:
            st.write("No recent runs found.")

    with st.expander("Post-processing: rechunk and embeddings", expanded=False):
        artifact_input_path = st.text_input(
            "Search-context artifact path",
            value="",
            placeholder="Paste a search_context_document JSON path here",
        )

        chunk_output_default = (
            Path(artifact_root).resolve() / "chunks" / "manual_rechunk_output.chunks.json"
        )
        chunk_output_path = st.text_input(
            "Chunk artifact output path",
            value=str(chunk_output_default),
        )

        if st.button("Rechunk artifact", width="stretch"):
            try:
                st.session_state["ingestion_rechunk_summary"] = service.rechunk_search_context_artifact(
                    artifact_path=Path(artifact_input_path).resolve(),
                    output_path=Path(chunk_output_path).resolve(),
                )
                st.session_state["ingestion_embedding_summary"] = None
            except Exception as exc:
                st.error(f"{type(exc).__name__}: {exc}")

        rechunk_summary = st.session_state["ingestion_rechunk_summary"]
        if rechunk_summary is not None:
            c1, c2, c3 = st.columns(3)
            c1.metric("Chunk status", str(rechunk_summary["status"]))
            c2.metric("Chunk count", str(rechunk_summary["chunk_count"]))
            c3.metric("Source mode", str(rechunk_summary["chunk_source_mode"]))

            st.write(f"Chunk artifact path: `{rechunk_summary['chunk_artifact_path']}`")

            embedding_output_dir = st.text_input(
                "Embedding output directory",
                value=str(Path(artifact_root).resolve() / "embeddings"),
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
                try:
                    st.session_state["ingestion_embedding_summary"] = service.generate_embeddings_for_chunk_artifact(
                        chunk_artifact_path=Path(rechunk_summary["chunk_artifact_path"]).resolve(),
                        output_dir=Path(embedding_output_dir).resolve(),
                        embedding_model=embedding_model,
                        endpoint=embedding_endpoint,
                        batch_size=int(embedding_batch_size),
                    )
                except Exception as exc:
                    st.error(f"{type(exc).__name__}: {exc}")

        embedding_summary = st.session_state["ingestion_embedding_summary"]
        if embedding_summary is not None:
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