from __future__ import annotations

import streamlit as st
from pathlib import Path

from app.config import AppConfig
from app.contracts.ingestion import IngestionRunRequest
from app.services.ingestion_service import IngestionService


def render(config: AppConfig) -> None:
    st.subheader("Deterministic Ingestion")

    source_root = st.text_input("Source root", value=str(config.default_source_root))
    artifact_root = st.text_input("Artifact root", value=str(config.artifact_root))
    db_path = st.text_input("DB path", value=str(config.db_path))
    mode = st.selectbox("Mode", ["context", "pdf"], index=0)

    service = IngestionService()

    if st.button("Run ingestion", use_container_width=True):
        request = IngestionRunRequest(
            source_root=Path(source_root).resolve(),
            artifact_root=Path(artifact_root).resolve(),
            db_path=Path(db_path).resolve(),
            mode=mode,
        )

        try:
            result = service.run_ingestion(request)
            summary = result.summary

            st.success(f"Run complete: {summary.run_id}")
            st.write(summary)

            raw = result.raw_result
            st.markdown("### Skipped")
            st.json(raw.get("skipped", []))

            st.markdown("### Failures")
            st.json(raw.get("failed", []))

            st.markdown("### Recent runs")
            st.json(service.list_recent_runs(Path(db_path).resolve(), limit=10))

        except Exception as exc:
            st.error(f"{type(exc).__name__}: {exc}")