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
                    "for the PII Redaction flow."
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

        except Exception as exc:
            st.error(f"{type(exc).__name__}: {exc}")