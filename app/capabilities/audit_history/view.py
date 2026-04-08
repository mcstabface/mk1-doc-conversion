from __future__ import annotations

import streamlit as st

from app.config import AppConfig
from app.services.audit_history_service import AuditHistoryService


def render(config: AppConfig) -> None:
    st.subheader("Audit / History")

    service = AuditHistoryService(config.db_path)

    st.markdown("### Recent runs")
    runs = service.list_recent_runs(limit=25)
    st.json(runs)

    if not runs:
        return

    run_options = {
        f"run {r['run_id']} | {r.get('status', '')} | {r.get('source_root', '')}": r
        for r in runs
    }
    selected_label = st.selectbox("Inspect run", list(run_options.keys()))
    selected_run = run_options[selected_label]
    run_id = int(selected_run["run_id"])

    st.markdown("### Source artifacts")
    artifacts = service.list_source_artifacts_for_run(run_id)
    st.json(artifacts)

    if artifacts:
        artifact_options = {
            f"{a['artifact_id']} | {a['logical_path']}": a
            for a in artifacts
        }
        artifact_label = st.selectbox(
            "Inspect truth override for source artifact",
            list(artifact_options.keys()),
        )
        source_artifact_id = int(artifact_options[artifact_label]["artifact_id"])

        st.markdown("### Active truth override")
        st.json(service.get_truth_override_for_source(source_artifact_id))