from __future__ import annotations

import streamlit as st

from app.config import AppConfig
from app.services.audit_history_service import AuditHistoryService


def render(config: AppConfig) -> None:
    st.subheader("Audit / History")

    service = AuditHistoryService(config.db_path)

    st.markdown("### Recent runs")
    runs = service.list_recent_runs(limit=25)
    if not runs:
        st.write("No recent runs found.")
        return

    st.dataframe(runs, width="stretch")

    run_options = {
        f"run {r['run_id']} | {r.get('status', '')} | {r.get('source_root', '')}": r
        for r in runs
    }
    selected_label = st.selectbox("Inspect run", list(run_options.keys()))
    selected_run = run_options[selected_label]
    run_id = int(selected_run["run_id"])

    st.markdown("### Source artifacts")
    artifacts = service.list_source_artifacts_for_run(run_id)
    if not artifacts:
        st.write("No source artifacts found for this run.")
        return

    st.dataframe(
        [
            {
                "artifact_id": a.get("artifact_id"),
                "logical_path": a.get("logical_path"),
                "physical_path": a.get("physical_path"),
                "source_type": a.get("source_type"),
                "size_bytes": a.get("size_bytes"),
            }
            for a in artifacts
        ],
        width="stretch",
    )

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
    truth_override = service.get_truth_override_for_source(source_artifact_id)
    if truth_override:
        st.json(truth_override)
    else:
        st.write("No active truth override for this source artifact.")