from __future__ import annotations

import streamlit as st
from pathlib import Path

from app.config import AppConfig
from app.contracts.redaction import (
    RedactionPlanRequest,
    RedactionApprovalRequest,
    RedactionPreviewRequest,
    RedactionCommitRequest,
)
from app.services.redaction_service import RedactionService


def render(config: AppConfig) -> None:
    st.subheader("PII Redaction")

    service = RedactionService(config.db_path)

    runs = service.list_runs(limit=50)
    if not runs:
        st.info("No runs found.")
        return

    run_options = {
        f"run {r['run_id']} | {r['status']} | {r.get('source_root', '')}": r
        for r in runs
    }
    selected_run_label = st.selectbox("Select run", list(run_options.keys()))
    selected_run = run_options[selected_run_label]
    run_id = int(selected_run["run_id"])

    artifacts = service.list_source_artifacts_for_run(run_id)
    if not artifacts:
        st.info("No source artifacts found for this run.")
        return

    artifact_options = {
        f"{a['artifact_id']} | {a['logical_path']}": a
        for a in artifacts
    }
    selected_artifact_labels = st.multiselect(
        "Select source artifacts",
        list(artifact_options.keys()),
    )

    profile = st.selectbox("Profile", ["business_sensitive"], index=0)
    ruleset_version = st.text_input("Ruleset version", value="business_sensitive_v1")
    ruleset_hash = st.text_input("Ruleset hash", value="business_sensitive_v1")

    if "redaction_plan_summary" not in st.session_state:
        st.session_state["redaction_plan_summary"] = None
    if "redaction_approval_summary" not in st.session_state:
        st.session_state["redaction_approval_summary"] = None
    if "redaction_preview_summary" not in st.session_state:
        st.session_state["redaction_preview_summary"] = None
    if "redaction_commit_summary" not in st.session_state:
        st.session_state["redaction_commit_summary"] = None

    if st.button("Create plan", use_container_width=True):
        if not selected_artifact_labels:
            st.error("Select at least one source artifact.")
        else:
            artifact_ids = [int(artifact_options[label]["artifact_id"]) for label in selected_artifact_labels]
            plan = service.create_plan(
                RedactionPlanRequest(
                    run_id=run_id,
                    profile=profile,
                    ruleset_version=ruleset_version,
                    ruleset_hash=ruleset_hash,
                    artifact_ids=artifact_ids,
                )
            )
            st.session_state["redaction_plan_summary"] = plan
            st.session_state["redaction_approval_summary"] = None
            st.session_state["redaction_preview_summary"] = None
            st.session_state["redaction_commit_summary"] = None

    plan = st.session_state["redaction_plan_summary"]
    if plan is not None:
        st.markdown("### Plan")
        st.write(plan)

        approval_confirm = st.checkbox(
            "I approve this redaction plan for preview/commit.",
            value=False,
        )

        if st.button("Record approval", use_container_width=True):
            if not approval_confirm:
                st.error("Approval confirmation is required.")
            else:
                approval = service.record_approval(
                    RedactionApprovalRequest(
                        plan_id=plan.plan_id,
                        approval_flags={"approved_in_app": True},
                    )
                )
                st.session_state["redaction_approval_summary"] = approval
                st.session_state["redaction_preview_summary"] = None
                st.session_state["redaction_commit_summary"] = None

    approval = st.session_state["redaction_approval_summary"]
    if approval is not None:
        st.markdown("### Approval")
        st.write(approval)

        single_artifact_label = st.selectbox(
            "Preview / commit artifact",
            selected_artifact_labels if selected_artifact_labels else [],
        )

        if single_artifact_label:
            source_artifact_id = int(artifact_options[single_artifact_label]["artifact_id"])

            if st.button("Generate preview", use_container_width=True):
                preview = service.get_preview(
                    RedactionPreviewRequest(
                        source_artifact_id=source_artifact_id,
                        profile=profile,
                        ruleset_version=ruleset_version,
                        ruleset_hash=ruleset_hash,
                        plan_id=plan.plan_id,
                        approval_id=approval.approval_id,
                    )
                )
                st.session_state["redaction_preview_summary"] = preview
                st.session_state["redaction_commit_summary"] = None

            preview = st.session_state["redaction_preview_summary"]
            if preview is not None:
                st.markdown("### Preview")
                st.write(preview.status)

                preview_doc = preview.document
                redaction_meta = preview_doc.get("redaction", {})
                text_content = preview_doc.get("text_content", "")

                st.markdown("#### Redaction metadata")
                st.json(redaction_meta)

                st.markdown("#### Preview text")
                st.text_area(
                    "Preview text",
                    value=text_content[:2000],
                    height=300,
                    disabled=True,
                    label_visibility="collapsed",
                )

                default_output = service.build_default_output_path(
                    source_artifact_id=source_artifact_id,
                    profile=profile,
                    plan_id=plan.plan_id,
                )
                output_path = st.text_input(
                    "Commit artifact output path",
                    value=str(default_output),
                )

                if st.button("Commit redaction", use_container_width=True):
                    commit = service.commit(
                        RedactionCommitRequest(
                            source_artifact_id=source_artifact_id,
                            profile=profile,
                            ruleset_version=ruleset_version,
                            ruleset_hash=ruleset_hash,
                            plan_id=plan.plan_id,
                            approval_id=approval.approval_id,
                            artifact_output_path=Path(output_path).resolve(),
                        )
                    )
                    st.session_state["redaction_commit_summary"] = commit

    commit = st.session_state["redaction_commit_summary"]
    if commit is not None:
        st.markdown("### Commit")
        st.write(commit)

        truth_state = service.get_truth_override_state(commit.source_artifact_id)
        st.markdown("### Active truth override")
        st.json(truth_state)