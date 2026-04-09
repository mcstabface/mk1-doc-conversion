from __future__ import annotations

from pathlib import Path

import streamlit as st

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

    if "redaction_selected_run_id" not in st.session_state:
        st.session_state["redaction_selected_run_id"] = run_id
    elif st.session_state["redaction_selected_run_id"] != run_id:
        st.session_state["redaction_selected_run_id"] = run_id
        st.session_state["redaction_plan_summary"] = None
        st.session_state["redaction_planned_artifact_id"] = None
        st.session_state["redaction_approval_summary"] = None
        st.session_state["redaction_preview_summary"] = None
        st.session_state["redaction_commit_summary"] = None
        st.session_state["redaction_selected_artifact_label"] = None

    artifacts = service.list_source_artifacts_for_run(run_id)
    if not artifacts:
        st.info("No source artifacts found for this run.")
        return

    eligible_artifacts = [
        a for a in artifacts
        if a.get("active_truth_artifact_path")
    ]

    if not eligible_artifacts:
        st.warning(
            "This run has source artifacts, but none currently resolve to an active "
            "search_context_document artifact for redaction."
        )
        st.info(
            "PII redaction operates on search_context_document truth artifacts, not on "
            "raw source files alone. This usually means the selected run was not ingested "
            "into context artifacts, or no active truth artifact is currently registered "
            "for these sources."
        )

        st.markdown("### Source artifacts in this run")
        st.json(
            [
                {
                    "artifact_id": a.get("artifact_id"),
                    "logical_path": a.get("logical_path"),
                    "source_type": a.get("source_type"),
                }
                for a in artifacts[:50]
            ]
        )

        st.markdown("### Next step")
        st.write(
            "Run deterministic ingestion in `context` mode for this corpus, then return "
            "to PII Redaction and select the resulting run."
        )
        return

    artifact_options = {
        f"{a['artifact_id']} | {a['logical_path']}": a
        for a in eligible_artifacts
    }
    artifact_labels = list(artifact_options.keys())

    default_artifact_label = artifact_labels[0]
    selected_artifact_label = st.selectbox(
        "Select source artifact",
        artifact_labels,
        index=0,
    )
    st.session_state["redaction_selected_artifact_label"] = selected_artifact_label

    selected_artifact = artifact_options[selected_artifact_label]
    selected_artifact_id = int(selected_artifact["artifact_id"])

    st.markdown("### Selected artifact")
    st.dataframe(
        [
            {
                "artifact_id": selected_artifact.get("artifact_id"),
                "logical_path": selected_artifact.get("logical_path"),
                "source_type": selected_artifact.get("source_type"),
                "active_truth_artifact_path": selected_artifact.get("active_truth_artifact_path"),
            }
        ],
        width="stretch",
    )

    profile = st.selectbox("Profile", ["business_sensitive"], index=0)
    ruleset_version = st.text_input("Ruleset version", value="business_sensitive_v1")
    ruleset_hash = st.text_input("Ruleset hash", value="business_sensitive_v1")

    if "redaction_plan_summary" not in st.session_state:
        st.session_state["redaction_plan_summary"] = None
    if "redaction_planned_artifact_id" not in st.session_state:
        st.session_state["redaction_planned_artifact_id"] = None
    if "redaction_approval_summary" not in st.session_state:
        st.session_state["redaction_approval_summary"] = None
    if "redaction_preview_summary" not in st.session_state:
        st.session_state["redaction_preview_summary"] = None
    if "redaction_commit_summary" not in st.session_state:
        st.session_state["redaction_commit_summary"] = None

    if st.button("Create plan", width="stretch"):
        plan = service.create_plan(
            RedactionPlanRequest(
                run_id=run_id,
                profile=profile,
                ruleset_version=ruleset_version,
                ruleset_hash=ruleset_hash,
                artifact_ids=[selected_artifact_id],
            )
        )
        st.session_state["redaction_plan_summary"] = plan
        st.session_state["redaction_planned_artifact_id"] = selected_artifact_id
        st.session_state["redaction_approval_summary"] = None
        st.session_state["redaction_preview_summary"] = None
        st.session_state["redaction_commit_summary"] = None

    plan = st.session_state["redaction_plan_summary"]
    if plan is not None:
        st.markdown("### Plan")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Plan ID", str(plan.plan_id))
        c2.metric("Run ID", str(plan.run_id))
        c3.metric("Status", plan.status)
        c4.metric("Suggestions", str(plan.suggestions_created))

        st.markdown("#### Category counts")
        st.dataframe(
            [
                {"category": category, "count": count}
                for category, count in plan.category_counts.items()
            ],
            width="stretch",
        )

        approval_confirm = st.checkbox(
            "I approve this redaction plan for preview/commit.",
            value=False,
        )

        if st.button("Approve plan and enable preview", width="stretch"):
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

        c1, c2, c3 = st.columns(3)
        c1.metric("Approval ID", str(approval.approval_id))
        c2.metric("Plan ID", str(approval.plan_id))
        c3.metric("Status", approval.status)

        source_artifact_id = st.session_state["redaction_planned_artifact_id"]

        if source_artifact_id is None:
            st.error("No planned artifact is bound to the current plan.")
            return

        if st.button("Generate preview", width="stretch"):
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

            if st.button("Commit redaction", width="stretch"):
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

        c1, c2, c3 = st.columns(3)
        c1.metric("Source Artifact ID", str(commit.source_artifact_id))
        c2.metric("Redacted Artifact ID", str(commit.redacted_artifact_id))
        c3.metric("Status", commit.status)

        st.write(f"Artifact path: `{commit.artifact_path}`")
        st.write(f"Artifact hash: `{commit.artifact_hash}`")

        truth_state = service.get_truth_override_state(commit.source_artifact_id)
        st.markdown("### Active truth override")
        st.json(truth_state)