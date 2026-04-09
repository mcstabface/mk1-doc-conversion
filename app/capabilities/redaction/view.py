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
        _format_run_label(run): run
        for run in runs
    }

    prefill_run_id = st.session_state.get("redaction_prefill_run_id")
    default_run_label = None
    if prefill_run_id is not None:
        for label, run in run_options.items():
            if int(run["run_id"]) == int(prefill_run_id):
                default_run_label = label
                break

    run_labels = list(run_options.keys())
    default_index = 0
    if default_run_label in run_labels:
        default_index = run_labels.index(default_run_label)

    selected_run_label = st.selectbox(
        "Select run",
        run_labels,
        index=default_index,
    )

    if prefill_run_id is not None:
        st.session_state["redaction_prefill_run_id"] = None
    selected_run = run_options[selected_run_label]
    run_id = int(selected_run["run_id"])

    if "redaction_selected_run_id" not in st.session_state:
        st.session_state["redaction_selected_run_id"] = run_id
    elif st.session_state["redaction_selected_run_id"] != run_id:
        st.session_state["redaction_selected_run_id"] = run_id
        st.session_state["redaction_plan_summary"] = None
        st.session_state["redaction_planned_artifact_id"] = None
        st.session_state["redaction_planned_artifact_ids"] = None
        st.session_state["redaction_approval_summary"] = None
        st.session_state["redaction_preview_summary"] = None
        st.session_state["redaction_commit_summary"] = None
        st.session_state["redaction_batch_commit_results"] = None
        st.session_state["redaction_rechunk_summary"] = None
        st.session_state["redaction_embedding_summary"] = None
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

        with st.expander("Source artifacts in this run", expanded=False):
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

    profile_options = {
        "business_sensitive": {
            "ruleset_version": "business_sensitive_v1",
            "ruleset_hash": "business_sensitive_v1",
        },
        "identity_contact": {
            "ruleset_version": "identity_contact_v1",
            "ruleset_hash": "identity_contact_v1",
        },
    }

    if "redaction_plan_summary" not in st.session_state:
        st.session_state["redaction_plan_summary"] = None
    if "redaction_planned_artifact_id" not in st.session_state:
        st.session_state["redaction_planned_artifact_id"] = None
    if "redaction_planned_artifact_ids" not in st.session_state:
        st.session_state["redaction_planned_artifact_ids"] = None
    if "redaction_approval_summary" not in st.session_state:
        st.session_state["redaction_approval_summary"] = None
    if "redaction_preview_summary" not in st.session_state:
        st.session_state["redaction_preview_summary"] = None
    if "redaction_commit_summary" not in st.session_state:
        st.session_state["redaction_commit_summary"] = None
    if "redaction_batch_commit_results" not in st.session_state:
        st.session_state["redaction_batch_commit_results"] = None
    if "redaction_rechunk_summary" not in st.session_state:
        st.session_state["redaction_rechunk_summary"] = None
    if "redaction_embedding_summary" not in st.session_state:
        st.session_state["redaction_embedding_summary"] = None

    st.caption(f"Eligible artifacts in run: {len(artifact_labels)}")

    with st.expander("Operator glide path", expanded=False):
        st.markdown(
            """
            **Without redaction**
            1. Ingest source into context artifacts
            2. Rechunk active truth
            3. Generate embeddings

            **With redaction**
            1. Ingest source into context artifacts
            2. Create redaction plan
            3. Approve plan
            4. Preview if needed
            5. Commit redaction
            6. Rechunk active truth
            7. Generate embeddings
            """
        )

    with st.container():
        st.markdown("### Plan builder")

        top_left, top_right = st.columns([2, 1])
        with top_left:
            planning_mode = st.selectbox(
                "Planning mode",
                ["single", "multi-select", "all eligible"],
                index=0,
            )
        with top_right:
            profile = st.selectbox("Profile", list(profile_options.keys()), index=0)

        ruleset_version = profile_options[profile]["ruleset_version"]
        ruleset_hash = profile_options[profile]["ruleset_hash"]

        meta_left, meta_right = st.columns(2)
        with meta_left:
            st.text_input("Ruleset version", value=ruleset_version, disabled=True)
        with meta_right:
            st.text_input("Ruleset hash", value=ruleset_hash, disabled=True)

        selected_artifact_labels: list[str] = []
        selected_artifact_id: int | None = None
        selected_artifact = None
        selected_artifact_active_truth_path: str | None = None

        if planning_mode == "single":
            selected_artifact_label = st.selectbox(
                "Select source artifact",
                artifact_labels,
                index=0,
            )
            st.session_state["redaction_selected_artifact_label"] = selected_artifact_label

            selected_artifact = artifact_options[selected_artifact_label]
            selected_artifact_id = int(selected_artifact["artifact_id"])
            selected_artifact_labels = [selected_artifact_label]
            selected_artifact_active_truth_path = selected_artifact.get("active_truth_artifact_path")

            with st.expander("Selected artifact", expanded=False):
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

        elif planning_mode == "multi-select":
            selected_artifact_labels = st.multiselect(
                "Select source artifacts",
                artifact_labels,
                default=[],
            )

            with st.expander("Selected artifacts", expanded=False):
                if selected_artifact_labels:
                    st.dataframe(
                        [
                            {
                                "artifact_id": artifact_options[label].get("artifact_id"),
                                "logical_path": artifact_options[label].get("logical_path"),
                                "source_type": artifact_options[label].get("source_type"),
                            }
                            for label in selected_artifact_labels
                        ],
                        width="stretch",
                    )
                else:
                    st.write("No artifacts selected.")

        else:
            selected_artifact_labels = artifact_labels

            with st.expander("Selected artifacts", expanded=False):
                st.write(f"All eligible artifacts selected: {len(selected_artifact_labels)}")
                st.dataframe(
                    [
                        {
                            "artifact_id": artifact_options[label].get("artifact_id"),
                            "logical_path": artifact_options[label].get("logical_path"),
                            "source_type": artifact_options[label].get("source_type"),
                        }
                        for label in selected_artifact_labels[:25]
                    ],
                    width="stretch",
                )
                if len(selected_artifact_labels) > 25:
                    st.caption("Showing first 25 selected artifacts.")

        planned_artifact_ids = [
            int(artifact_options[label]["artifact_id"])
            for label in selected_artifact_labels
        ]

        if st.button("Create plan", width="stretch"):
            if not planned_artifact_ids:
                st.error("Select at least one artifact for planning.")
            else:
                plan = service.create_plan(
                    RedactionPlanRequest(
                        run_id=run_id,
                        profile=profile,
                        ruleset_version=ruleset_version,
                        ruleset_hash=ruleset_hash,
                        artifact_ids=planned_artifact_ids,
                    )
                )
                st.session_state["redaction_plan_summary"] = plan
                st.session_state["redaction_planned_artifact_ids"] = planned_artifact_ids
                st.session_state["redaction_planned_artifact_id"] = (
                    planned_artifact_ids[0] if len(planned_artifact_ids) == 1 else None
                )
                st.session_state["redaction_approval_summary"] = None
                st.session_state["redaction_preview_summary"] = None
                st.session_state["redaction_commit_summary"] = None
                st.session_state["redaction_batch_commit_results"] = None
                st.session_state["redaction_rechunk_summary"] = None
                st.session_state["redaction_embedding_summary"] = None

    with st.expander("Prior plans", expanded=False):
        history_artifact_label = st.selectbox(
            "Inspect history for artifact",
            artifact_labels,
            index=0,
            key=f"redaction_history_artifact_{run_id}",
        )
        history_artifact = artifact_options[history_artifact_label]
        history_artifact_id = int(history_artifact["artifact_id"])

        plan_history = service.list_plan_history_for_source_artifact(
            source_artifact_id=history_artifact_id,
            limit=10,
        )

        if plan_history:
            st.dataframe(
                [
                    {
                        "plan_id": item.get("plan_id"),
                        "run_id": item.get("run_id"),
                        "profile": item.get("profile"),
                        "ruleset_version": item.get("ruleset_version"),
                        "plan_status": item.get("plan_status"),
                        "suggestions_created": item.get("suggestions_created"),
                        "approval_id": item.get("approval_id"),
                        "redacted_artifact_id": item.get("redacted_artifact_id"),
                        "plan_created_utc": item.get("plan_created_utc"),
                        "approved_utc": item.get("approved_utc"),
                        "committed_utc": item.get("committed_utc"),
                    }
                    for item in plan_history
                ],
                width="stretch",
            )

            history_options = {
                (
                    f"plan {item['plan_id']} | "
                    f"{item['profile']} | "
                    f"{item['ruleset_version']} | "
                    f"suggestions={item['suggestions_created']}"
                ): item
                for item in plan_history
            }

            selected_history_label = st.selectbox(
                "Inspect prior plan history",
                list(history_options.keys()),
                key=f"redaction_history_{history_artifact_id}",
            )
            selected_history = history_options[selected_history_label]

            hist_left, hist_right = st.columns([1, 1])
            with hist_left:
                st.markdown("#### Category counts")
                st.dataframe(
                    [
                        {"category": category, "count": count}
                        for category, count in selected_history.get("category_counts", {}).items()
                    ],
                    width="stretch",
                )
            with hist_right:
                st.markdown("#### Prior plan state")
                st.json(
                    {
                        "plan_id": selected_history.get("plan_id"),
                        "run_id": selected_history.get("run_id"),
                        "profile": selected_history.get("profile"),
                        "ruleset_version": selected_history.get("ruleset_version"),
                        "ruleset_hash": selected_history.get("ruleset_hash"),
                        "plan_status": selected_history.get("plan_status"),
                        "suggestions_created": selected_history.get("suggestions_created"),
                        "approval_id": selected_history.get("approval_id"),
                        "approved_utc": selected_history.get("approved_utc"),
                        "redacted_artifact_id": selected_history.get("redacted_artifact_id"),
                        "redacted_artifact_path": selected_history.get("redacted_artifact_path"),
                        "committed_utc": selected_history.get("committed_utc"),
                    }
                )

            st.caption(
                "Historical preview text is not currently persisted in the schema, so prior preview "
                "documents cannot be reloaded yet from history."
            )
        else:
            st.write("No prior plans found for this source artifact.")

    plan = st.session_state["redaction_plan_summary"]
    if plan is not None:
        with st.expander("Current plan", expanded=True):
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Plan ID", str(plan.plan_id))
            c2.metric("Run ID", str(plan.run_id))
            c3.metric("Status", plan.status)
            c4.metric("Suggestions", str(plan.suggestions_created))

            st.caption(
                f"Artifacts selected: {plan.artifacts_selected} | "
                f"Artifacts with suggestions: {plan.artifacts_with_suggestions}"
            )

            st.markdown("#### Category counts")
            st.dataframe(
                [
                    {"category": category, "count": count}
                    for category, count in plan.category_counts.items()
                ],
                width="stretch",
            )

            if st.button("Approve plan", width="stretch"):
                approval = service.record_approval(
                    RedactionApprovalRequest(
                        plan_id=plan.plan_id,
                        approval_flags={"approved_in_app": True},
                    )
                )
                st.session_state["redaction_approval_summary"] = approval
                st.session_state["redaction_preview_summary"] = None
                st.session_state["redaction_commit_summary"] = None
                st.session_state["redaction_batch_commit_results"] = None
                st.session_state["redaction_rechunk_summary"] = None
                st.session_state["redaction_embedding_summary"] = None

    approval = st.session_state["redaction_approval_summary"]
    if approval is not None:
        with st.expander("Approval", expanded=True):
            c1, c2, c3 = st.columns(3)
            c1.metric("Approval ID", str(approval.approval_id))
            c2.metric("Plan ID", str(approval.plan_id))
            c3.metric("Status", approval.status)

            source_artifact_id = st.session_state["redaction_planned_artifact_id"]
            planned_ids = st.session_state["redaction_planned_artifact_ids"] or []

            if len(planned_ids) != 1:
                st.info(
                    "Preview text is only shown for single-artifact plans. "
                    "Batch commit is available below for this approved multi-artifact plan."
                )

                commit_artifact_ids = service.list_artifact_ids_with_suggestions_for_plan(
                    plan.plan_id
                )

                st.caption(
                    f"Artifacts selected in plan: {plan.artifacts_selected} | "
                    f"Artifacts with redaction suggestions: {plan.artifacts_with_suggestions}"
                )

                if not commit_artifact_ids:
                    st.info(
                        "This approved plan currently has no artifacts with redaction "
                        "suggestions to commit. This usually means the corpus is already "
                        "redacted for this profile/ruleset, or the selected artifacts did "
                        "not produce any matches."
                    )
                    st.session_state["redaction_batch_commit_results"] = None
                    return

                batch_output_root = st.text_input(
                    "Batch commit output directory",
                    value=str(service.db_path.parent.parent / "redacted"),
                )

                if st.button("Commit all artifacts in approved plan", width="stretch"):
                    st.session_state["redaction_batch_commit_results"] = service.commit_batch(
                        artifact_ids=commit_artifact_ids,
                        profile=plan.profile,
                        ruleset_version=plan.ruleset_version,
                        ruleset_hash=plan.ruleset_hash,
                        plan_id=plan.plan_id,
                        approval_id=approval.approval_id,
                        output_root=Path(batch_output_root),
                    )

                batch_results = st.session_state["redaction_batch_commit_results"]
                if batch_results:
                    success_count = len([r for r in batch_results if r["status"] == "COMPLETE"])
                    failure_count = len(batch_results) - success_count

                    r1, r2, r3 = st.columns(3)
                    r1.metric("Artifacts attempted", str(len(batch_results)))
                    r2.metric("Committed", str(success_count))
                    r3.metric("Failed", str(failure_count))

                    st.dataframe(batch_results, width="stretch")

                return

            if source_artifact_id is None:
                st.error("No planned artifact is bound to the current plan.")
                return

            if st.button("Generate preview", width="stretch"):
                preview = service.get_preview(
                    RedactionPreviewRequest(
                        source_artifact_id=source_artifact_id,
                        profile=plan.profile,
                        ruleset_version=plan.ruleset_version,
                        ruleset_hash=plan.ruleset_hash,
                        plan_id=plan.plan_id,
                        approval_id=approval.approval_id,
                    )
                )
                st.session_state["redaction_preview_summary"] = preview
                st.session_state["redaction_commit_summary"] = None
                st.session_state["redaction_batch_commit_results"] = None

    preview = st.session_state["redaction_preview_summary"]
    if preview is not None:
        with st.expander("Preview", expanded=True):
            st.write(preview.status)

            preview_doc = preview.document
            redaction_meta = preview_doc.get("redaction", {})
            text_content = preview_doc.get("text_content", "")

            preview_left, preview_right = st.columns([1, 2])

            with preview_left:
                st.markdown("#### Redaction metadata")
                st.json(redaction_meta)

            with preview_right:
                st.markdown("#### Preview text")
                st.text_area(
                    "Preview text",
                    value=text_content[:2000],
                    height=320,
                    label_visibility="collapsed",
                )

            default_output = service.build_default_output_path(
                source_artifact_id=preview.source_artifact_id,
                profile=plan.profile,
                plan_id=plan.plan_id,
            )
            output_path = st.text_input(
                "Commit artifact output path",
                value=str(default_output),
            )

            if st.button("Commit redaction", width="stretch"):
                commit = service.commit(
                    RedactionCommitRequest(
                        source_artifact_id=preview.source_artifact_id,
                        profile=plan.profile,
                        ruleset_version=plan.ruleset_version,
                        ruleset_hash=plan.ruleset_hash,
                        plan_id=plan.plan_id,
                        approval_id=approval.approval_id,
                        artifact_output_path=Path(output_path).resolve(),
                    )
                )
                st.session_state["redaction_commit_summary"] = commit

    commit = st.session_state["redaction_commit_summary"]
    if commit is not None:
        with st.expander("Commit", expanded=True):
            c1, c2, c3 = st.columns(3)
            c1.metric("Source Artifact ID", str(commit.source_artifact_id))
            c2.metric("Redacted Artifact ID", str(commit.redacted_artifact_id))
            c3.metric("Status", commit.status)

            st.write(f"Artifact path: `{commit.artifact_path}`")
            st.write(f"Artifact hash: `{commit.artifact_hash}`")

            truth_state = service.get_truth_override_state(commit.source_artifact_id)
            st.markdown("#### Active truth override")
            st.json(truth_state)
    with st.expander("Post-processing: rechunk and embeddings", expanded=False):
        if planning_mode != "single":
            st.info("Post-processing is currently available in single-artifact mode.")
        else:
            pipeline_source_artifact_id = (
                st.session_state["redaction_planned_artifact_id"]
                if st.session_state["redaction_planned_artifact_id"] is not None
                else selected_artifact_id
            )

            active_truth_artifact_path = None
            if pipeline_source_artifact_id is not None:
                truth_state = service.get_truth_override_state(pipeline_source_artifact_id)
                if truth_state and truth_state.get("active_artifact_path"):
                    active_truth_artifact_path = truth_state["active_artifact_path"]

            if not active_truth_artifact_path:
                active_truth_artifact_path = selected_artifact_active_truth_path

            if not active_truth_artifact_path:
                st.info("No active truth artifact is available for rechunking.")
            else:
                st.write(f"Active truth artifact: `{active_truth_artifact_path}`")

                default_chunk_output = (
                    service.db_path.parent.parent
                    / "chunks"
                    / f"{Path(active_truth_artifact_path).stem}.chunks.json"
                )

                chunk_output_path = st.text_input(
                    "Chunk artifact output path",
                    value=str(default_chunk_output),
                )

                if st.button("Rechunk active truth", width="stretch"):
                    st.session_state["redaction_rechunk_summary"] = service.rechunk_search_context_artifact(
                        artifact_path=Path(active_truth_artifact_path),
                        output_path=Path(chunk_output_path),
                    )
                    st.session_state["redaction_embedding_summary"] = None

                rechunk_summary = st.session_state["redaction_rechunk_summary"]
                if rechunk_summary is not None:
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Chunk status", str(rechunk_summary["status"]))
                    c2.metric("Chunk count", str(rechunk_summary["chunk_count"]))
                    c3.metric("Source mode", str(rechunk_summary["chunk_source_mode"]))

                    st.write(f"Chunk artifact path: `{rechunk_summary['chunk_artifact_path']}`")

                    embedding_output_dir = st.text_input(
                        "Embedding output directory",
                        value=str(service.db_path.parent.parent / "embeddings"),
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
                        st.session_state["redaction_embedding_summary"] = service.generate_embeddings_for_chunk_artifact(
                            chunk_artifact_path=Path(rechunk_summary["chunk_artifact_path"]),
                            output_dir=Path(embedding_output_dir),
                            embedding_model=embedding_model,
                            endpoint=embedding_endpoint,
                            batch_size=int(embedding_batch_size),
                        )

                embedding_summary = st.session_state["redaction_embedding_summary"]
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