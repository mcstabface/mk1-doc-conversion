import argparse
import json
from pathlib import Path

from experts.redaction_plan_expert import RedactionPlanExpert
from experts.redaction_approval_gate_expert import RedactionApprovalGateExpert
from experts.redaction_approval_record_expert import RedactionApprovalRecordExpert
from experts.redaction_preview_expert import RedactionPreviewExpert
from experts.redaction_commit_expert import RedactionCommitExpert


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run deterministic redaction lifecycle: plan -> approval -> preview -> optional commit."
    )

    parser.add_argument("--db-path", required=True)
    parser.add_argument("--run-id", required=True, type=int)
    parser.add_argument("--source-artifact-id", required=True, type=int)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--ruleset-version", required=True)
    parser.add_argument("--ruleset-hash", required=True)

    parser.add_argument(
        "--commit",
        action="store_true",
        help="If provided, commit after preview.",
    )

    parser.add_argument(
        "--artifact-output-path",
        help="Required when --commit is set.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    if args.commit and not args.artifact_output_path:
        raise ValueError(
            "--artifact-output-path is required when --commit is set."
        )

    plan_expert = RedactionPlanExpert(db_path=args.db_path)
    approval_gate_expert = RedactionApprovalGateExpert(db_path=args.db_path)
    approval_record_expert = RedactionApprovalRecordExpert(db_path=args.db_path)
    preview_expert = RedactionPreviewExpert(db_path=args.db_path)
    commit_expert = RedactionCommitExpert(db_path=args.db_path)

    plan_payload = {
        "run_id": args.run_id,
        "profile": args.profile,
        "ruleset_version": args.ruleset_version,
        "ruleset_hash": args.ruleset_hash,
        "artifact_ids": [args.source_artifact_id],
    }

        plan_result = plan_expert.run(plan_payload)
        plan_id = plan_result["redaction_plan"]["plan_id"]

        output = {
            "plan": plan_result,
        }

        # Only run gate when committing

        if args.commit:

            gate_payload = {
                "plan_id": plan_id,
                "profile": args.profile,
                "ruleset_version": args.ruleset_version,
                "ruleset_hash": args.ruleset_hash,
                "yes_commit": True,
            }

            gate_result = approval_gate_expert.run(gate_payload)

            output["approval_gate"] = gate_result

        # Always record approval intent

        approval_record_payload = {
            "plan_id": plan_id,
            "approval_flags": {
                "yes_commit": args.commit,
                "source": "redaction_lifecycle_cli",
            },
        }

        approval_record_result = approval_record_expert.run(
            approval_record_payload
        )

        output["approval_record"] = approval_record_result

        approval_id = (
            approval_record_result
            ["redaction_approval_record"]
            ["approval_id"]
        )

        # Always run preview

        preview_payload = {
            "source_artifact_id": args.source_artifact_id,
            "profile": args.profile,
            "ruleset_version": args.ruleset_version,
            "ruleset_hash": args.ruleset_hash,
            "plan_id": plan_id,
            "approval_id": approval_id,
        }

        preview_result = preview_expert.run(preview_payload)

        output["preview"] = preview_result

        # Optional commit

        if args.commit:

            commit_payload = {
                "source_artifact_id": args.source_artifact_id,
                "profile": args.profile,
                "ruleset_version": args.ruleset_version,
                "ruleset_hash": args.ruleset_hash,
                "plan_id": plan_id,
                "approval_id": approval_id,
                "artifact_output_path": str(
                    Path(args.artifact_output_path).resolve()
                ),
            }

            commit_result = commit_expert.run(commit_payload)

            output["commit"] = commit_result

    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()