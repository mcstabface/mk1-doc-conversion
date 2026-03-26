from pathlib import Path
import argparse
import json

from experts.redaction_commit_expert import RedactionCommitExpert


def parse_args():
    parser = argparse.ArgumentParser(
        description="Commit approved redaction and activate as active truth."
    )

    parser.add_argument(
        "--db-path",
        required=True,
        help="SQLite database path",
    )

    parser.add_argument(
        "--source-artifact-id",
        required=True,
        type=int,
    )

    parser.add_argument(
        "--plan-id",
        required=True,
        type=int,
    )

    parser.add_argument(
        "--approval-id",
        required=True,
        type=int,
    )

    parser.add_argument(
        "--profile",
        required=True,
    )

    parser.add_argument(
        "--ruleset-version",
        required=True,
    )

    parser.add_argument(
        "--ruleset-hash",
        required=True,
    )

    parser.add_argument(
        "--artifact-output-path",
        required=True,
        help="Path to write the committed redacted artifact.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    expert = RedactionCommitExpert(
        db_path=args.db_path
    )

    payload = {
        "source_artifact_id": args.source_artifact_id,
        "profile": args.profile,
        "ruleset_version": args.ruleset_version,
        "ruleset_hash": args.ruleset_hash,
        "plan_id": args.plan_id,
        "approval_id": args.approval_id,
        "artifact_output_path": str(Path(args.artifact_output_path).resolve()),
    }

    result = expert.run(payload)

    print("\nCOMMIT RESULT")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()