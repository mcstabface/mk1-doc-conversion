from pathlib import Path
import argparse
import json

from experts.redaction_preview_expert import RedactionPreviewExpert


def parse_args():

    parser = argparse.ArgumentParser(
        description="Preview redacted document without committing changes."
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

    return parser.parse_args()


def main():

    args = parse_args()

    expert = RedactionPreviewExpert(
        db_path=args.db_path
    )

    payload = {
        "source_artifact_id": args.source_artifact_id,
        "profile": args.profile,
        "ruleset_version": args.ruleset_version,
        "ruleset_hash": args.ruleset_hash,
        "plan_id": args.plan_id,
        "approval_id": args.approval_id,
    }

    result = expert.run(payload)

    print("\nPREVIEW RESULT")
    print(
        json.dumps(
            result,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()