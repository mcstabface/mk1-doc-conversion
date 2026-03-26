import argparse
import json

from experts.redaction_plan_expert import RedactionPlanExpert


def parse_args():
    parser = argparse.ArgumentParser(
        description="Smoke test: redaction plan creation for one source artifact."
    )
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--run-id", required=True, type=int)
    parser.add_argument("--source-artifact-id", required=True, type=int)
    parser.add_argument("--profile", default="business_sensitive")
    parser.add_argument("--ruleset-version", required=True)
    parser.add_argument("--ruleset-hash", required=True)
    return parser.parse_args()


def main():
    args = parse_args()

    expert = RedactionPlanExpert(db_path=args.db_path)

    payload = {
        "run_id": args.run_id,
        "profile": args.profile,
        "ruleset_version": args.ruleset_version,
        "ruleset_hash": args.ruleset_hash,
        "artifact_ids": [args.source_artifact_id],
    }

    result = expert.run(payload)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()