from experts.redaction.redaction_plan_expert import RedactionPlanExpert

expert = RedactionPlanExpert(
    db_path="/home/stabby/Documents/mk1-doc-conversion/artifacts/db/test_source_mid.db"
)

result = expert.run(
    {
        "run_id": 1,
        "profile": "business_sensitive",
        "ruleset_version": "v1",
        "ruleset_hash": "business_sensitive_v1",
        "artifact_ids": [17],
    }
)

print(result)