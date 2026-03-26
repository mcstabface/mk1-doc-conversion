from experts.redaction.redaction_approval_gate_expert import RedactionApprovalGateExpert

expert = RedactionApprovalGateExpert(
    db_path="/home/stabby/Documents/mk1-doc-conversion/artifacts/db/test_source_mid.db"
)

result = expert.run(
    {
        "plan_id": 5,
        "profile": "business_sensitive",
        "ruleset_version": "v1",
        "ruleset_hash": "business_sensitive_v1",
        "yes_commit": True,
    }
)

print(result)