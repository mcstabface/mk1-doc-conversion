from experts.redaction.redaction_approval_record_expert import RedactionApprovalRecordExpert

expert = RedactionApprovalRecordExpert(
    db_path="/home/stabby/Documents/mk1-doc-conversion/artifacts/db/test_source_mid.db"
)

result = expert.run(
    {
        "plan_id": 5,
        "approval_flags": {
            "yes_commit": True,
            "reviewed_by": "test_user",
        },
    }
)

print(result)