import json
import time
from pathlib import Path

from experts.redaction.redaction_commit_expert import RedactionCommitExpert

db_path = "/home/stabby/Documents/mk1-doc-conversion/artifacts/db/test_source_mid.db"
source_artifact_id = 17
plan_id = 5
approval_id = 1

source_search_context_path = Path(
    "/home/stabby/Documents/mk1-doc-conversion/artifacts/search_context/"
    "IMAGES__0001__EFTA02731023.pdf__eb21e327d83f.json"
)

artifact_output_path = (
    "/home/stabby/Documents/mk1-doc-conversion/artifacts/search_context/"
    "IMAGES__0001__EFTA02731023.pdf__eb21e327d83f.redacted.json"
)

with open(source_search_context_path, "r", encoding="utf-8") as f:
    redacted_document = json.load(f)

# Keep the artifact validator happy by preserving the original valid shape,
# then layer redaction metadata on top.
redacted_document["created_utc"] = int(time.time())
redacted_document["producer_expert"] = "RedactionCommitExpert"
redacted_document["run_id"] = 1
redacted_document["status"] = "COMPLETE"

redacted_document["redaction"] = {
    "plan_id": plan_id,
    "approval_id": approval_id,
    "profile": "business_sensitive",
    "ruleset_version": "v1",
    "ruleset_hash": "business_sensitive_v1",
}

# Minimal commit-path proof: replace text content with obviously redacted text.
redacted_document["text_content"] = "[CURRENCY_AMOUNT] test redacted content"

expert = RedactionCommitExpert(db_path=db_path)

result = expert.run(
    {
        "source_artifact_id": source_artifact_id,
        "redacted_document": redacted_document,
        "profile": "business_sensitive",
        "ruleset_version": "v1",
        "ruleset_hash": "business_sensitive_v1",
        "plan_id": plan_id,
        "approval_id": approval_id,
        "artifact_output_path": artifact_output_path,
    }
)

print(result)