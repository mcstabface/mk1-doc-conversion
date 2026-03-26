PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_utc INTEGER NOT NULL,
    finished_utc INTEGER,
    source_root TEXT NOT NULL,
    status TEXT NOT NULL,
    files_discovered INTEGER NOT NULL DEFAULT 0,
    files_eligible INTEGER NOT NULL DEFAULT 0,
    files_converted INTEGER NOT NULL DEFAULT 0,
    files_skipped INTEGER NOT NULL DEFAULT 0,
    files_failed INTEGER NOT NULL DEFAULT 0,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS source_artifacts (
    artifact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    physical_path TEXT NOT NULL,
    container_path TEXT,
    logical_path TEXT NOT NULL,
    source_type TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    modified_utc INTEGER,
    sha256 TEXT NOT NULL,
    first_seen_run_id INTEGER NOT NULL,
    last_seen_run_id INTEGER NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    UNIQUE(logical_path, sha256),
    FOREIGN KEY(first_seen_run_id) REFERENCES runs(run_id),
    FOREIGN KEY(last_seen_run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS conversions (
    conversion_id INTEGER PRIMARY KEY AUTOINCREMENT,
    artifact_id INTEGER NOT NULL,
    run_id INTEGER NOT NULL,
    output_pdf_path TEXT NOT NULL,
    converter_used TEXT NOT NULL,
    conversion_status TEXT NOT NULL,
    error_message TEXT,
    created_utc INTEGER NOT NULL,
    FOREIGN KEY(artifact_id) REFERENCES source_artifacts(artifact_id),
    FOREIGN KEY(run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS conversion_decisions (
    decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    artifact_id INTEGER NOT NULL,
    decision_type TEXT NOT NULL,
    reason TEXT,
    source_hash TEXT,
    output_pdf_path TEXT,
    pdf_hash TEXT,
    registry_run_id INTEGER,
    created_utc INTEGER NOT NULL,
    FOREIGN KEY(run_id) REFERENCES runs(run_id),
    FOREIGN KEY(artifact_id) REFERENCES source_artifacts(artifact_id)
);

CREATE TABLE IF NOT EXISTS search_context_registry (
    source_path TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    artifact_hash TEXT NOT NULL,
    artifact_path TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    created_utc INTEGER NOT NULL,
    run_id INTEGER NOT NULL,
    PRIMARY KEY (source_path, artifact_type)
);

CREATE TABLE IF NOT EXISTS redacted_artifacts (
    redacted_artifact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_artifact_id INTEGER NOT NULL,
    plan_id INTEGER NOT NULL,
    approval_id INTEGER NOT NULL,
    profile TEXT NOT NULL,
    ruleset_version TEXT NOT NULL,
    ruleset_hash TEXT NOT NULL,
    artifact_path TEXT NOT NULL,
    artifact_hash TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    created_utc INTEGER NOT NULL,
    FOREIGN KEY(source_artifact_id) REFERENCES source_artifacts(artifact_id)
);

CREATE TABLE IF NOT EXISTS artifact_truth_overrides (
    source_artifact_id INTEGER PRIMARY KEY,
    active_artifact_type TEXT NOT NULL,
    active_artifact_path TEXT NOT NULL,
    active_artifact_hash TEXT NOT NULL,
    redacted_artifact_id INTEGER NOT NULL,
    created_utc INTEGER NOT NULL,
    FOREIGN KEY(source_artifact_id) REFERENCES source_artifacts(artifact_id),
    FOREIGN KEY(redacted_artifact_id) REFERENCES redacted_artifacts(redacted_artifact_id)
);

CREATE TABLE IF NOT EXISTS redaction_plan_runs (
    plan_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    profile TEXT NOT NULL,
    ruleset_version TEXT NOT NULL,
    ruleset_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    created_utc INTEGER NOT NULL,
    FOREIGN KEY(run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS redaction_plan_suggestions (
    suggestion_id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id INTEGER NOT NULL,
    artifact_id INTEGER NOT NULL,
    category TEXT NOT NULL,
    original_text TEXT NOT NULL,
    replacement_text TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    created_utc INTEGER NOT NULL,
    FOREIGN KEY(plan_id) REFERENCES redaction_plan_runs(plan_id),
    FOREIGN KEY(artifact_id) REFERENCES source_artifacts(artifact_id)
);

CREATE TABLE IF NOT EXISTS redaction_approvals (
    approval_id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id INTEGER NOT NULL,
    approved_utc INTEGER NOT NULL,
    approval_flags TEXT,
    FOREIGN KEY(plan_id) REFERENCES redaction_plan_runs(plan_id)
);

CREATE INDEX IF NOT EXISTS idx_truth_override_redacted_id
    ON artifact_truth_overrides(redacted_artifact_id);

CREATE INDEX IF NOT EXISTS idx_search_context_registry_source_hash
    ON search_context_registry(source_hash);

CREATE INDEX IF NOT EXISTS idx_search_context_registry_run_id
    ON search_context_registry(run_id);

CREATE INDEX IF NOT EXISTS idx_conversion_decisions_run_id
ON conversion_decisions(run_id);

CREATE INDEX IF NOT EXISTS idx_conversion_decisions_artifact_id
ON conversion_decisions(artifact_id);

CREATE INDEX IF NOT EXISTS idx_source_artifacts_logical_path
    ON source_artifacts(logical_path);

CREATE INDEX IF NOT EXISTS idx_source_artifacts_last_seen_run_id
    ON source_artifacts(last_seen_run_id);

CREATE INDEX IF NOT EXISTS idx_conversions_artifact_id
    ON conversions(artifact_id);

CREATE INDEX IF NOT EXISTS idx_conversions_run_id
    ON conversions(run_id);
