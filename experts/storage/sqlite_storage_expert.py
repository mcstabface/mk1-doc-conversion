cursor.execute("""
CREATE TABLE IF NOT EXISTS doc_conversion_registry (
    source_path TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    pdf_hash TEXT,
    output_pdf TEXT NOT NULL,
    created_utc INTEGER NOT NULL,
    run_id INTEGER,
    PRIMARY KEY (source_path, source_hash)
)
""")

cursor.execute("""
CREATE INDEX IF NOT EXISTS idx_doc_conversion_registry_run_id
ON doc_conversion_registry(run_id)
""")