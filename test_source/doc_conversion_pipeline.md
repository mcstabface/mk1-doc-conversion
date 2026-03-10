# MK1 Document Conversion Pipeline

Purpose:
Deterministic delta-aware Word-to-PDF conversion pipeline with artifact-first persistence.

Core rules:
- Track each discovered source artifact in SQLite
- Treat zip contents as logical artifacts with provenance
- Convert only new or changed eligible sources
- Preserve immutable run history
- Store PDFs as artifacts, not as the only source of truth

Initial schema:
- runs
- source_artifacts
- conversions