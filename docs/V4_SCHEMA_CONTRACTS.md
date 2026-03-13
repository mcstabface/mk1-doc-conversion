# V4 Schema Contracts

## Purpose

This document defines the authoritative persisted artifact schemas for MK1 V4.
All schema validation and artifact emission must conform to these contracts.

Principles:
- deterministic
- versioned
- auditable
- fail-fast on invalid artifacts
- no hidden fields relied upon by downstream stages

---

## Common Required Fields

All persisted artifacts MUST include:

- artifact_type: str
- schema_version: str
- created_utc: int
- producer_expert: str
- run_id: int | str | null
- status: str

Recommended status values:
- COMPLETE
- SKIPPED
- FAILED
- STALE

---

## 1. search_context_document

artifact_type:
- search_context_document

schema_version:
- search_context_document_v1

Required fields:
- artifact_type: str
- schema_version: str
- created_utc: int
- producer_expert: str
- run_id: int | str | null
- status: str
- source_path: str
- logical_path: str
- document_hash: str
- text_content: str
- metadata: dict

Optional fields:
- source_type: str
- title: str
- page_count: int
- extraction_method: str

---

## 2. search_context_chunk

artifact_type:
- search_context_chunk

schema_version:
- search_context_chunk_v1

Required fields:
- artifact_type: str
- schema_version: str
- created_utc: int
- producer_expert: str
- run_id: int | str | null
- status: str
- chunk_id: str
- logical_path: str
- source_path: str
- document_hash: str
- chunk_index: int
- text: str
- text_hash: str
- token_count: int

Optional fields:
- start_char: int
- end_char: int
- section_path: list[str]
- source_title: str

Rules:
- chunk_id must be globally unique within corpus
- chunk_index must be zero-based within document
- text must be non-empty when status=COMPLETE
- text_hash must represent the exact persisted chunk text

---

## 3. embedding_artifact

artifact_type:
- embedding_artifact

schema_version:
- embedding_artifact_v1

Required fields:
- artifact_type: str
- schema_version: str
- created_utc: int
- producer_expert: str
- run_id: int | str | null
- status: str
- chunk_id: str
- logical_path: str
- document_hash: str
- text_hash: str
- embedding_model: str
- embedding_dim: int
- vector: list[float]

Optional fields:
- embedding_provider: str
- source_path: str

Rules:
- len(vector) must equal embedding_dim
- embedding_dim must equal expected dimension for embedding_model
- text_hash must represent the exact chunk text used for embedding

---

## 4. query_context

artifact_type:
- query_context

schema_version:
- query_context_v1

Required fields:
- artifact_type: str
- schema_version: str
- created_utc: int
- producer_expert: str
- run_id: int | str | null
- status: str
- query_text: str
- ranker: str
- top_k: int
- context_text: str
- sources: list[dict]

Optional fields:
- expanded_queries: list[str]
- scoring_metadata: dict
- retrieval_diagnostics: dict

---

## 5. query_answer

artifact_type:
- query_answer

schema_version:
- query_answer_v1

Required fields:
- artifact_type: str
- schema_version: str
- created_utc: int
- producer_expert: str
- run_id: int | str | null
- status: str
- query_text: str
- ranker: str
- top_k: int
- answer_text: str
- source_count: int
- sources: list[dict]

Optional fields:
- extracted_evidence: list[dict]
- answer_method: str

Rules:
- source_count must equal len(sources)

---

## Validation Policy

- write-time validation is mandatory
- invalid artifacts must fail the producing step
- downstream experts must assume valid schemas and should not silently repair malformed artifacts
- schema changes require a new schema_version