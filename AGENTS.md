# MK1 Development Agent Instructions

You are assisting development of the MK1 Deterministic Retrieval Engine.

MK1 is a deterministic Modular Expert System (MES).

The system uses modular experts connected by deterministic pipelines.

The architecture must NOT be redesigned.

---

# Architectural Principles

1. deterministic pipelines
2. artifact-driven architecture
3. modular experts
4. strong observability
5. evaluation-driven development

---

# Non-Negotiable Rules

Do NOT introduce:

- agentic behavior
- hidden state
- nondeterministic logic
- background workers
- runtime learning
- architectural redesign

All pipelines must remain deterministic.

Given the same inputs, the system must produce identical outputs.

---

# Artifact Philosophy

Every stage must produce inspectable artifacts.

Examples:

- search_context_document
- chunks
- query_search_context
- query_diagnostics
- query_eval

Artifacts must remain stable and debuggable.

Never remove artifact generation unless explicitly requested.

---

# Pipeline Structure

Query pipeline:

query
→ SearchContextQueryExpert
→ lexical ranking (BM25)
→ hybrid fusion
→ context assembly
→ deterministic evidence extraction

Ingestion pipeline:

conversion_director
→ FingerprintExpert
→ ConversionRegistryExpert
→ SearchContextDocumentExpert
→ ChunkExpert

---

# Code Modification Rules

When suggesting code changes:

1. Prefer **small patches**
2. Preserve deterministic behavior
3. Preserve artifact schemas
4. Maintain clear expert boundaries
5. Avoid rewriting entire files

When possible, provide:

"replace this block with this block"

style patches.

---

# Current Development Focus

Retrieval evaluation and hardening.

Relevant files:

tools/evaluate_queries.py
query_search_context.py
hybrid_fusion_expert.py

Focus areas:

- retrieval diagnostics
- ranking quality
- corpus scaling preparation

Do not redesign the architecture.

- Do not hardcode dataset names, artifact roots, or corpus-specific paths in reusable pipeline code.
- Derive paths from provided inputs or config.
- Tool defaults may be dataset-scoped only when explicitly intended for local evaluation convenience.