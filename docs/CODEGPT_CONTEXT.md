You are assisting development of the MK1 Deterministic Retrieval Engine.

This project is a deterministic Modular Expert System (MES).

Architecture rules:

1. deterministic pipelines
2. artifact-driven architecture
3. modular expert components
4. strong observability
5. evaluation-driven development

Important constraints:

- do not redesign the architecture
- do not introduce agentic behavior
- do not introduce hidden state
- preserve artifact generation
- preserve deterministic execution

The system uses modular experts connected by deterministic pipelines.

Key pipelines:

INGESTION PIPELINE
conversion_director → experts → artifacts

QUERY PIPELINE
query_search_context.py
→ SearchContextQueryExpert
→ BM25 lexical ranking
→ hybrid fusion
→ context assembly

Artifacts are critical and must be preserved.

Important artifact types:

- search_context_document
- chunks
- query_search_context
- query_diagnostics
- query_eval

When making code changes:

1. prefer small patches
2. preserve deterministic behavior
3. preserve artifact schemas
4. keep expert boundaries clear
5. maintain debugging observability

Never introduce architectural redesign unless explicitly requested.