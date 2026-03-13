# MK1 Session Resume

## 1. Project Identity
- Project: MK1 Deterministic Retrieval Engine
- Architecture: deterministic Modular Expert System (MES)
- Mode: artifact-driven, evaluation-driven, non-agentic

## 2. Non-Negotiable Invariants
- deterministic execution
- no hidden state
- no agentic runtime behavior
- modular experts only
- artifact generation must be preserved
- debuggable pipelines only
- no architectural redesign unless explicitly requested

## 3. Current Development Focus
- retrieval evaluation and hardening
- evaluate_queries.py improvements
- retrieval diagnostics
- ranking quality improvements
- corpus scaling preparation

## 4. Current Pipeline State
- ingestion pipeline working
- search_context_document generation working
- chunk generation working
- BM25 lexical ranking working
- vector search working
- hybrid fusion working
- context assembly working
- deterministic answer extraction working

## 5. Dataset / Artifact Layout
- dataset artifacts live under: artifacts/<dataset_name>/
- current active dataset: test_source_mid
- no new hardcoded artifact paths
- all query/eval/diagnostic artifacts must be dataset-scoped

## 6. Important Files
- query_search_context.py
- tools/evaluate_queries.py
- experts/llm_search/hybrid_fusion_expert.py
- docs/PROJECT_EXPLAINER.md
- docs/SYSTEM_MAP.md
- AGENTS.md

## 7. Current Known Metrics
- mean_precision_at_k: 0.5833333333333333
- mean_recall_at_k: 1.0
- mrr: 0.8666666666666666
- current behavior: recall is strong, precision/ranking still needs hardening

## 8. Last Confirmed Progress
- query_eval artifact schema was expanded
- dataset-scoped artifact layout is working under artifacts/test_source_mid/
- hybrid fusion observability work is in progress
- retrieval diagnostics need to expose lexical_score/vector_score/final_rank cleanly

## 9. Next Intended Step
- continue retrieval observability hardening
- verify lexical_score and vector_score propagate into query_diagnostics
- then use diagnostics to tune hybrid ranking safely

## 10. Artifact Checkpoints
Create or refresh an artifact when any of these happen:
- evaluation metrics change
- ranking logic changes
- artifact schema changes
- dataset path logic changes
- diagnostics fields change
- corpus scaling assumptions change

## 11. Resume Prompt
Resume MK1 retrieval development from this artifact.
Read AGENTS.md, docs/PROJECT_EXPLAINER.md, docs/SYSTEM_MAP.md, and this file first.
Preserve invariants.
Do not redesign the architecture.
Work in small deterministic patches.
Prefer replace-this-with-this edits.
Identify when a new or refreshed artifact should be written to preserve system memory.