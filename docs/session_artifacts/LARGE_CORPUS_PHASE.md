```
\# MK1 Retrieval System – Large Corpus Phase
```

```
`This document defines the next development phase for the MK1 deterministic retrieval system: \*\*large corpus scaling and stress testing\*\*.`

`The goal of this phase is to verify that the architecture continues to perform correctly and efficiently as corpus size increases significantly.`

`---`

`\# Phase Goals`

`The system currently operates on a \*\*small validation corpus (~1000 chunks)\*\*.`

`Next we will test behavior on significantly larger corpora to validate:`

`- ingestion stability`

`- embedding generation performance`

`- vector index scalability`

`- hybrid retrieval behavior`

`- ranking stability`

`- query latency`

`- artifact generation correctness`

`Target scale milestones:`

`| Phase | Target |`

`|------|------|`

`| Current | ~1K chunks |`

`| Phase 1 | 10K chunks |`

`| Phase 2 | 100K chunks |`

`| Phase 3 | 1M+ chunks |`

`The system must remain \*\*deterministic and artifact-driven\*\* at all scales.`

`---`

`\# Architectural Invariants`

`The following rules must remain true during scaling:`

`1. Deterministic pipeline behavior`

`2. Artifact-first architecture`

`3. Dataset isolation under \`artifacts/\<dataset\>/\``

`4. No hardcoded dataset names`

`5. All pipeline stages produce inspectable artifacts`

`6. No hidden state or agentic behaviors`

`7. Query evaluation must remain reproducible`

`---`

`\# Current System Components`

`The retrieval pipeline currently includes:`
```

document ingestion  
→ search\_context\_document  
→ chunk generation  
→ embedding generation  
→ vector index  
→ lexical ranking (BM25)  
→ vector search  
→ hybrid fusion  
→ context assembly  
→ deterministic answer extraction  
→ query diagnostics  
→ query evaluation  
→ query comparison

```
Artifacts are stored under:
```

artifacts/\<dataset\>/

```
Major artifact types:

`- \`search\_context\``

`- \`search\_context\_chunks\``

`- \`embeddings\``

`- \`query\_context\``

`- \`query\_diagnostics\``

`- \`query\_eval\``

`- \`query\_eval\_compare\``

`---`

`\# Scaling Risks to Evaluate`

`Large corpus scaling may introduce new issues:`

`\#\#\# Indexing risks`

`- embedding generation speed`

`- vector index build time`

`- index memory size`

`- artifact explosion`

`\#\#\# Query-time risks`

`- vector search latency`

`- BM25 ranking latency`

`- hybrid fusion cost`

`- context assembly cost`

`\#\#\# Quality risks`

`- ranking drift`

`- duplicate source dominance`

`- vector noise`

`- lexical overshadowing`

`---`

`\# New Metrics to Capture`

`The system must begin capturing query-time metrics:`
```

query\_latency\_total\_ms  
bm25\_ranking\_latency\_ms  
vector\_search\_latency\_ms  
fusion\_latency\_ms  
context\_assembly\_latency\_ms

```
These metrics should be written to query diagnostics artifacts.

`---`

`\# Large Corpus Test Plan`

`\#\# Step 1 – Expand Corpus`

`Add significantly more documents.`

`Possible sources:`

`- additional FBI release documents`

`- public legal filings`

`- Wikipedia dumps`

`- news archives`

`- technical documents`

`Goal:`
```

10K–50K chunks

```
---

`\#\# Step 2 – Re-run Ingestion Pipeline`

`Run the full ingestion pipeline:`
```

doc\_to\_search\_context\_expert  
→ search\_context\_chunk\_expert  
→ embedding\_chunk\_expert  
→ build\_vector\_index

```
Verify:

`- artifact counts`

`- index build success`

`- embedding reuse logic`

`---`

`\#\# Step 3 – Validate Retrieval`

`Run:`
```

python tools/evaluate\_queries.py

```
Verify:

`- recall remains high`

`- precision does not collapse`

`- diagnostics artifacts remain readable`

`---`

`\#\# Step 4 – Compare Against Baseline`

`Use:`
```

python tools/compare\_query\_eval.py

```
to compare against the current relevance baseline.

`---`

`\#\# Step 5 – Measure Query Latency`

`Begin tracking:`

`- query runtime`

`- vector search time`

`- ranking time`

`Ensure queries remain performant under large corpus sizes.`

`---`

`\# Success Criteria`

`The large corpus phase succeeds if:`

`- the system scales to at least \*\*50K chunks\*\*`

`- evaluation metrics remain stable`

`- query latency remains reasonable`

`- artifact pipeline remains inspectable`

`- deterministic behavior is preserved`

`---`

`\# Deliverables`

`During this phase we expect to produce:`

`- large corpus ingestion scripts`

`- corpus statistics artifacts`

`- latency metrics`

`- improved diagnostics`

`- updated evaluation baselines`
```


