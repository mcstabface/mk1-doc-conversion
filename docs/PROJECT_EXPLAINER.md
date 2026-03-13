

```
`\# MK1 — Project Explainer`


`This file orients coding agents and contributors to the MK1 system.`


`Read this file before making changes.`


`---`


`\# What MK1 Is`


`MK1 is a \*\*deterministic Modular Expert System (MES)\*\* designed for:`


`- document ingestion`

`- structured artifact generation`

`- deterministic search context retrieval`

`- inspectable pipelines`


`The system is intentionally \*\*not agentic\*\*.`


`All behavior is implemented through \*\*modular experts connected by deterministic pipelines\*\*.`


`Every stage produces \*\*structured artifacts\*\* that can be inspected, persisted, and replayed.`


`---`


`\# Core Design Principles`


`MK1 follows five architectural principles.`


`1. \*\*Deterministic Execution\*\*`


`Pipelines must produce identical outputs given identical inputs.`


`2. \*\*Artifact-Driven Architecture\*\*`


`Every stage outputs a structured artifact that can be inspected.`


`3. \*\*Modular Experts\*\*`


`Each expert performs one bounded responsibility.`


`4. \*\*Strong Observability\*\*`


`Artifacts must allow debugging of every stage.`


`5. \*\*Evaluation-Driven Development\*\*`


`System improvements are validated through evaluation artifacts.`


`---`


`\# Non-Negotiable Architectural Invariants`


`These rules \*\*must not be violated\*\*.`


`See:`


arch\_invariants.md


`Key invariants include:`


`- No hidden state`

`- No agentic runtime behavior`

`- Pipelines must remain deterministic`

`- Experts must remain modular and bounded`

`- Artifacts must remain inspectable`

`- The system must remain debuggable`


`If a change risks violating these rules, stop and reconsider the approach.`


`---`


`\# High-Level System Architecture`


`MK1 is composed of deterministic pipelines built from experts.`


`\#\# Ingestion Pipeline`


source documents  
↓  
conversion\_director  
↓  
FingerprintExpert  
↓  
ConversionRegistryExpert  
↓  
SearchContextDocumentExpert  
↓  
ChunkExpert


`Artifacts produced:`


`- document inventory`

`- fingerprints`

`- registry entries`

`- search context documents`

`- chunks`


`---`


`\#\# Query Pipeline`


query  
↓  
SearchContextQueryExpert  
↓  
BM25 lexical ranking  
↓  
context assembly  
↓  
deterministic evidence extraction


`Artifacts produced:`


`- query\_search\_context`

`- ranked\_chunks`

`- query\_diagnostics`


`---`


`\# Important Artifact Types`


`Artifacts are stored to allow inspection and replay.`


search\_context\_document  
chunks  
query\_search\_context  
query\_diagnostics


`Artifacts are critical for debugging and evaluation.`


`---`


`\# Current Development Focus`


`Current development is focused on \*\*retrieval evaluation and hardening\*\*.`


`Primary work areas:`


tools/evaluate\_queries.py  
retrieval diagnostics  
ranking quality improvements  
corpus scaling preparation


`Architectural redesign is \*\*not the goal\*\*.`


`---`


`\# Key Files`


`\#\#\# Architectural Rules`


arch\_invariants.md


`\#\#\# Pipeline Directors`


director/conversion\_director.py


`\#\#\# Query Pipeline`


query\_search\_context.py  
search\_context\_query\_expert.py


`\#\#\# Chunk Generation`


chunk\_expert.py


`\#\#\# Retrieval Evaluation`


tools/evaluate\_queries.py


`---`


`\# Working Rules for Code Changes`


`When modifying the system:`


`1. Preserve deterministic behavior`

`2. Preserve artifact generation`

`3. Avoid architectural redesign`

`4. Prefer small, safe patches`

`5. Maintain clear expert boundaries`


`Large refactors should only occur if explicitly requested.`


`---`


`\# When in Doubt`


`If a design decision is unclear:`


`1. Check \`arch\_invariants.md\``

`2. Inspect existing artifact schemas`

`3. Follow established pipeline patterns`

`4. Prefer consistency with existing experts`


`Consistency and determinism take priority over novelty.`


`---`


`\# Summary`


`MK1 is a \*\*deterministic modular expert system\*\* designed for transparent, inspectable pipelines.`


`Goals:`


`- deterministic execution`

`- artifact-first architecture`

`- modular experts`

`- strong observability`

`- evaluation-driven improvement`
```


