# Project / Conversation Summary

## 1. Project Overview

The user is building a **Modular Expert System (MES)** document retrieval pipeline capable of:

- Deterministic ingestion of large document corpora

- Chunk-based document storage

- Hybrid lexical + vector retrieval

- Context assembly for LLM question answering

Current corpus:

- **Enron email dataset**

- ~517k documents

- ~874k chunk artifacts

Goal:

Build a **fast, deterministic, locally-running search + retrieval system** suitable for enterprise document search and AI augmentation.

Architecture emphasizes:

- modular "experts"

- deterministic pipelines

- artifact-based storage

- reproducible runs

- local execution (no external vector DB services)


# 2. Current System State

The pipeline currently supports:

### Document Ingestion

Documents are converted into **search\_context\_chunks artifacts**.

Structure:

```
artifacts/enron\_full\_v2/search\_context\_chunks/
```

Each artifact:

```
\{source\_hash\}.search\_context\_chunks.json
```

Contains:

```
\{

  source: \{ logical\_path, metadata... \},

  chunks: \[

    \{

      chunk\_index,

      chunk\_id,

      text,

      token\_estimate,

      char\_count

    \}

  \]

\}
```

Totals:

```
documents: 517,401

chunks:    874,749
```


### Embedding Store

Embeddings generated using:

```
model: nomic-embed-text

endpoint: http://localhost:11434/api/embeddings
```

Batch embedding implemented.

Artifacts stored in:

```
artifacts/enron\_full\_v2/embeddings
```

Embedding generation completed successfully:

```
874,078 embedding artifacts

0 failures

runtime ≈ 4 hours
```


### Lexical Index

A **sharded SQLite lexical index** was created.

Initial monolithic DB:

```
lexical\_index.db

≈ 42 GB
```

Replaced with **sharded index**:

```
artifacts/enron\_full\_v2/lexical\_index\_sharded/
```

Configuration:

```
shards: 64
```

Each shard:

```
shard\_\{00..63\}.db
```

Table:

```
postings(

    term TEXT,

    logical\_path TEXT,

    chunk\_index INT,

    chunk\_id TEXT

)
```

Shard routing:

```
SHA256(term) → first 8 bytes → mod 64
```

Lookup strategy:

```
query\_terms → shard lookup → SQL SELECT postings
```


### Query Pipeline

Entry script:

```
query\_search\_context.py
```

Pipeline stages:

```
1. query expansion

2. lexical prefilter (sharded index)

3. chunk artifact loading

4. BM25 ranking

5. hybrid fusion

6. MMR diversity ranking

7. context assembly

8. answer generation
```


### Current Performance

Latest run:

```
QUERY: What risks did Enron executives discuss regarding energy trading strategies?

CANDIDATE CHUNKS: 449

RANKED CHUNKS: 7

RETURNED CHUNKS: 5

runtime: ~3.8 seconds
```

Performance progression during development:

```
initial: ~3m34s (full scan)

after fixes: ~18s

current: ~3.8s
```

Major improvement due to:

```
sharded lexical index

selective term intersection

candidate pool reduction
```


# 3. Work Completed

### Ingestion Pipeline

✔ Enron corpus ingested  
✔ 517,401 documents processed  
✔ 874,749 chunk artifacts produced


### Embedding System

✔ Batch embedding via Ollama  
✔ `nomic-embed-text` model  
✔ deterministic artifact storage


### Lexical Search

✔ lexical index builder implemented  
✔ monolithic index replaced with **64-shard SQLite index**


### Query Engine

Implemented experts:

```
search\_context\_query\_expert

bm25\_rank\_expert

hybrid\_fusion\_expert

mmr\_diversity\_ranker

search\_context\_assemble\_expert

search\_context\_answer\_expert
```


### Retrieval Optimizations

Implemented:

```
term selectivity ranking

intersection of rarest terms

fallback to rarest-term-only

candidate chunk deduplication
```

Candidate pool reduced from:

```
874k → 449
```


### Tokenization Standardization

Created shared tokenizer.

Purpose:

```
consistent tokenization across experts

stopword filtering
```

Used by:

```
search\_context\_query\_expert

bm25\_rank\_expert

search\_context\_rank\_expert
```


# 4. Current Problem / Task

The system **works and is performant**, but the next phase is:

```
improving ranking quality and search efficiency
```

Remaining issues:

1. Some **irrelevant chunks appear in top results**

2. Ranking stages may still use inconsistent tokenization

3. Vector search currently scans embedding artifacts instead of using ANN indexing

Focus is now shifting from:

```
retrieval speed → ranking quality
```


# 5. Relevant Files / Modules

### Query Pipeline

```
query\_search\_context.py
```

Main entry point.


### Retrieval Experts

```
experts/llm\_search/search\_context\_query\_expert.py
```

Performs lexical candidate generation using sharded index.


```
experts/llm\_search/search\_context\_bm25\_rank\_expert.py
```

Lexical ranking stage.


```
experts/llm\_search/hybrid\_fusion\_expert.py
```

Combines vector + lexical results.


```
experts/llm\_search/vector\_search\_expert.py
```

Vector similarity retrieval from embedding artifacts.


### Diversity / Filtering

```
experts/llm\_search/mmr\_diversity\_ranker.py
```

Reduces redundant results.


```
experts/llm\_search/score\_gap\_filter.py
```

Filters low relevance results.


### Context Assembly

```
experts/llm\_search/search\_context\_assemble\_expert.py
```

Builds final LLM context window.


### Answer Generation

```
experts/llm\_search/search\_context\_answer\_expert.py
```

Produces final LLM response.


### Embedding Generation

```
experts/llm\_search/embedding\_chunk\_expert.py
```

Generates chunk embeddings.


### Query Embedding

```
experts/llm\_search/query\_embedding\_expert.py
```

Generates embedding for search queries.


### Shared Tokenizer

Recently introduced:

```
experts/llm\_search/tokenization.py
```

Provides:

```
tokenize(text) -\> set\[str\]
```

Includes stopword filtering.


# 6. Key Constraints / Invariants

Must preserve:

```
deterministic artifact structure

modular expert architecture

local execution (no cloud dependencies)
```

Additional rules:

```
do not change chunk artifact schema

do not change embedding artifact format

maintain reproducible pipelines

avoid hidden heuristics
```

Design philosophy:

```
explicit \> implicit

deterministic \> agentic

modular experts \> monolithic systems
```


# 7. Technical Details

### Embedding Model

```
nomic-embed-text
```

Endpoint:

```
http://localhost:11434/api/embeddings
```

Batch embedding supported.


### Sharded Lexical Index

Shard routing:

```
SHA256(term) % 64
```

Query process:

```
tokenize(query)

→ determine shards

→ fetch postings

→ intersect rarest terms
```


### Candidate Selection

Strategy:

```
sort terms by posting count

select rarest 2 terms

intersect postings

fallback to rarest term
```

This keeps candidate pool small.


### Retrieval Pipeline

```
query → tokenize

      → shard lookup

      → candidate refs

      → chunk artifact load

      → BM25 rank

      → hybrid fusion

      → MMR diversity

      → context assembly

      → LLM answer
```


# 8. Decisions Already Made

These architectural decisions are **intentional and should not be changed lightly**:

```
artifact-based architecture

modular experts

local execution

SQLite sharded lexical index

Ollama for embeddings

hybrid lexical + vector search
```

Vector DB services (Pinecone etc.) are intentionally avoided.


# 9. Known Issues / Limitations

### Ranking Quality

Some irrelevant chunks appear in top results.

Possible causes:

```
BM25 weighting

query expansion noise

term intersection strategy
```


### Vector Search Performance

Vector retrieval currently reads embedding artifacts directly.

No ANN index yet.


### Tokenization Consistency

Recently centralized tokenizer.

Need to verify **all ranking stages use it**.


# 10. Next Action

Primary next step:

```
verify and enforce shared tokenizer usage across ALL ranking experts
```

Then improve ranking quality by tuning:

```
BM25 scoring

hybrid fusion weights

candidate selection strategy
```

Potential future optimization:

```
ANN index for vector search

(FAISS or HNSW)
```

Goal:

```
\< 1 second query latency

high quality ranking
```

