MK1 Document Conversion & Retrieval System

MK1 is a deterministic modular expert system for document ingestion, artifact generation, and hybrid retrieval.

The system converts raw documents into structured artifacts, generates searchable chunks and embeddings, and performs hybrid lexical + semantic retrieval over the resulting corpus.

The design prioritizes:

determinism

auditability

modular architecture

artifact-driven pipelines

enterprise-grade observability

MK1 intentionally avoids agentic orchestration. Every step in the pipeline is implemented as a single-purpose expert module.

Core Design Principles

MK1 follows several architectural invariants.

Deterministic pipelines

Every stage produces explicit artifacts that can be inspected and reproduced.

Expert modules

Each stage is implemented as a small, isolated component called an Expert.

Artifact-driven architecture

All system state is stored as artifacts rather than hidden runtime memory.

Auditability

Every stage of the system can be replayed and inspected.

LLM isolation

Large language models never operate on raw documents.
They operate only on curated artifacts produced by deterministic pipelines.

Current System Status

Current version: V3 – Hybrid Retrieval Layer

The system now supports:

deterministic document ingestion

normalized search context artifacts

chunk generation

lexical ranking (BM25)

semantic vector search

hybrid retrieval

diversity reranking

deterministic answer extraction

The pipeline is fully artifact-driven and auditable.

System Architecture

The retrieval pipeline currently operates as follows:

Query
 │
 ▼
Query Expansion Expert
 │
 ▼
Search Context Query Expert
 │
 ▼
Ranker Selection
 │
 ├─ overlap
 ├─ bm25
 └─ hybrid
 │
 ├─ BM25 lexical ranking
 │
 ├─ Query Embedding
 │
 ├─ Vector Search
 │
 └─ Hybrid Fusion
 │
 ▼
MMR Diversity Reranking
 │
 ▼
Context Assembly
 │
 ▼
Answer Artifact

Hybrid ranking combines lexical and semantic retrieval while maintaining lexical precision as the primary signal. 

MK1_V3_RETRIEVAL_COMPLETION_ART…

Directory Structure
mk1-doc-conversion

experts/
    ingestion/
    llm_search/
        bm25_rank_expert.py
        embedding_chunk_expert.py
        vector_search_expert.py
        hybrid_fusion_expert.py

tools/
    build_embeddings.py

artifacts/
    search_context_chunks/
    embeddings/
    query_context/
    query_answer/

query_search_context.py

Artifacts are written to disk at every stage of the pipeline.

Artifact Types
Search Context Chunk

Location:

artifacts/search_context_chunks/

Represents a retrieval unit extracted from a document.

Fields typically include:

logical_path
chunk_index
text
token_estimate
source_hash
Embedding Artifact

Location:

artifacts/embeddings/

Each chunk generates one embedding vector.

Embedding model:

nomic-embed-text
dimension: 768

Example artifact:

Procurement_Directive_404-Omega_<hash>_0000.nomic-embed-text.embedding.json
Query Context Artifact

Location:

artifacts/query_context/

Contains:

ranked chunks

query expansion metadata

ranking diagnostics

Query Answer Artifact

Location:

artifacts/query_answer/

Contains:

extracted answer evidence

source references

ranking metadata

Ranking Strategies

The system supports three ranking modes.

Overlap

Baseline ranking using token overlap.

Purpose:

debugging

baseline comparison

BM25

Primary lexical ranking algorithm.

Characteristics:

term frequency scoring

inverse document frequency weighting

deterministic scoring

BM25 provides strong precision for enterprise documents.

Hybrid (V3)

Hybrid retrieval combines lexical ranking with semantic vector search.

Fusion strategy:

vector_bonus_weight = 0.10
vector_only_score_floor = 0.60

Behavior:

lexical ranking forms the backbone

vector matches add a small relevance bonus

vector-only results require strong similarity

This preserves lexical precision while improving recall. 

MK1_V3_RETRIEVAL_COMPLETION_ART…

Running a Query

Example query execution:

python query_search_context.py \
  --query "procurement risk" \
  --chunk-root artifacts/search_context_chunks \
  --ranker hybrid \
  --max-chunks-per-source 1

Outputs:

artifacts/query_context/*.json
artifacts/query_answer/*.json

The CLI also prints debugging diagnostics including ranking details and evidence sources.

Embedding Generation

Embeddings are generated from chunk artifacts using:

tools/build_embeddings.py

Run:

python -m tools.build_embeddings

This tool:

scans chunk artifacts

generates embeddings

writes embedding artifacts

Embeddings are generated using a local Ollama model.

Example Ollama endpoint:

http://localhost:11434/api/embeddings
Current Test Corpus

The V3 system has been validated using a small controlled corpus:

Procurement Directive 404-Omega

Deep Space Risk & Issue Log

Operation Luminous Apex

Griffin Plumage SOP

Exoplanetary Biosphere SOW

The dataset was intentionally small to allow deterministic debugging.

Known Limitations

These limitations are expected at the V3 stage.

Corpus Size

Testing has been performed on a small dataset.

Future testing targets:

~100 documents
~3000 documents
Vector Search Performance

Vector search currently uses brute-force cosine similarity.

This approach will not scale indefinitely.

Possible future solutions:

FAISS

HNSW

vector databases

Embedding Pipeline

Embeddings currently run as a batch process.

Future improvements may include:

incremental embedding updates

embedding validation

embedding coverage diagnostics

V4 Roadmap

V4 development will focus on scaling and system hardening.

Primary goals:

large corpus testing

PDF ingestion pipeline

scalable vector indexing

retrieval diagnostics

ranking tuning

artifact schema tightening

search observability

Repository

GitHub repository:

https://github.com/mcstabface/mk1-doc-conversion

Summary

MK1 is a deterministic retrieval architecture designed to demonstrate a modular expert system approach to document processing and hybrid search.

At the completion of V3 the system now supports:

lexical search

semantic vector retrieval

hybrid fusion ranking

deterministic artifact pipelines

This provides a stable foundation for scaling the system in V4.
