MK1 Modular Expert System (MES)
Deterministic Document Ingestion + Retrieval Engine

Overview
MK1 is a deterministic, artifact-driven document retrieval system designed for enterprise-scale data.
It converts raw documents into structured artifacts and retrieves evidence-backed answers using a hybrid approach (keyword + semantic search).
This is not a chatbot.
It is controlled retrieval infrastructure.

What This Solves
Enterprise document systems typically fail because:
    • Important context is split across documents
    • Exact wording rarely matches user queries
    • Large documents dominate results
    • Retrieval results are inconsistent and not explainable
MK1 addresses this by:
    • combining keyword + semantic retrieval
    • enforcing diversity across documents
    • reconstructing context during retrieval
    • producing fully traceable outputs

Core Principles
    • Deterministic → same input = same output
    • Artifact-driven → every stage produces inspectable data
    • Modular → each step is handled by a specific “expert”
    • Explainable → no black-box behavior

High-Level Pipeline
Documents
  ↓
Normalization
  ↓
Chunking
  ↓
Embeddings
  ↓
Vector Index
  ↓
Hybrid Retrieval (BM25 + Vector)
  ↓
Ranking + Diversity (MMR)
  ↓
Context Assembly
  ↓
Answer Generation

Quick Start
1. Install Dependencies
    • Python 3.10+
    • Ollama (for embeddings + local models)
Install Python dependencies:
pip install -r requirements.txt

2. Start Ollama
ollama serve
Pull required models:
ollama pull nomic-embed-text
ollama pull qwen2.5:3b-instruct

3. Ingest Documents
python main.py \
  --source ./your_documents \
  --artifact-root ./artifacts/run_001
This will generate:
    • search context documents
    • chunk artifacts
    • embeddings
    • vector index

4. Run a Query
python query_search_context.py \
  --query "your question here" \
  --artifact-root ./artifacts/run_001 \
  --ranker hybrid
Output includes:
    • answer
    • supporting evidence
    • ranked chunks
    • diagnostics

Key Concepts (Simple)
Chunking
Documents are split into smaller pieces (“chunks”).
Why:
    • makes retrieval possible
Limitation:
    • meaning can be split across chunks
MK1 handles this during retrieval (not by perfecting chunking).

Hybrid Retrieval
Two methods combined:
    • Keyword search (BM25) → precise
    • Semantic search (embeddings) → flexible
Result:
    • accurate + tolerant of wording differences

Diversity (MMR)
Prevents results from being dominated by one document.
Ensures:
    • broader evidence
    • better answers

Context Assembly
Controls how much information is used to answer a query.
Includes:
    • max total context size
    • max chunks per document

Adjacency (Context Repair)
When a chunk is selected, nearby chunks can also be included.
Purpose:
    • restore meaning lost during chunking

Configuration (Important Knobs)
Chunking
Parameter	Description
chunk_size	Size of each chunk
overlap	Overlap between chunks
Guideline:
    • Larger = more context
    • Smaller = more precision

Retrieval
Parameter	Description
ranker	bm25 / hybrid
vector_bonus_weight	weight of semantic signal
vector_only_score_floor	threshold for semantic-only results

Diversity
Parameter	Description
lambda_weight	balance relevance vs diversity

Context Assembly
Parameter	Description
max_context_chars	total context size
max_chunks_per_source	limit per document

Output Artifacts
Each query produces:
Query Answer
artifacts/query_answer/
    • final answer
    • supporting evidence

Query Context
artifacts/query_context/
    • selected chunks
    • source references

Diagnostics
artifacts/query_diagnostics/
    • ranking decisions
    • scoring details
    • candidate counts

What to Expect
Works well when:
    • documents are moderately structured
    • queries are real user questions
    • hybrid retrieval is enabled

Known Limitations
    • chunk boundaries can split meaning
    • ranking still needs tuning in edge cases
    • large datasets require optimized vector indexing
    • embedding generation can be slow without batching

Recommended Usage Pattern
If starting fresh:
    1. ingest a small dataset
    2. run 5–10 real queries
    3. evaluate results
    4. adjust:
        ◦ chunk size
        ◦ max_chunks_per_source
    5. scale up

Design Philosophy
Most systems try to:
perfectly split documents
MK1 assumes:
document splitting is imperfect
and compensates during retrieval by:
    • combining search methods
    • enforcing diversity
    • reconstructing context

Future Improvements
    • ANN vector search (FAISS/HNSW tuning)
    • smarter chunking (structure-aware)
    • improved ranking signals
    • UI enhancements
    • retrieval benchmarking expansion

Contributing / Extending
The system is built around modular experts.
To extend:
    • add new expert under experts/
    • define input/output artifacts
    • plug into pipeline
Do not:
    • introduce non-deterministic behavior
    • bypass artifact generation

Summary
MK1 provides:
    • deterministic ingestion
    • hybrid retrieval
    • explainable outputs
    • scalable architecture
It is designed for:
reliable answers from real-world, messy documents

Here is a clean, production-quality README you can drop into the repo. It’s written for developers who want to use the system quickly without needing to understand MES deeply.

MK1 Modular Expert System (MES)
Deterministic Document Ingestion + Retrieval Engine

Overview
MK1 is a deterministic, artifact-driven document retrieval system designed for enterprise-scale data.
It converts raw documents into structured artifacts and retrieves evidence-backed answers using a hybrid approach (keyword + semantic search).
This is not a chatbot.
It is controlled retrieval infrastructure.

What This Solves
Enterprise document systems typically fail because:
    • Important context is split across documents
    • Exact wording rarely matches user queries
    • Large documents dominate results
    • Retrieval results are inconsistent and not explainable
MK1 addresses this by:
    • combining keyword + semantic retrieval
    • enforcing diversity across documents
    • reconstructing context during retrieval
    • producing fully traceable outputs

Core Principles
    • Deterministic → same input = same output
    • Artifact-driven → every stage produces inspectable data
    • Modular → each step is handled by a specific “expert”
    • Explainable → no black-box behavior

High-Level Pipeline
Documents
  ↓
Normalization
  ↓
Chunking
  ↓
Embeddings
  ↓
Vector Index
  ↓
Hybrid Retrieval (BM25 + Vector)
  ↓
Ranking + Diversity (MMR)
  ↓
Context Assembly
  ↓
Answer Generation

Quick Start
1. Install Dependencies
    • Python 3.10+
    • Ollama (for embeddings + local models)
Install Python dependencies:
pip install -r requirements.txt

2. Start Ollama
ollama serve
Pull required models:
ollama pull nomic-embed-text
ollama pull qwen2.5:3b-instruct

3. Ingest Documents
python main.py \
  --source ./your_documents \
  --artifact-root ./artifacts/run_001
This will generate:
    • search context documents
    • chunk artifacts
    • embeddings
    • vector index

4. Run a Query
python query_search_context.py \
  --query "your question here" \
  --artifact-root ./artifacts/run_001 \
  --ranker hybrid
Output includes:
    • answer
    • supporting evidence
    • ranked chunks
    • diagnostics

Key Concepts (Simple)
Chunking
Documents are split into smaller pieces (“chunks”).
Why:
    • makes retrieval possible
Limitation:
    • meaning can be split across chunks
MK1 handles this during retrieval (not by perfecting chunking).

Hybrid Retrieval
Two methods combined:
    • Keyword search (BM25) → precise
    • Semantic search (embeddings) → flexible
Result:
    • accurate + tolerant of wording differences

Diversity (MMR)
Prevents results from being dominated by one document.
Ensures:
    • broader evidence
    • better answers

Context Assembly
Controls how much information is used to answer a query.
Includes:
    • max total context size
    • max chunks per document

Adjacency (Context Repair)
When a chunk is selected, nearby chunks can also be included.
Purpose:
    • restore meaning lost during chunking

Configuration (Important Knobs)
Chunking
Parameter	Description
chunk_size	Size of each chunk
overlap	Overlap between chunks
Guideline:
    • Larger = more context
    • Smaller = more precision

Retrieval
Parameter	Description
ranker	bm25 / hybrid
vector_bonus_weight	weight of semantic signal
vector_only_score_floor	threshold for semantic-only results

Diversity
Parameter	Description
lambda_weight	balance relevance vs diversity

Context Assembly
Parameter	Description
max_context_chars	total context size
max_chunks_per_source	limit per document

Output Artifacts
Each query produces:
Query Answer
artifacts/query_answer/
    • final answer
    • supporting evidence

Query Context
artifacts/query_context/
    • selected chunks
    • source references

Diagnostics
artifacts/query_diagnostics/
    • ranking decisions
    • scoring details
    • candidate counts

What to Expect
Works well when:
    • documents are moderately structured
    • queries are real user questions
    • hybrid retrieval is enabled

Known Limitations
    • chunk boundaries can split meaning
    • ranking still needs tuning in edge cases
    • large datasets require optimized vector indexing
    • embedding generation can be slow without batching

Recommended Usage Pattern
If starting fresh:
    1. ingest a small dataset
    2. run 5–10 real queries
    3. evaluate results
    4. adjust:
        ◦ chunk size
        ◦ max_chunks_per_source
    5. scale up

Design Philosophy
Most systems try to:
perfectly split documents
MK1 assumes:
document splitting is imperfect
and compensates during retrieval by:
    • combining search methods
    • enforcing diversity
    • reconstructing context

Future Improvements
    • ANN vector search (FAISS/HNSW tuning)
    • smarter chunking (structure-aware)
    • improved ranking signals
    • UI enhancements
    • retrieval benchmarking expansion

Contributing / Extending
The system is built around modular experts.
To extend:
    • add new expert under experts/
    • define input/output artifacts
    • plug into pipeline
Do not:
    • introduce non-deterministic behavior
    • bypass artifact generation

Summary
MK1 provides:
    • deterministic ingestion
    • hybrid retrieval
    • explainable outputs
    • scalable architecture
It is designed for:
reliable answers from real-world, messy documents

If you want, next step I’d recommend is:
    • a “dev onboarding checklist” (5–10 min setup guide your team can follow without you)
