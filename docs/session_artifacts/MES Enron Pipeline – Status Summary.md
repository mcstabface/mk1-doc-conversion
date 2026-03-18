# MES Enron Pipeline – Status Summary

## Project

Deterministic enterprise document ingestion and retrieval system built on a **Modular Expert System (MES)** architecture.

Goal:  
Provide **deterministic retrieval with inspectable artifacts** from enterprise documents.

UI shows:

- document count

- chunk count

- embeddings count

- deterministic run signature

- ranked evidence for answers


# System Architecture

Pipeline stages:

```
Raw documents

    ↓

Conversion Experts

    ↓

search\_context\_document artifacts

    ↓

Chunking Expert

    ↓

search\_context\_chunks artifacts

    ↓

Embedding Expert

    ↓

embedding artifacts

    ↓

Vector Index

    ↓

Hybrid Retrieval (lexical + vector)
```

All artifacts are **JSON and deterministic**.


# Dataset

Dataset used:

```
Enron Maildir
```

Location:

```
./test\_source\_enron/maildir
```

Total processed:

```
517,401 emails
```


# Ingestion Results

Full ingestion completed successfully.

Validation:

```
chunk\_file\_count: 517401

non\_empty\_chunk\_files: 517401
```

Meaning:

- Every document produced a chunk artifact

- No empty chunk artifacts remain

- Pipeline conversion stage is stable

Artifacts produced:

```
artifacts/enron\_full\_v2/search\_context/

artifacts/enron\_full\_v2/search\_context\_chunks/
```


# Performance

Conversion performance:

```
517,401 documents

≈ 1 hour ingestion
```

Chunking:

```
517,401 chunks
```

Hardware:

```
CPU: i7-13700HX

RAM: 60 GB

GPU: RTX (8GB)
```


# Embedding Stage

Embedding expert validated with a smoke test.

Example output:

```
embedding\_vector\_batch

written\_count: 1

artifact\_paths:

sent\_231...embedding.json
```

So the embedding expert works correctly.


# Problem Discovered

Current embedding pipeline is **serial**.

Observed system state during embedding run:

CPU usage:

```
~5–15%
```

GPU usage:

```
~57%
```

Embedding script:

```
python tools/build\_embeddings.py
```

Throughput estimate:

```
~4 embeddings/sec
```

Total embeddings required:

```
517,401
```

Estimated runtime:

```
≈ 36 hours
```


# Root Cause

EmbeddingChunkExpert sends **one embedding request per chunk**:

```
1 chunk → 1 HTTP request → Ollama
```

This creates massive overhead.

The system is **request-bound**, not compute-bound.


# Correct Solution

Switch to **batch embeddings**.

Instead of:

```
1 embedding request
```

Send:

```
32–128 chunks per request
```

Expected improvement:

```
10×–30× faster
```

Projected embedding time:

```
1–3 hours instead of 36
```


# Current Status

Embedding run was **terminated intentionally** to implement batching.

Partial embeddings were deleted.

Corpus ready for re-embedding:

```
artifacts/enron\_full\_v2/search\_context\_chunks/
```

Total chunk artifacts:

```
517,401
```


# Next Engineering Task

Modify:

```
experts/llm\_search/embedding\_chunk\_expert.py
```

Goal:

Add **batch embedding requests** to Ollama.


# Desired Behavior

Current:

```
for chunk in chunks:

    embed(chunk)
```

New behavior:

```
batch = \[\]

for chunk in chunks:

    batch.append(chunk)

if len(batch) == batch\_size:

    embed(batch)
```

Batch size target:

```
64 or 128
```


# Ollama Endpoint

Current endpoint:

```
http://localhost:11434/api/embeddings
```

Model:

```
nomic-embed-text
```

Expected request format:

```
\{

  "model": "nomic-embed-text",

  "input": \["text1","text2","text3"\]

\}
```


# Success Criteria

After patch:

Running

```
tools/build\_embeddings.py
```

should produce:

```
517,401 embedding artifacts
```

Runtime target:

```
≈ 1–3 hours
```


# Demo Objective

The final demo should show:

1️⃣ deterministic ingestion  
2️⃣ artifact inspection  
3️⃣ scalable pipeline  
4️⃣ hybrid retrieval  
5️⃣ evidence-backed answers

With statistics visible in the UI:

```
Documents: 517401

Chunks: 517401

Embeddings: 517401
```


# End of Status Artifact

