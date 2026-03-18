# MES Project Syntax Reference

This document records common commands and operational syntax for working with the Modular Expert System (MES) ingestion and retrieval pipeline.

Purpose:

* Prevent rediscovering command patterns
* Provide copy-paste safe commands
* Serve as operational documentation for demos and team use

---

# Core Pipeline Commands

## Full Context Pipeline

Generate retrieval artifacts (search context documents and chunks).

```
python main.py \
  --mode context \
  --source ./test_source_enron/maildir \
  --artifact-root ./artifacts/enron_full_v2 \
  --db-path ./artifacts/db/conversion_memory.db
```

Produces:

```
artifacts/enron_full_v2/
  search_context_documents/
  search_context_chunks/
```

---

## Embedding Pipeline

Generate embedding artifacts from chunk artifacts.

```
python -m tools.build_embeddings \
  --chunk-root artifacts/enron_full_v2/search_context_chunks \
  --output-root artifacts/enron_full_v2/embeddings
```

With timestamps:

```
python -m tools.build_embeddings \
  --chunk-root artifacts/enron_full_v2/search_context_chunks \
  --output-root artifacts/enron_full_v2/embeddings | ts
```

---

## Filter Embedding by Source Path

```
python -m tools.build_embeddings \
  --chunk-root artifacts/enron_full_v2/search_context_chunks \
  --output-root artifacts/enron_full_v2/embeddings \
  --source-contains test_source_mid
```

---

# Artifact Inspection

## Count chunk artifacts

```
ls artifacts/enron_full_v2/search_context_chunks | wc -l
```

## Count embeddings

```
ls artifacts/enron_full_v2/embeddings | wc -l
```

## Count document artifacts

```
ls artifacts/enron_full_v2/search_context_documents | wc -l
```

---

# Reset Stages

## Reset chunk + embedding stage

Used after modifying chunking logic.

```
rm -rf artifacts/enron_full_v2/search_context_chunks
rm -rf artifacts/enron_full_v2/embeddings
```

Then rerun:

```
python main.py --mode context ...
python -m tools.build_embeddings ...
```

---

# Performance Monitoring

## Watch embeddings being written

```
watch -n2 'ls artifacts/enron_full_v2/embeddings | wc -l'
```

---

# Common Performance Commands

Measure runtime:

```
time python -m tools.build_embeddings ...
```

Monitor system usage:

```
btop
```

---

# Notes

Artifact pipeline stages are deterministic.

Artifacts are treated as canonical truth once written.

Changing expert logic requires deleting downstream artifact directories before regeneration.

---

# Planned Future Commands

Semantic search

```
python -m tools.semantic_search
```

Index builder

```
python -m tools.build_vector_index
```

---

End of file
