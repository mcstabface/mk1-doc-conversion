MK1 SYSTEM MAP

INGESTION PIPELINE
main.py
→ conversion_director.py
→ FingerprintExpert
→ ConversionRegistryExpert

CONTEXT PIPELINE
query_search_context.py
→ SearchContextQueryExpert
→ BM25Ranker
→ ContextAssembler

ARTIFACTS
artifacts/
  search_context_document
  chunks
  query_diagnostics