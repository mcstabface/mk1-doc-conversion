"""
artifact_validator.py

Centralized artifact schema validation for MK1.

All persisted artifacts must pass validation before being written to disk.
"""

from typing import Dict, Any


COMMON_REQUIRED = [
    "artifact_type",
    "schema_version",
    "created_utc",
    "producer_expert",
    "run_id",
    "status",
]


def _require_fields(obj: Dict[str, Any], fields: list[str], name: str):
    missing = [f for f in fields if f not in obj]
    if missing:
        raise ValueError(f"{name} missing required fields: {missing}")


def validate_common_fields(artifact: Dict[str, Any]):
    _require_fields(artifact, COMMON_REQUIRED, "artifact")


# --------------------------------------------------
# Document Artifact
# --------------------------------------------------

DOCUMENT_REQUIRED = [
    "source_path",
    "logical_path",
    "document_hash",
    "text_content",
]


def validate_search_context_document(artifact: Dict[str, Any]):

    validate_common_fields(artifact)

    if artifact["artifact_type"] != "search_context_document":
        raise ValueError("artifact_type mismatch for document")

    _require_fields(artifact, DOCUMENT_REQUIRED, "search_context_document")

    if not artifact["text_content"]:
        raise ValueError("document text_content cannot be empty")


# --------------------------------------------------
# Chunk Artifact
# --------------------------------------------------

CHUNK_REQUIRED = [
    "chunk_id",
    "logical_path",
    "source_path",
    "document_hash",
    "chunk_index",
    "text",
    "text_hash",
    "token_count",
]


def validate_search_context_chunk(artifact: Dict[str, Any]):

    validate_common_fields(artifact)

    if artifact["artifact_type"] != "search_context_chunk":
        raise ValueError("artifact_type mismatch for chunk")

    _require_fields(artifact, CHUNK_REQUIRED, "search_context_chunk")

    if not artifact["text"]:
        raise ValueError("chunk text cannot be empty")

    if not isinstance(artifact["chunk_index"], int):
        raise ValueError("chunk_index must be int")

    if not isinstance(artifact["text_hash"], str):
        raise ValueError("text_hash must be str")

    if not artifact["text_hash"]:
        raise ValueError("text_hash cannot be empty")

    if not isinstance(artifact["token_count"], int):
        raise ValueError("token_count must be int")
# --------------------------------------------------
# Chunk Collection Artifact
# --------------------------------------------------

CHUNK_COLLECTION_REQUIRED = [
    "source",
    "chunking",
    "chunks",
]


def _validate_chunk_record_in_collection(chunk: Dict[str, Any], idx: int):
    _require_fields(chunk, CHUNK_REQUIRED, f"search_context_chunk_collection.chunks[{idx}]")

    if not chunk["text"]:
        raise ValueError(f"chunk collection item {idx} text cannot be empty")

    if not isinstance(chunk["chunk_index"], int):
        raise ValueError(f"chunk collection item {idx} chunk_index must be int")

    if not isinstance(chunk["text_hash"], str):
        raise ValueError(f"chunk collection item {idx} text_hash must be str")

    if not chunk["text_hash"]:
        raise ValueError(f"chunk collection item {idx} text_hash cannot be empty")

    if not isinstance(chunk["token_count"], int):
        raise ValueError(f"chunk collection item {idx} token_count must be int")


def validate_search_context_chunk_collection(artifact: Dict[str, Any]):

    validate_common_fields(artifact)

    if artifact["artifact_type"] != "search_context_chunk_collection":
        raise ValueError("artifact_type mismatch for chunk collection")

    _require_fields(
        artifact,
        CHUNK_COLLECTION_REQUIRED,
        "search_context_chunk_collection",
    )

    if not isinstance(artifact["source"], dict):
        raise ValueError("search_context_chunk_collection.source must be dict")

    if not isinstance(artifact["chunking"], dict):
        raise ValueError("search_context_chunk_collection.chunking must be dict")

    chunks = artifact["chunks"]
    if not isinstance(chunks, list):
        raise ValueError("search_context_chunk_collection.chunks must be list")

    for idx, chunk in enumerate(chunks):
        if not isinstance(chunk, dict):
            raise ValueError(
                f"search_context_chunk_collection.chunks[{idx}] must be dict"
            )
        _validate_chunk_record_in_collection(chunk, idx)

# NOTE: validate_artifact dispatcher is defined once at the bottom of this file.
# Keeping a single dispatcher prevents accidental schema bypass via function overwrite.

# --------------------------------------------------
# Embedding Artifact
# --------------------------------------------------

EMBED_REQUIRED = [
    "chunk_id",
    "logical_path",
    "document_hash",
    "text_hash",
    "embedding_model",
    "embedding_dim",
    "vector",
]


def validate_embedding_artifact(artifact: Dict[str, Any]):

    validate_common_fields(artifact)

    if artifact["artifact_type"] != "embedding_artifact":
        raise ValueError("artifact_type mismatch for embedding")

    _require_fields(artifact, EMBED_REQUIRED, "embedding_artifact")

    vector = artifact["vector"]

    if not isinstance(vector, list):
        raise ValueError("embedding vector must be list")

    if len(vector) != artifact["embedding_dim"]:
        raise ValueError(
            f"embedding_dim mismatch: expected {artifact['embedding_dim']} got {len(vector)}"
        )


# --------------------------------------------------
# Query Context Artifact
# --------------------------------------------------

QUERY_CONTEXT_REQUIRED = [
    "query_text",
    "ranker",
    "top_k",
    "context_text",
    "sources",
]


def validate_query_context(artifact: Dict[str, Any]):

    validate_common_fields(artifact)

    if artifact["artifact_type"] != "query_context":
        raise ValueError("artifact_type mismatch for query_context")

    _require_fields(artifact, QUERY_CONTEXT_REQUIRED, "query_context")

    if not isinstance(artifact["sources"], list):
        raise ValueError("sources must be list")


# --------------------------------------------------
# Query Answer Artifact
# --------------------------------------------------

QUERY_ANSWER_REQUIRED = [
    "query_text",
    "ranker",
    "top_k",
    "answer_text",
    "source_count",
    "sources",
]


def validate_query_answer(artifact: Dict[str, Any]):

    validate_common_fields(artifact)

    if artifact["artifact_type"] != "query_answer":
        raise ValueError("artifact_type mismatch for query_answer")

    _require_fields(artifact, QUERY_ANSWER_REQUIRED, "query_answer")

    if artifact["source_count"] != len(artifact["sources"]):
        raise ValueError("source_count mismatch")

# --------------------------------------------------
# Query Diagnostics Artifact
# --------------------------------------------------

QUERY_DIAGNOSTICS_REQUIRED = [
    "query_text",
    "ranker",
    "expanded_queries",
    "candidate_count",
    "ranked_count",
    "results",
]


def validate_query_diagnostics(artifact: Dict[str, Any]):

    validate_common_fields(artifact)

    if artifact["artifact_type"] != "query_diagnostics":
        raise ValueError("artifact_type mismatch for query_diagnostics")

    _require_fields(artifact, QUERY_DIAGNOSTICS_REQUIRED, "query_diagnostics")

    if not isinstance(artifact["expanded_queries"], list):
        raise ValueError("expanded_queries must be list")

    if not isinstance(artifact["candidate_count"], int):
        raise ValueError("candidate_count must be int")

    if not isinstance(artifact["ranked_count"], int):
        raise ValueError("ranked_count must be int")

    if "lexical_candidate_count" in artifact and not isinstance(
        artifact["lexical_candidate_count"], int
    ):
        raise ValueError("lexical_candidate_count must be int")

    if "vector_candidate_count" in artifact and not isinstance(
        artifact["vector_candidate_count"], int
    ):
        raise ValueError("vector_candidate_count must be int")

    if "final_returned_count" in artifact and not isinstance(
        artifact["final_returned_count"], int
    ):
        raise ValueError("final_returned_count must be int")

    if "context_used_count" in artifact and not isinstance(
        artifact["context_used_count"], int
    ):
        raise ValueError("context_used_count must be int")

    if "included_results" in artifact and not isinstance(
        artifact["included_results"], list
    ):
        raise ValueError("included_results must be list")

    if "excluded_results" in artifact and not isinstance(
        artifact["excluded_results"], list
    ):
        raise ValueError("excluded_results must be list")

    if not isinstance(artifact["results"], list):
        raise ValueError("results must be list")

# --------------------------------------------------
# Vector Index Metadata Artifact
# --------------------------------------------------

VECTOR_INDEX_METADATA_REQUIRED = [
    "embedding_count",
    "embedding_dim",
    "index_type",
    "distance_metric",
    "entries",
]


def validate_vector_index_metadata(artifact: Dict[str, Any]):

    validate_common_fields(artifact)

    if artifact["artifact_type"] != "vector_index_metadata":
        raise ValueError("artifact_type mismatch for vector_index_metadata")

    _require_fields(artifact, VECTOR_INDEX_METADATA_REQUIRED, "vector_index_metadata")

    if not isinstance(artifact["embedding_count"], int):
        raise ValueError("embedding_count must be int")

    if not isinstance(artifact["embedding_dim"], int):
        raise ValueError("embedding_dim must be int")

    if not isinstance(artifact["entries"], list):
        raise ValueError("entries must be list")

    for i, entry in enumerate(artifact["entries"]):
        if not isinstance(entry, dict):
            raise ValueError(f"entries[{i}] must be dict")

        required = ["chunk_id", "logical_path", "artifact_path"]

        for r in required:
            if r not in entry:
                raise ValueError(f"entries[{i}] missing required field '{r}'")

# --------------------------------------------------
# Corpus Stats Artifact
# --------------------------------------------------

CORPUS_STATS_REQUIRED = [
    "document_count",
    "chunk_count",
    "embedding_count",
    "matched_embedding_count",
    "missing_embedding_count",
    "orphan_embedding_count",
    "embedding_coverage_pct",
]


def validate_corpus_stats(artifact: Dict[str, Any]):

    validate_common_fields(artifact)

    if artifact["artifact_type"] != "corpus_stats":
        raise ValueError("artifact_type mismatch for corpus_stats")

    _require_fields(artifact, CORPUS_STATS_REQUIRED, "corpus_stats")

    numeric_fields = [
        "document_count",
        "chunk_count",
        "embedding_count",
        "matched_embedding_count",
        "missing_embedding_count",
        "orphan_embedding_count",
    ]

    for field in numeric_fields:
        if not isinstance(artifact[field], int):
            raise ValueError(f"{field} must be int")

    if not isinstance(artifact["embedding_coverage_pct"], float):
        raise ValueError("embedding_coverage_pct must be float")

# --------------------------------------------------
# Query Eval Artifact
# --------------------------------------------------

QUERY_EVAL_REQUIRED = [
    "query_count",
    "results",
]

def validate_query_eval(artifact):

    validate_common_fields(artifact)

    if artifact["artifact_type"] != "query_eval":
        raise ValueError("artifact_type mismatch for query_eval")

    _require_fields(artifact, QUERY_EVAL_REQUIRED, "query_eval")

    if not isinstance(artifact["query_count"], int):
        raise ValueError("query_count must be int")

    if not isinstance(artifact["results"], list):
        raise ValueError("results must be list")


# --------------------------------------------------
# Query Eval Run Manifest Artifact
# --------------------------------------------------

QUERY_EVAL_RUN_MANIFEST_REQUIRED = [
    "dataset",
    "query_set_path",
    "query_set_sha256",
    "chunk_root",
    "ranker_requested",
    "top_k",
    "vector_deps_available",
    "vector_deps_unavailable_reason",
]


def validate_query_eval_run_manifest(artifact):

    validate_common_fields(artifact)

    if artifact["artifact_type"] != "query_eval_run_manifest":
        raise ValueError("artifact_type mismatch for query_eval_run_manifest")

    _require_fields(
        artifact,
        QUERY_EVAL_RUN_MANIFEST_REQUIRED,
        "query_eval_run_manifest",
    )

    if not isinstance(artifact["dataset"], str):
        raise ValueError("dataset must be str")

    if not isinstance(artifact["query_set_path"], str):
        raise ValueError("query_set_path must be str")

    if not isinstance(artifact["query_set_sha256"], str) or not artifact["query_set_sha256"]:
        raise ValueError("query_set_sha256 must be non-empty str")

    if not isinstance(artifact["chunk_root"], str):
        raise ValueError("chunk_root must be str")

    if not isinstance(artifact["ranker_requested"], str):
        raise ValueError("ranker_requested must be str")

    if not isinstance(artifact["top_k"], int):
        raise ValueError("top_k must be int")

    if not isinstance(artifact["vector_deps_available"], bool):
        raise ValueError("vector_deps_available must be bool")

    if artifact["vector_deps_unavailable_reason"] is not None and not isinstance(
        artifact["vector_deps_unavailable_reason"], str
    ):
        raise ValueError("vector_deps_unavailable_reason must be str or None")


# --------------------------------------------------
# Dispatcher
# --------------------------------------------------

def validate_artifact(artifact: Dict[str, Any]):

    artifact_type = artifact.get("artifact_type")

    if artifact_type == "search_context_document":
        validate_search_context_document(artifact)

    elif artifact_type == "search_context_chunk":
        validate_search_context_chunk(artifact)

    elif artifact_type == "embedding_artifact":
        validate_embedding_artifact(artifact)

    elif artifact_type == "query_context":
        validate_query_context(artifact)

    elif artifact_type == "query_answer":
        validate_query_answer(artifact)

    elif artifact_type == "search_context_chunk_collection":
        validate_search_context_chunk_collection(artifact)

    elif artifact_type == "query_diagnostics":
        validate_query_diagnostics(artifact)

    elif artifact_type == "vector_index_metadata":
        validate_vector_index_metadata(artifact)

    elif artifact_type == "corpus_stats":
        validate_corpus_stats(artifact)
    
    elif artifact_type == "query_eval":
        validate_query_eval(artifact)

    elif artifact_type == "query_eval_run_manifest":
        validate_query_eval_run_manifest(artifact)

    else:
        raise ValueError(f"Unknown artifact_type: {artifact_type}")