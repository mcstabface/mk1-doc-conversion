import streamlit as st

from demo_backend import load_demo_summary, run_demo_query

st.set_page_config(page_title="MK1 Demo", layout="wide")

DATASET_NAME = "enron_full_v2"
summary = load_demo_summary(DATASET_NAME)

st.markdown(
    """
    <style>
    .block-container {padding-top: 2rem; padding-bottom: 2rem; max-width: 1200px;}
    h1, h2, h3 {letter-spacing: -0.02em;}
    .hero {padding: 1.25rem 0 1.5rem 0;}
    .sub {font-size: 1.1rem; color: #4b5563; margin-top: -0.5rem;}
    .card {
        padding: 1rem 1.1rem;
        border: 1px solid #e5e7eb;
        border-radius: 16px;
        background: #f9fafb;
        color: #111827;
    }

    .card * {
        color: #111827 !important;
    }
    .label {font-size: 0.8rem; color: #6b7280; text-transform: uppercase; letter-spacing: 0.06em;}
    .metric {font-size: 2rem; font-weight: 700; color: #111827;}
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown('<div class="hero"><h1>Reliable answers from enterprise documents</h1><div class="sub">Deterministic retrieval with visible evidence and inspectable artifacts.</div></div>', unsafe_allow_html=True)

c1, c2, c3, c4 = st.columns(4)
for col, label, value in [
    (c1, "Documents", summary["document_count"]),
    (c2, "Chunks", summary["chunk_count"]),
    (c3, "Embeddings", summary["embedding_count"]),
    (c4, "Artifact Types", len(summary["artifact_types"])),
]:
    with col:
        st.markdown(f'<div class="card"><div class="label">{label}</div><div class="metric">{value}</div></div>', unsafe_allow_html=True)

st.markdown("### Query")
query_text = st.text_input(
    "Ask a question about the corpus",
    placeholder="What changed in the project plan?",
    label_visibility="collapsed"
)
run_query = st.button("Run Query", use_container_width=True)

query_result = None
query_error = None

if run_query and query_text.strip():
    try:
        query_result = run_demo_query(DATASET_NAME, query_text)
    except Exception as exc:
        query_error = exc

if query_error is not None:
    st.error(f"Query execution failed: {type(query_error).__name__}: {query_error}")

if query_result:
    diagnostics = query_result.get("diagnostics_summary", {}) or {}
    evidence = query_result.get("evidence", []) or []

    chunk_list = []
    for item in evidence:
        if isinstance(item, dict) and item.get("chunk_index") is not None:
            chunk_list.append(str(item["chunk_index"]))

    run_id = (
        query_result.get("run_id")
        or diagnostics.get("run_id")
        or query_result.get("retrieval_trace", {}).get("run_id")
        or "unknown"
    )
    ranker = diagnostics.get("ranker", "unknown")
    ranked_count = diagnostics.get("ranked_count", 0)
    query_used = diagnostics.get("query_text", query_text)

    st.markdown("### Deterministic Run Signature")
    with st.expander(f"Run ID • {run_id}", expanded=False):
        st.markdown(
            f"""
<div class="card">
<b>Query:</b> {query_used}<br>
<b>Run ID:</b> {run_id}<br>
<b>Ranking Method:</b> {ranker}<br>
<b>Ranked Results:</b> {ranked_count}<br>
<b>Chunks Used:</b> {", ".join(chunk_list) if chunk_list else "none"}
</div>
""",
            unsafe_allow_html=True,
        )

left, right = st.columns([1.2, 1])

with left:
    st.markdown("### Answer")
    if query_result:
        st.markdown(f'<div class="card">{query_result["answer_text"]}</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="card">Answer will appear here.</div>', unsafe_allow_html=True)

with right:
    st.markdown("### Why this answer")

    if query_result:
        evidence = query_result.get("evidence", [])

        if evidence:
            for i, item in enumerate(evidence[:3], start=1):
                if isinstance(item, dict):
                    source = item.get("logical_path") or item.get("source") or "source"
                    chunk = item.get("chunk_index", "?")
                    score = item.get("score", "?")
                    chunk_id = item.get("chunk_id", "")

                    with st.expander(f"Evidence #{i} • {source} • score {score}", expanded=False):
                        st.markdown(
                            f"""
<div class="card">
<b>Source:</b> {source}<br>
<b>Chunk:</b> {chunk}<br>
<b>Score:</b> {score}<br>
<b>Chunk ID:</b> {chunk_id[:24]}...
</div>
""",
                            unsafe_allow_html=True,
                        )
                else:
                    with st.expander(f"Evidence #{i}", expanded=False):
                        st.markdown(
                            f'<div class="card">{str(item)}</div>',
                            unsafe_allow_html=True,
                        )
        else:
            st.markdown('<div class="card">No evidence returned.</div>', unsafe_allow_html=True)

    else:
        st.markdown(
            '<div class="card">Evidence and source support will appear here.</div>',
            unsafe_allow_html=True,
        )


st.markdown("### Evidence Ranking")
if query_result:
    results = query_result.get("ranked_results", [])
    if results:
        for i, r in enumerate(results[:5], start=1):
            source = r.get("source", "source")
            score = r.get("score", "?")
            text = r.get("text", "")

            with st.expander(f"#{i} • {source} • score {score}", expanded=(i == 1)):
                st.markdown(
                    f'<div class="card">{text}</div>',
                    unsafe_allow_html=True,
                )
    else:
        st.markdown("No ranked results.")
else:
    st.markdown('<div class="card">Top source results will appear here.</div>', unsafe_allow_html=True)