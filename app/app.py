from __future__ import annotations

import streamlit as st

from app.config import load_app_config
from app.capability_registry import get_registered_capabilities


def main() -> None:
    config = load_app_config()
    capabilities = get_registered_capabilities()

    st.set_page_config(page_title=config.app_title, layout="wide")
    st.title(config.app_title)

    enabled = [c for c in capabilities if c.enabled_by_default]
    labels = {c.label: c for c in enabled}
    label_names = list(labels.keys())

    pending_label = st.session_state.pop("pending_capability_label", None)
    current_label = st.session_state.get("selected_capability_label")

    if pending_label in labels:
        default_label = pending_label
    elif current_label in labels:
        default_label = current_label
    else:
        default_label = label_names[0]

    selected_label = st.sidebar.radio(
        "Capabilities",
        label_names,
        index=label_names.index(default_label),
    )
    st.session_state["selected_capability_label"] = selected_label

    selected = labels[selected_label]
    st.sidebar.caption(selected.description)

    if selected.capability_id == "ingestion":
        from app.capabilities.ingestion.view import render
        render(config)
    elif selected.capability_id == "redaction":
        from app.capabilities.redaction.view import render
        render(config)
    elif selected.capability_id == "audit_history":
        from app.capabilities.audit_history.view import render
        render(config)


if __name__ == "__main__":
    main()