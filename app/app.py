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
    selected_label = st.sidebar.radio("Capabilities", list(labels.keys()))
    selected = labels[selected_label]

    st.sidebar.caption(selected.description)

    if selected.capability_id == "ingestion":
        from app.capabilities.ingestion.view import render
        render(config)
    elif selected.capability_id == "redaction":
        st.info("Redaction capability shell present but not wired yet.")
    elif selected.capability_id == "audit_history":
        st.info("Audit / History capability shell present but not wired yet.")


if __name__ == "__main__":
    main()