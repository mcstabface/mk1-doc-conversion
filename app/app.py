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

    st.write(f"Loaded capability: `{selected.capability_id}`")
    st.write("Shell online. Capability views not wired yet.")


if __name__ == "__main__":
    main()