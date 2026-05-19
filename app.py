"""
MAA Payment Record Management System — Streamlit Web UI.
Run with: streamlit run app.py
"""

import streamlit as st

import db
from ui import admissions, dashboard, doctor_share
from ui import ingest as ingest_page
from ui import reports as reports_page

st.set_page_config(
    page_title="MAA Records",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def get_conn():
    return db.init_db()


conn = get_conn()

if "_page" not in st.session_state:
    st.session_state["_page"] = "Dashboard"

with st.sidebar:
    st.title("🏥 MAA Records")
    for _p in ["Dashboard", "Ingest", "Admissions", "Reports"]:
        if st.button(
            _p, key=f"nav_{_p}", width="stretch",
            type="primary" if st.session_state["_page"] == _p else "secondary",
        ):
            st.session_state["_page"] = _p
            st.rerun()

    st.divider()
    st.caption("CLINICAL")
    if st.button(
        "🩺 Doctor Share", key="nav_ds", width="stretch",
        type="primary" if st.session_state["_page"] == "Doctor Share" else "secondary",
    ):
        st.session_state["_page"] = "Doctor Share"
        st.rerun()

_PAGE_MAP = {
    "Dashboard":    dashboard,
    "Ingest":       ingest_page,
    "Admissions":   admissions,
    "Reports":      reports_page,
    "Doctor Share": doctor_share,
}

_PAGE_MAP[st.session_state["_page"]].render(conn)
