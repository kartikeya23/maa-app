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

_VALID_PAGES = {"Dashboard", "Ingest", "Admissions", "Reports", "Doctor Share"}

if "_page" not in st.session_state:
    _qp = st.query_params.get("page", "Dashboard")
    st.session_state["_page"] = _qp if _qp in _VALID_PAGES else "Dashboard"


def _nav(page: str) -> None:
    st.session_state["_page"] = page
    st.query_params["page"] = page
    st.rerun()


with st.sidebar:
    st.title("🏥 MAA Records")
    for _p in ["Dashboard", "Ingest", "Admissions", "Reports"]:
        if st.button(
            _p, key=f"nav_{_p}", width="stretch",
            type="primary" if st.session_state["_page"] == _p else "secondary",
        ):
            _nav(_p)

    st.divider()
    st.caption("CLINICAL")
    if st.button(
        "🩺 Doctor Share", key="nav_ds", width="stretch",
        type="primary" if st.session_state["_page"] == "Doctor Share" else "secondary",
    ):
        _nav("Doctor Share")

_PAGE_MAP = {
    "Dashboard":    dashboard,
    "Ingest":       ingest_page,
    "Admissions":   admissions,
    "Reports":      reports_page,
    "Doctor Share": doctor_share,
}

_PAGE_MAP[st.session_state["_page"]].render(conn)
