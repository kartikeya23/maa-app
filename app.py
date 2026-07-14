"""
MAA Payment Record Management System — Streamlit Web UI.
Run with: streamlit run app.py
"""

import logging
from pathlib import Path

import streamlit as st

import db
import log
from ui import admissions, dashboard, doctor_share, doctor_summary
from ui import ingest as ingest_page
from ui import reports as reports_page

log.setup_logging()
logger = logging.getLogger("maa.app")

st.set_page_config(
    page_title="MAA Records",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def get_conn():
    logger.info("App started")
    return db.init_db()


@st.cache_resource
def run_daily_backup() -> tuple[Path | None, str | None]:
    """Once per server process; date-named file makes it daily."""
    return db.backup_db(get_conn())


conn = get_conn()
backup_path, backup_error = run_daily_backup()

_VALID_PAGES = {"Dashboard", "Ingest", "Admissions", "Reports", "Doctor Share", "Doctor Summary"}

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
    if st.button(
        "📊 Doctor Summary", key="nav_dsum", width="stretch",
        type="primary" if st.session_state["_page"] == "Doctor Summary" else "secondary",
    ):
        _nav("Doctor Summary")

_PAGE_MAP = {
    "Dashboard":       dashboard,
    "Ingest":          ingest_page,
    "Admissions":      admissions,
    "Reports":         reports_page,
    "Doctor Share":    doctor_share,
    "Doctor Summary":  doctor_summary,
}

try:
    _PAGE_MAP[st.session_state["_page"]].render(conn)
except Exception:
    logger.exception("Unhandled error rendering page %s", st.session_state["_page"])
    raise  # let Streamlit show its error UI

with st.sidebar:
    if backup_path:
        st.caption(f"Last backup: {backup_path.stem.removeprefix('maa-')}")
        if backup_error:
            st.caption(f"⚠️ {backup_error}")
    elif backup_error:
        st.caption(f"⚠️ Backup failed: {backup_error}")
