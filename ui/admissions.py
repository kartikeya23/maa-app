# pages/admissions.py
import json
from datetime import date

import streamlit as st

import db
import reports

_PAGE_SIZE = 50


def render(conn) -> None:
    st.title("Admissions")

    opts = db.get_filter_options(conn)

    with st.sidebar:
        st.subheader("Filters")
        name_search = st.text_input("Search name", placeholder="Patient name…")
        date_from   = st.date_input("From date", value=None)
        date_to     = st.date_input("To date",   value=None)
        pol_year    = st.selectbox("Policy Year", ["(all)"] + opts["policy_year"])
        status      = st.selectbox("Status",      ["(all)"] + opts["status"])
        speciality  = st.selectbox("Speciality",  ["(all)"] + opts["pkg_speciality_name"])

    filters: dict = {}
    if date_from:
        filters["date_from"] = str(date_from)
    if date_to:
        filters["date_to"] = str(date_to)
    if pol_year != "(all)":
        filters["policy_year"] = pol_year
    if status != "(all)":
        filters["status"] = status
    if speciality != "(all)":
        filters["pkg_speciality_name"] = speciality

    df = db.query_admissions(conn, filters)

    if name_search:
        df = df[df["patient_name"].str.contains(name_search, case=False, na=False)]

    # Reset page when filters (including name search) change
    _filter_hash = json.dumps({**filters, "_name": name_search}, sort_keys=True)
    if st.session_state.get("adm_filters_hash") != _filter_hash:
        st.session_state["adm_filters_hash"] = _filter_hash
        st.session_state["adm_page_num"] = 1

    st.write(f"**{len(df):,} admissions** match the current filters.")

    if df.empty:
        st.info("No records found. Adjust filters or ingest data.")
    else:
        total_pages = max(1, (len(df) - 1) // _PAGE_SIZE + 1)
        page_num = max(1, min(st.session_state.get("adm_page_num", 1), total_pages))
        start    = (page_num - 1) * _PAGE_SIZE
        page_df  = df.iloc[start : start + _PAGE_SIZE]

        st.dataframe(page_df, width='stretch', hide_index=True)

        if total_pages > 1:
            pc1, pc2, pc3 = st.columns([1, 4, 1])
            if pc1.button("← Prev", disabled=(page_num <= 1), key="adm_prev", width="stretch"):
                st.session_state["adm_page_num"] = page_num - 1
                st.rerun()
            row_end = min(start + _PAGE_SIZE, len(df))
            pc2.markdown(
                f"<div style='text-align:center;padding-top:6px;color:#666;font-size:0.9rem'>"
                f"Page {page_num} of {total_pages} &ensp;·&ensp; rows {start + 1}–{row_end} of {len(df)}"
                f"</div>",
                unsafe_allow_html=True,
            )
            if pc3.button("Next →", disabled=(page_num >= total_pages), key="adm_next", width="stretch"):
                st.session_state["adm_page_num"] = page_num + 1
                st.rerun()

        st.divider()

        selected_tid = st.text_input("Enter TID to view package details")
        if selected_tid:
            pkgs = db.query_packages_for_tid(conn, selected_tid.strip())
            if pkgs.empty:
                st.warning(f"No packages found for TID: {selected_tid}")
            else:
                with st.expander(f"Packages for {selected_tid}", expanded=True):
                    st.dataframe(pkgs, width='stretch', hide_index=True)

        st.divider()
        xlsx_bytes = reports.generate_report(df, "Admissions", "admission_report")
        st.download_button(
            label="Download filtered data as Excel",
            data=xlsx_bytes,
            file_name=f"MAA_Admissions_{date.today()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
