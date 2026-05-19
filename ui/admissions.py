# pages/admissions.py
from datetime import date

import streamlit as st

import db
import reports


def render(conn) -> None:
    st.title("Admissions")

    opts = db.get_filter_options(conn)

    with st.sidebar:
        st.subheader("Filters")
        date_from  = st.date_input("From date", value=None)
        date_to    = st.date_input("To date",   value=None)
        pol_year   = st.selectbox("Policy Year", ["(all)"] + opts["policy_year"])
        status     = st.selectbox("Status",      ["(all)"] + opts["status"])
        speciality = st.selectbox("Speciality",  ["(all)"] + opts["pkg_speciality_name"])

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

    st.write(f"**{len(df):,} admissions** match the current filters.")

    if df.empty:
        st.info("No records found. Adjust filters or ingest data.")
    else:
        PAGE_SIZE = 50
        total_pages = max(1, (len(df) - 1) // PAGE_SIZE + 1)
        page_num = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1)
        start    = (page_num - 1) * PAGE_SIZE
        page_df  = df.iloc[start : start + PAGE_SIZE]

        st.dataframe(page_df, width='stretch', hide_index=True)

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
