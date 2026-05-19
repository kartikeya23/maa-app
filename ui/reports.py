# pages/reports.py
from datetime import date

import streamlit as st

import db
import reports as _reports


def render(conn) -> None:
    st.title("Reports")

    report_type = st.selectbox(
        "Report type",
        ["Admission Summary", "Monthly Summary", "FY Summary", "FY Admission Detail",
         "Month Admission Detail", "Raw Export"],
    )

    month_pick       = None
    fy_pick          = "(all)"
    fy_detail_pick   = None
    month_detail_pick: list[str] = []

    with st.sidebar:
        st.subheader("Report Filters")
        opts = db.get_filter_options(conn)
        status_filter = st.multiselect("Status", opts["status"])

        if report_type == "Monthly Summary":
            month_pick = st.text_input("Month (YYYY-MM, blank = all)") or None
        elif report_type == "FY Summary":
            fy_df      = db.query_fy_summary(conn)
            fy_options = sorted(fy_df["financial_year"].unique().tolist()) if not fy_df.empty else []
            fy_pick    = st.selectbox("Financial Year", ["(all)"] + fy_options)
        elif report_type == "FY Admission Detail":
            fy_detail_options = db.get_available_fys(conn)
            fy_detail_pick    = st.selectbox("Financial Year", fy_detail_options) if fy_detail_options else None
        elif report_type == "Month Admission Detail":
            month_options = db.get_available_months(conn)
            month_options.sort(reverse=True)
            month_detail_pick = st.multiselect(
                "Month(s)", month_options, default=month_options[:1] if month_options else []
            )

    if report_type == "Admission Summary":
        df = db.query_admissions(conn)
        if status_filter:
            df = df[df["statuses"].apply(
                lambda s: any(f in (s or "") for f in status_filter)
            )]
        rtype = "admission_report"
        title = "MAA Admission Summary"

    elif report_type == "Monthly Summary":
        df = db.query_monthly_summary(conn)
        if month_pick:
            df = df[df["month"] == month_pick]
        rtype = "monthly_summary"
        title = "MAA Monthly Summary"

    elif report_type == "FY Summary":
        df = db.query_fy_summary(conn)
        if fy_pick != "(all)":
            df = df[df["financial_year"] == fy_pick]
        rtype = "fy_summary"
        title = "MAA FY Summary"

    if report_type in ("Admission Summary", "Monthly Summary", "FY Summary"):
        st.subheader(f"Preview ({min(20, len(df))} of {len(df):,} rows)")
        st.dataframe(df.head(20), width='stretch', hide_index=True)
        if not df.empty:
            xlsx_bytes = _reports.generate_report(df, title, rtype)
            st.download_button(
                label=f"Download {report_type} as Excel",
                data=xlsx_bytes,
                file_name=f"MAA_{rtype}_{date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            st.info("No data for this selection.")

    elif report_type == "FY Admission Detail":
        if fy_detail_pick:
            df    = db.query_fy_admission_detail(conn, fy_detail_pick)
            title = f"MAA FY Admission Detail {fy_detail_pick}"

            st.subheader(f"Preview ({min(20, len(df))} of {len(df):,} rows)")
            st.dataframe(df.head(20), width='stretch', hide_index=True)

            if not df.empty:
                xlsx_bytes = _reports.generate_fy_detail_report(df, fy_detail_pick)
                st.download_button(
                    label="Download FY Admission Detail as Excel",
                    data=xlsx_bytes,
                    file_name=f"MAA_FY_Detail_{fy_detail_pick}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            else:
                st.info("No data for this financial year.")
        else:
            st.info("No financial year data available. Ingest some records first.")

    elif report_type == "Month Admission Detail":
        if month_detail_pick:
            df    = db.query_month_admission_detail(conn, month_detail_pick)
            label = ", ".join(sorted(month_detail_pick))
            title = f"MAA Month Admission Detail {label}"

            st.subheader(f"Preview ({min(20, len(df))} of {len(df):,} rows)")
            st.dataframe(df.head(20), width='stretch', hide_index=True)

            if not df.empty:
                xlsx_bytes = _reports.generate_month_detail_report(df, label)
                st.download_button(
                    label="Download Month Admission Detail as Excel",
                    data=xlsx_bytes,
                    file_name=f"MAA_Month_Detail_{label.replace(', ', '_')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            else:
                st.info("No data for the selected month(s).")
        else:
            st.info("Select at least one month from the sidebar.")

    else:  # Raw Export
        df    = db.query_all_claims(conn)
        if status_filter:
            df = df[df["status"].isin(status_filter)]
        rtype = "raw_export"
        title = "MAA Raw Export"

        st.subheader(f"Preview ({min(20, len(df))} of {len(df):,} rows)")
        st.dataframe(df.head(20), width='stretch', hide_index=True)

        if not df.empty:
            xlsx_bytes = _reports.generate_report(df, title, rtype)
            st.download_button(
                label=f"Download {report_type} as Excel",
                data=xlsx_bytes,
                file_name=f"MAA_{rtype}_{date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            st.info("No data for this selection.")
