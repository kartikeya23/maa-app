"""
MAA Payment Record Management System — Streamlit Web UI.
Run with: streamlit run app.py
"""

import glob
from datetime import date
from pathlib import Path

import plotly.express as px
import streamlit as st

import db
import ingest as ingest_module
import reports

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="MAA Records",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── DB connection (cached) ────────────────────────────────────────────────────

@st.cache_resource
def get_conn():
    return db.init_db()


conn = get_conn()

# ── Sidebar navigation ────────────────────────────────────────────────────────

st.sidebar.title("🏥 MAA Records")
page = st.sidebar.radio(
    "Navigate",
    ["Dashboard", "Ingest", "Admissions", "Reports"],
    index=0,
)

# ── Helper ────────────────────────────────────────────────────────────────────

def fmt_inr(val: float) -> str:
    if val is None:
        return "₹0"
    return f"₹{val:,.0f}"


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: INGEST
# ══════════════════════════════════════════════════════════════════════════════

if page == "Ingest":
    st.title("Ingest CSV Files")

    uploaded = st.file_uploader(
        "Upload GenericSearchReport CSV files",
        type=["csv"],
        accept_multiple_files=True,
    )
    auto_detect = st.button("Auto-detect CSVs in current directory")
    dry_run     = st.checkbox("Dry Run (parse only, don't write to DB)")

    csv_paths: list[str] = []

    # Save uploaded files to temp paths
    if uploaded:
        tmp_dir = Path("/tmp/maa_uploads")
        tmp_dir.mkdir(exist_ok=True)
        for f in uploaded:
            dest = tmp_dir / f.name
            dest.write_bytes(f.read())
            csv_paths.append(str(dest))

    if auto_detect:
        # CSVs live in the parent directory (one level above maa_app/)
        csv_dir = Path(__file__).parent.parent
        cwd_csvs = sorted(glob.glob(str(csv_dir / "GenericSearchReport*.csv")))
        csv_paths = cwd_csvs
        if cwd_csvs:
            st.info(f"Found {len(cwd_csvs)} CSV file(s) in `{csv_dir}`.")
        else:
            st.warning(f"No GenericSearchReport*.csv files found in `{csv_dir}`.")

    if csv_paths and st.button("Run Ingest", type="primary"):
        st.subheader("Ingest Log")
        total_new = total_updated = total_unchanged = 0

        for csv_path in csv_paths:
            try:
                rows = ingest_module.parse_csv(csv_path)
                new, updated, unchanged = db.upsert_claims(conn, rows, dry_run=dry_run)
                total_new       += new
                total_updated   += updated
                total_unchanged += unchanged
                if dry_run:
                    st.write(
                        f"🔍 `{Path(csv_path).name}` — "
                        f"**{new} new** (est.) | {updated} updated (est.) | {unchanged} unchanged"
                    )
                else:
                    st.write(
                        f"✅ `{Path(csv_path).name}` — "
                        f"**{new} new** | {updated} updated | {unchanged} unchanged"
                    )
            except Exception as e:
                st.error(f"❌ `{Path(csv_path).name}`: {e}")

        st.divider()
        if dry_run:
            st.info(
                f"Dry run: **{total_new} new** (est.), {total_updated} updated (est.), "
                f"{total_unchanged} unchanged. DB not modified."
            )
        else:
            total = db.get_total_record_count(conn)
            st.success(
                f"Done — **{total_new} new**, {total_updated} updated, "
                f"{total_unchanged} unchanged. "
                f"Database total: **{total:,} records**."
            )
            # Clear caches so Dashboard reflects new data
            st.cache_data.clear()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════

elif page == "Dashboard":
    st.title("Dashboard")

    stats = db.query_total_stats(conn)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Admissions",  f"{stats['admissions']:,}")
    c2.metric("Total Approved",    fmt_inr(stats["total_approved"]))
    c3.metric("Total Paid",        fmt_inr(stats["total_paid"]))
    c4.metric("Received (−TDS)",   fmt_inr(stats["total_received"]))
    c5.metric("Outstanding",       fmt_inr(stats["outstanding"]))

    st.divider()

    monthly = db.query_monthly_summary(conn)
    if not monthly.empty:
        col_a, col_b = st.columns([2, 1])

        with col_a:
            st.subheader("Monthly: Approved vs Paid")
            fig = px.bar(
                monthly,
                x="month",
                y=["total_approved", "total_paid"],
                barmode="group",
                labels={"value": "Amount (₹)", "month": "Month", "variable": ""},
                color_discrete_map={
                    "total_approved": "#2196F3",
                    "total_paid":     "#4CAF50",
                },
            )
            fig.update_layout(legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ))
            st.plotly_chart(fig, width='stretch')

        with col_b:
            st.subheader("Status Breakdown")
            status_df = db.query_status_breakdown(conn)
            if not status_df.empty:
                fig2 = px.pie(
                    status_df,
                    names="status",
                    values="count",
                    hole=0.4,
                )
                fig2.update_traces(textinfo="percent+label")
                st.plotly_chart(fig2, width='stretch')

    st.subheader("Recent Admissions (last 10)")
    recent = db.query_recent_admissions(conn, n=10)
    if recent.empty:
        st.info("No data yet. Go to Ingest to load CSV files.")
    else:
        st.dataframe(recent, width='stretch', hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: ADMISSIONS
# ══════════════════════════════════════════════════════════════════════════════

elif page == "Admissions":
    st.title("Admissions")

    opts = db.get_filter_options(conn)

    with st.sidebar:
        st.subheader("Filters")
        date_from = st.date_input("From date", value=None)
        date_to   = st.date_input("To date",   value=None)
        pol_year  = st.selectbox("Policy Year", ["(all)"] + opts["policy_year"])
        status    = st.selectbox("Status",      ["(all)"] + opts["status"])
        speciality = st.selectbox("Speciality", ["(all)"] + opts["pkg_speciality_name"])

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
        # Pagination
        PAGE_SIZE = 50
        total_pages = max(1, (len(df) - 1) // PAGE_SIZE + 1)
        page_num = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1)
        start = (page_num - 1) * PAGE_SIZE
        page_df = df.iloc[start : start + PAGE_SIZE]

        st.dataframe(page_df, width='stretch', hide_index=True)

        # TID detail expander
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


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: REPORTS
# ══════════════════════════════════════════════════════════════════════════════

elif page == "Reports":
    st.title("Reports")

    report_type = st.selectbox(
        "Report type",
        ["Admission Summary", "Monthly Summary", "FY Summary", "FY Admission Detail",
         "Month Admission Detail", "Raw Export"],
    )

    # Sidebar filter variables — initialized before conditional sidebar blocks
    month_pick = None
    fy_pick = "(all)"
    fy_detail_pick = None
    month_detail_pick = []

    with st.sidebar:
        st.subheader("Report Filters")
        opts = db.get_filter_options(conn)
        status_filter = st.multiselect("Status", opts["status"])

        if report_type == "Monthly Summary":
            month_pick = st.text_input("Month (YYYY-MM, blank = all)") or None
        elif report_type == "FY Summary":
            fy_df = db.query_fy_summary(conn)
            fy_options = sorted(fy_df["financial_year"].unique().tolist()) if not fy_df.empty else []
            fy_pick = st.selectbox("Financial Year", ["(all)"] + fy_options)
        elif report_type == "FY Admission Detail":
            fy_detail_options = db.get_available_fys(conn)
            fy_detail_pick = st.selectbox("Financial Year", fy_detail_options) if fy_detail_options else None
        elif report_type == "Month Admission Detail":
            month_options = db.get_available_months(conn)
            month_options.sort(reverse=True)
            month_detail_pick = st.multiselect("Month(s)", month_options, default=month_options[:1] if month_options else [])

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
            xlsx_bytes = reports.generate_report(df, title, rtype)
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
            df = db.query_fy_admission_detail(conn, fy_detail_pick)
            title = f"MAA FY Admission Detail {fy_detail_pick}"

            st.subheader(f"Preview ({min(20, len(df))} of {len(df):,} rows)")
            st.dataframe(df.head(20), width='stretch', hide_index=True)

            if not df.empty:
                xlsx_bytes = reports.generate_fy_detail_report(df, fy_detail_pick)
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
            df = db.query_month_admission_detail(conn, month_detail_pick)
            label = ", ".join(sorted(month_detail_pick))
            title = f"MAA Month Admission Detail {label}"

            st.subheader(f"Preview ({min(20, len(df))} of {len(df):,} rows)")
            st.dataframe(df.head(20), width='stretch', hide_index=True)

            if not df.empty:
                xlsx_bytes = reports.generate_month_detail_report(df, label)
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
        df = db.query_all_claims(conn)
        if status_filter:
            df = df[df["status"].isin(status_filter)]
        rtype = "raw_export"
        title = "MAA Raw Export"

        st.subheader(f"Preview ({min(20, len(df))} of {len(df):,} rows)")
        st.dataframe(df.head(20), width='stretch', hide_index=True)

        if not df.empty:
            xlsx_bytes = reports.generate_report(df, title, rtype)
            st.download_button(
                label=f"Download {report_type} as Excel",
                data=xlsx_bytes,
                file_name=f"MAA_{rtype}_{date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            st.info("No data for this selection.")
