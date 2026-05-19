# pages/dashboard.py
import plotly.express as px
import streamlit as st

import db
from utils import fmt_inr


def render(conn) -> None:
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
