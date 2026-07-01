# ui/doctor_summary.py
import plotly.express as px
import streamlit as st

import db
import reports
from utils import fmt_inr, load_doctors

_STATUS_COLORS = {
    "Claim Paid":     "#2e7d32",
    "Claim Approved": "#e65100",
    "Claim Raised":   "#b8860b",
    "Query Raised":   "#b8860b",
    "Rejected":       "#9e9e9e",
    "Non-MAA":        "#9e9e9e",
}


def render(conn) -> None:
    doctors = load_doctors()

    with st.sidebar:
        st.subheader("Doctor")
        selected_doctor = st.selectbox("Doctor", list(doctors.keys()), key="dsum_doctor")

    st.title(f"Doctor Summary — {selected_doctor}")

    df = db.get_doctor_all_entries(conn, selected_doctor)
    if df.empty:
        st.info("No entries recorded yet for this doctor.")
        return

    totals = db.get_doctor_lifetime_totals(conn, selected_doctor)
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Entries",              f"{totals['entries']} ({totals['paid_entries']} paid)")
    m2.metric("Total MAA Payment",    fmt_inr(totals["total_maa_payment"]))
    m3.metric("Total Doctor Share",   fmt_inr(totals["total_doctor_share"]))
    m4.metric("Total Hosp Share",     fmt_inr(totals["total_hospital_share"]))
    m5.metric("Outstanding (Unpaid)", fmt_inr(totals["outstanding_doctor_share"]))

    st.divider()
    col_a, col_b = st.columns([2, 1])

    with col_a:
        st.subheader("Monthly Trend — Doctor vs Hospital Share")
        monthly = (
            df.groupby("month")[["doctor_share", "hospital_share"]]
            .sum()
            .reset_index()
            .sort_values("month")
        )
        monthly["month_label"] = monthly["month"].apply(reports.month_label)
        fig = px.bar(
            monthly, x="month_label",
            y=["doctor_share", "hospital_share"],
            barmode="group",
            labels={"value": "Amount (₹)", "month_label": "Month", "variable": ""},
            color_discrete_map={"doctor_share": "#1F3864", "hospital_share": "#00695C"},
        )
        fig.update_layout(legend=dict(
            orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
        ))
        st.plotly_chart(fig, width="stretch")

    with col_b:
        st.subheader("Doctor Share: Paid vs Unpaid")
        paid_amt   = float(df.loc[df["doctor_paid"] == 1, "doctor_share"].fillna(0).sum())
        unpaid_amt = float(df.loc[df["doctor_paid"] == 0, "doctor_share"].fillna(0).sum())
        if paid_amt + unpaid_amt > 0:
            fig2 = px.pie(
                names=["Paid", "Unpaid"], values=[paid_amt, unpaid_amt], hole=0.4,
                color=["Paid", "Unpaid"],
                color_discrete_map={"Paid": "#2e7d32", "Unpaid": "#b8860b"},
            )
            fig2.update_traces(textinfo="percent+label")
            st.plotly_chart(fig2, width="stretch")
        else:
            st.caption("No doctor share recorded yet.")

    st.subheader("Entries by MAA Status")
    status_counts = (
        df["maa_status"].apply(lambda s: s or "Non-MAA")
        .value_counts().reset_index()
    )
    status_counts.columns = ["status", "count"]
    fig3 = px.bar(
        status_counts, x="status", y="count", color="status",
        color_discrete_map=_STATUS_COLORS,
    )
    fig3.update_layout(showlegend=False, xaxis_title="", yaxis_title="Entries")
    st.plotly_chart(fig3, width="stretch")

    st.divider()
    st.subheader("All Entries")
    st.dataframe(
        df[["month", "patient_name", "admission_date", "total_ex", "maa_payment",
            "doctor_share", "hospital_share", "maa_status", "doctor_paid"]].rename(columns={
            "month": "Month", "patient_name": "Patient", "admission_date": "Admission Date",
            "total_ex": "Total Ex", "maa_payment": "MAA Payment", "doctor_share": "Doctor Share",
            "hospital_share": "Hospital Share", "maa_status": "MAA Status", "doctor_paid": "Paid",
        }),
        hide_index=True, width="stretch",
    )
