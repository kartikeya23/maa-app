# ui/doctor_share.py
import re
from datetime import date as _date

import pandas as pd
import streamlit as st

import db
import reports
from utils import fmt_inr


def _link_and_infer_status(conn, row_id: int, tid: str) -> None:
    inferred = db.infer_maa_status(conn, tid)
    updates: dict = {"tid": tid}
    if inferred:
        updates["maa_status"] = inferred
    adm_row = conn.execute(
        "SELECT date_of_admission FROM claims WHERE tid = ? LIMIT 1", (tid,)
    ).fetchone()
    if adm_row and adm_row[0]:
        updates["admission_date"] = adm_row[0]
    db.update_doctor_expense(conn, row_id, updates)


@st.dialog("Entry Details", width="large")
def _entry_detail_dialog(row_id: int, conn):
    raw_rows = pd.read_sql_query(
        "SELECT * FROM doctor_expenses WHERE id = ?", conn, params=[row_id]
    )
    if raw_rows.empty:
        st.error("Entry not found.")
        return
    r = raw_rows.iloc[0]

    comp_df = db.get_doctor_expenses(conn, r["month"])
    comp    = comp_df[comp_df["id"] == row_id]
    maa_pmt = (
        float(comp["maa_payment"].iloc[0])
        if not comp.empty and pd.notna(comp["maa_payment"].iloc[0])
        else None
    )

    tid_badge = f" · TID `{r['tid']}`" if pd.notna(r["tid"]) else " · *No MAA link*"
    st.subheader(r["patient_name"])
    _updated = str(r.get("updated_at", "") or "")[:10]
    st.caption(f"Month: {r['month']} · Adm: {r['admission_date']} · Updated: {_updated}{tid_badge}")

    tab_edit, tab_maa = st.tabs(["✏️ Edit Entry", "🏥 MAA Claim"])

    with tab_edit:
        n1, n2 = st.columns(2)
        new_patient_name = n1.text_input(
            "Patient Name", value=str(r["patient_name"] or ""), key="d_patient_name"
        )
        _adm_default = None
        try:
            _adm_default = _date.fromisoformat(str(r["admission_date"]))
        except Exception:
            pass
        new_admission_date = n2.date_input(
            "Admission Date", value=_adm_default, key="d_admission_date"
        )

        c1, c2, c3 = st.columns(3)
        new_hosp     = c1.number_input("Hospital Ex ₹",  value=float(r["hosp_ex"] or 0),     min_value=0.0, step=100.0, key="d_hosp",     help="Hospital expenses paid by the hospital (e.g. implants, consumables) — deducted before calculating doctor share.")
        new_pharma   = c2.number_input("Pharmacy Ex ₹",  value=float(r["pharma_ex"] or 0),   min_value=0.0, step=100.0, key="d_pharma",   help="Pharmacy / medicine costs borne by the hospital — deducted before calculating doctor share.")
        new_dialysis = c3.number_input("Dialysis Ex ₹",  value=float(r["dialysis_ex"] or 0), min_value=0.0, step=100.0, key="d_dialysis", help="Dialysis session costs borne by the hospital — deducted before calculating doctor share.")

        d1, d2 = st.columns(2)
        new_pct      = d1.number_input("Doctor %", value=float(r["doctor_pct"] or 0.4) * 100,
                                       min_value=0.0, max_value=100.0, step=5.0, key="d_pct",
                                       help="Percentage of (MAA payment − expenses) paid to the doctor; used only when no flat override is set.") / 100.0
        new_flat_raw = d2.number_input("Flat Override ₹ (0 = use %)",
                                       value=float(r["doctor_flat"] or 0), min_value=0.0, step=500.0, key="d_flat",
                                       help="Fixed rupee amount for doctor; if set, this replaces the percentage calculation entirely.")
        new_flat = new_flat_raw if new_flat_raw > 0 else None

        maa_status_opts = ["", "Claim Paid", "Claim Approved", "Claim Raised", "Query Raised", "Rejected"]
        cur_status = r["maa_status"] or ""
        if cur_status not in maa_status_opts:
            maa_status_opts.append(cur_status)
        new_maa_status = st.selectbox("MAA Status", maa_status_opts,
                                      index=maa_status_opts.index(cur_status), key="d_maa_status",
                                      help="Current stage of the MAA claim: Claim Raised → Claim Approved → Claim Paid.")
        new_comments   = st.text_input("Comments", value=r["comments"] or "", key="d_comments",
                                       help="Any additional notes about this entry (e.g. pending documents, special arrangements).")

        with st.expander("⚙️ Advanced"):
            adv1, adv2 = st.columns(2)
            new_filing_month = adv1.text_input(
                "Filing Month (YYYY-MM)", value=str(r["month"]), key="d_filing_month",
                help="Moves this entry to a different month bucket.",
            )
            new_doctor_paid = adv2.checkbox(
                "Paid to Doctor", value=bool(r["doctor_paid"]), key="d_doctor_paid",
                help="Tick once the doctor's share has been physically paid out.",
            )
            new_pay_month = st.text_input(
                "Payment Month (YYYY-MM)", value=r["doctor_payment_month"] or "",
                key="d_pay_month", help="Month in which payment was made to the doctor.",
            )

        tot_ex = new_hosp + new_pharma + new_dialysis
        share  = new_flat if new_flat else (
            new_pct * ((maa_pmt or 0) - tot_ex) if maa_pmt is not None else None
        )
        hosp_s = (maa_pmt - (share or 0) - tot_ex) if maa_pmt is not None and share is not None else None

        p1, p2, p3 = st.columns(3)
        p1.metric("Total Ex",       fmt_inr(tot_ex))
        p2.metric("Doctor Share",   fmt_inr(share)  if share  is not None else "—")
        p3.metric("Hospital Share", fmt_inr(hosp_s) if hosp_s is not None else "—")
        if share is not None and share < 0:
            st.warning("Doctor share is negative — expenses exceed MAA payment.")

        if st.button("💾 Save Changes", type="primary", key="d_save"):
            db.update_doctor_expense(conn, row_id, {
                "patient_name":         new_patient_name or None,
                "admission_date":       str(new_admission_date) if new_admission_date else None,
                "hosp_ex":              new_hosp,
                "pharma_ex":            new_pharma,
                "dialysis_ex":          new_dialysis,
                "doctor_pct":           new_pct,
                "doctor_flat":          new_flat,
                "comments":             new_comments or None,
                "maa_status":           new_maa_status or None,
                "month":                new_filing_month or None,
                "doctor_paid":          1 if new_doctor_paid else 0,
                "doctor_payment_month": new_pay_month or None,
            })
            for _k in ["d_patient_name", "d_admission_date", "d_hosp", "d_pharma",
                        "d_dialysis", "d_pct", "d_flat", "d_maa_status",
                        "d_filing_month", "d_doctor_paid", "d_pay_month", "d_comments"]:
                st.session_state.pop(_k, None)
            st.success("Saved.")

    with tab_maa:
        tid = r["tid"]
        if pd.notna(tid) and tid:
            st.success(f"Linked to TID **{tid}**")
            pkgs = db.query_packages_for_tid(conn, tid)
            if not pkgs.empty:
                st.dataframe(
                    pkgs[["pkg_name", "pkg_speciality_name", "approved_amount", "paid_amount",
                           "status", "payment_date"]].rename(columns={
                        "pkg_name": "Package", "pkg_speciality_name": "Speciality",
                        "approved_amount": "Approved ₹", "paid_amount": "Paid ₹",
                        "status": "Status", "payment_date": "Payment Date",
                    }),
                    hide_index=True, width='stretch',
                )
                _bill_total = pkgs["pkg_rate"].sum()
                _paid_amt   = pkgs.loc[pkgs["status"].str.contains("paid", case=False, na=False), "approved_amount"].fillna(0).sum()
                t1, t2, t3, t4 = st.columns(4)
                t1.metric("Bill Total",      fmt_inr(_bill_total))
                t2.metric("Total Approved",  fmt_inr(pkgs["approved_amount"].sum()))
                t3.metric("Total Paid",      fmt_inr(_paid_amt))
                t4.metric("Received (−TDS)", fmt_inr(_paid_amt * 0.9))
            st.divider()
            _ba1, _ba2 = st.columns(2)
            def _auto_detect_status():
                inferred = db.infer_maa_status(conn, tid)
                if inferred:
                    db.update_doctor_expense(conn, row_id, {"maa_status": inferred})
            _ba1.button("🔄 Auto-detect Status", key="d_autodetect", on_click=_auto_detect_status)
            _ba2.button(
                "🔗 Unlink TID", key="d_unlink",
                on_click=db.update_doctor_expense,
                args=(conn, row_id, {"tid": None}),
            )
        else:
            st.info("No MAA claim linked. Search below to find and link a matching admission.")
            src_name   = st.text_input("Search by name", value=str(r["patient_name"] or ""), key="d_src",
                                       help="Type the patient's name as it appears in the MAA portal to find their TID.")
            src_expand = st.checkbox("Expand search to ±1 month", key="d_expand",
                                     help="Also search admissions from the month before and after the filing month when no exact-month match is found.")
            if src_name:
                candidates = db.search_claims_for_matching(conn, src_name, r["month"], expand=src_expand)
                if candidates:
                    lbl = [
                        f"{c['patient_name']} | {c['tid']} | Adm: {c['date_of_admission']}"
                        f" | {fmt_inr(c['maa_paid'] or 0)} | {c['status']}"
                        for c in candidates
                    ]
                    idx = st.radio("Matching admissions", range(len(lbl)),
                                   format_func=lambda i: lbl[i], key="d_cand")
                    chosen = candidates[idx]
                    st.button(
                        "🔗 Link to this admission", type="primary", key="d_link",
                        on_click=_link_and_infer_status,
                        args=(conn, row_id, chosen["tid"]),
                    )
                else:
                    st.warning("No unlinked admissions found. Try expanding to ±1 month.")


@st.dialog("Confirm Delete", width="small")
def _confirm_delete_dialog(ids: list[int], conn):
    n = len(ids)
    st.warning(f"Permanently delete **{n}** {'entry' if n == 1 else 'entries'}?")
    st.caption("This cannot be undone.")
    c1, c2 = st.columns(2)
    if c1.button("🗑 Delete", type="primary", width="stretch", key="dlg_del_confirm"):
        for id_ in ids:
            db.delete_doctor_expense(conn, int(id_))
        st.rerun()
    if c2.button("Cancel", width="stretch", key="dlg_del_cancel"):
        st.rerun()


def render(conn) -> None:
    st.title("Doctor Share — Dr. Kavesh")

    available_months = db.get_doctor_expense_months(conn)

    with st.sidebar:
        st.subheader("Filters")
        months_asc = sorted(available_months)
        if months_asc:
            _month_mode = st.radio(
                "Month selection", ["Single", "Range"], horizontal=True, key="ds_month_mode",
            )
            if _month_mode == "Single":
                _single = st.selectbox(
                    "Month", months_asc, index=len(months_asc) - 1,
                    format_func=reports.month_label, key="ds_single_month",
                )
                selected_months: list[str] = [_single]
            else:
                from_month = st.selectbox(
                    "From", months_asc, index=len(months_asc) - 1,
                    format_func=reports.month_label, key="ds_from_month",
                )
                to_month = st.selectbox(
                    "To", months_asc, index=len(months_asc) - 1,
                    format_func=reports.month_label, key="ds_to_month",
                )
                if from_month > to_month:
                    st.warning("'From' must be ≤ 'To'.")
                    selected_months = []
                else:
                    selected_months = [m for m in months_asc if from_month <= m <= to_month]
        else:
            selected_months = []
        status_filter = st.multiselect(
            "MAA Status",
            ["Claim Paid", "Claim Approved", "Claim Raised", "Query Raised", "Rejected", "Non-MAA"],
            help="Filter entries by their MAA claim stage; 'Non-MAA' shows entries with no linked claim.",
        )
        paid_filter = st.selectbox("Doctor Paid", ["All", "Paid", "Unpaid"],
                                   help="Filter by whether the doctor's share has been paid out.")

    full_df = db.get_doctor_expenses(conn, selected_months) if selected_months else pd.DataFrame()

    df = full_df.copy()
    if not df.empty:
        if status_filter:
            masks = []
            for sf in status_filter:
                if sf == "Non-MAA":
                    masks.append(df["tid"].isna())
                else:
                    masks.append(df["maa_status"].fillna("") == sf)
            combined = masks[0]
            for m in masks[1:]:
                combined |= m
            df = df[combined]
        if paid_filter == "Paid":
            df = df[df["doctor_paid"] == 1]
        elif paid_filter == "Unpaid":
            df = df[df["doctor_paid"] == 0]

    with st.expander("➕ Add Entry", expanded=not available_months):
        if available_months:
            _NEW_MONTH_SENTINEL = "➕ New month…"
            _month_opts = available_months + [_NEW_MONTH_SENTINEL]
            _month_sel = st.selectbox(
                "Add to month", _month_opts, key="add_entry_month",
                help="Which month this entry belongs to",
            )
            if _month_sel == _NEW_MONTH_SENTINEL:
                add_month = st.text_input(
                    "New month (YYYY-MM)", key="add_entry_new_month", placeholder="2025-06"
                )
            else:
                add_month = _month_sel
        else:
            add_month = st.text_input("Month (YYYY-MM)", key="add_entry_month", placeholder="2025-06")

        entry_type = st.radio("Patient type", ["MAA Patient", "Non-MAA Patient"], horizontal=True,
                              help="MAA patients have a government insurance claim; Non-MAA patients are self-pay or covered by other insurance.")

        if entry_type == "MAA Patient":
            search_name = st.text_input("Search patient name (from physical bill)", key="ae_search",
                                        help="Enter the name as written on the hospital bill; the system will look for a matching TID in the MAA portal data.")
            candidates: list[dict] = []
            if search_name and add_month:
                candidates = db.search_claims_for_matching(conn, search_name, add_month, expand=False)
                if not candidates:
                    if st.checkbox("No results — expand search to ±1 month?", key="expand_search"):
                        candidates = db.search_claims_for_matching(conn, search_name, add_month, expand=True)

            if candidates:
                cand_labels = [
                    f"{c['patient_name']} | TID: {c['tid']} | Adm: {c['date_of_admission']}"
                    f" | MAA: {fmt_inr(c['maa_paid'] or 0)} | {c['status']}"
                    for c in candidates
                ]
                selected_idx = st.radio(
                    "Select matching admission", range(len(cand_labels)),
                    format_func=lambda i: cand_labels[i],
                )
                chosen = candidates[selected_idx]
                st.info(f"Selected: **{chosen['patient_name']}** (TID: {chosen['tid']}, Adm: {chosen['date_of_admission']})")

                c1, c2, c3 = st.columns(3)
                hosp_ex     = c1.number_input("Hospital Ex ₹",  min_value=0, value=0, step=100, format="%d", key="ae_hosp",
                                              help="Hospital expenses borne by the hospital (e.g. implants, consumables) — deducted before calculating doctor share.")
                pharma_ex   = c2.number_input("Pharmacy Ex ₹",  min_value=0, value=0, step=100, format="%d", key="ae_pharma",
                                              help="Pharmacy / medicine costs borne by the hospital — deducted before calculating doctor share.")
                dialysis_ex = c3.number_input("Dialysis Ex ₹",  min_value=0, value=0, step=100, format="%d", key="ae_dialysis",
                                              help="Dialysis session costs borne by the hospital — deducted before calculating doctor share.")
                doctor_pct_input = st.number_input(
                    "Doctor % (default 40%)", min_value=0.0, max_value=100.0,
                    value=40.0, step=5.0, key="ae_pct",
                    help="Percentage of (MAA payment − expenses) that goes to the doctor; overridden if a flat amount is entered below.",
                ) / 100.0
                doctor_flat_raw = st.number_input("Flat override ₹ (0 = use %)", min_value=0, value=0, step=500, format="%d", key="ae_flat",
                                                  help="Fixed rupee amount for the doctor; leave 0 to use the percentage above.")
                comments_input  = st.text_input("Comments", key="ae_comments",
                                                help="Any additional notes about this entry (e.g. pending documents, special arrangements).")

                flat_val         = doctor_flat_raw if doctor_flat_raw > 0 else None
                total_ex_preview = hosp_ex + pharma_ex + dialysis_ex
                maa_preview      = chosen["maa_paid"] or 0
                share_preview    = flat_val if flat_val else doctor_pct_input * (maa_preview - total_ex_preview)
                st.caption(
                    f"Preview → Total Ex: {fmt_inr(total_ex_preview)} | "
                    f"MAA Payment: {fmt_inr(maa_preview)} | "
                    f"Doctor Share: {fmt_inr(share_preview)}"
                )
                if share_preview < 0:
                    st.warning("Doctor share is negative — expenses exceed MAA payment.")

                if st.button("Save Entry", type="primary", key="ae_save_maa"):
                    db.save_doctor_expense(
                        conn, month=add_month,
                        patient_name=chosen["patient_name"],
                        admission_date=chosen["date_of_admission"],
                        hosp_ex=hosp_ex, pharma_ex=pharma_ex, dialysis_ex=dialysis_ex,
                        doctor_pct=doctor_pct_input, doctor_flat=flat_val,
                        comments=comments_input or None,
                        maa_status=db.infer_maa_status(conn, chosen["tid"]), tid=chosen["tid"],
                    )
                    st.success(f"Added entry for {chosen['patient_name']}.")
                    for _k in ["ae_search", "ae_hosp", "ae_pharma", "ae_dialysis",
                               "ae_pct", "ae_flat", "ae_comments", "expand_search"]:
                        st.session_state.pop(_k, None)
                    st.rerun()
            elif search_name:
                st.warning("No matching admissions found. Try a partial name or expand to ±1 month.")

        else:  # Non-MAA
            nm_name     = st.text_input("Patient Name", key="nm_name")
            nm_date     = st.date_input("Admission Date", key="nm_date")
            c1, c2, c3  = st.columns(3)
            nm_hosp     = c1.number_input("Hospital Ex ₹",  min_value=0, value=0, step=100, format="%d", key="nm_hosp",
                                          help="Hospital expenses borne by the hospital — recorded for reference, not used in any automatic calculation.")
            nm_pharma   = c2.number_input("Pharmacy Ex ₹",  min_value=0, value=0, step=100, format="%d", key="nm_pharma",
                                          help="Pharmacy / medicine costs borne by the hospital — recorded for reference.")
            nm_dialysis = c3.number_input("Dialysis Ex ₹",  min_value=0, value=0, step=100, format="%d", key="nm_dialysis",
                                          help="Dialysis session costs borne by the hospital — recorded for reference.")
            nm_share    = st.number_input("Doctor Share ₹", min_value=0, value=0, step=500, format="%d", key="nm_share",
                                          help="The fixed rupee amount agreed as the doctor's fee for this non-MAA patient.")
            nm_comments = st.text_input("Comments", key="nm_comments",
                                        help="Any additional notes about this entry (e.g. pending documents, special arrangements).")

            if st.button("Save Entry", type="primary", key="nm_save"):
                if not nm_name:
                    st.error("Patient name is required.")
                elif nm_share <= 0:
                    st.error("Doctor share must be greater than 0.")
                else:
                    db.save_doctor_expense(
                        conn, month=add_month, patient_name=nm_name,
                        admission_date=str(nm_date),
                        hosp_ex=nm_hosp, pharma_ex=nm_pharma, dialysis_ex=nm_dialysis,
                        doctor_flat=nm_share, comments=nm_comments or None, tid=None,
                    )
                    st.success(f"Added non-MAA entry for {nm_name}.")
                    for _k in ["nm_name", "nm_date", "nm_hosp", "nm_pharma",
                               "nm_dialysis", "nm_share", "nm_comments"]:
                        st.session_state.pop(_k, None)
                    st.rerun()

    if not selected_months:
        st.info("Select at least one month from the sidebar.")
    elif full_df.empty:
        st.info("No entries for the selected month(s). Use '➕ Add Entry' above.")
    else:
        paid_ct    = int((df["doctor_paid"] == 1).sum()) if not df.empty else 0
        unpaid_ct  = int((df["doctor_paid"] == 0).sum()) if not df.empty else 0
        non_maa_ct = int(df["tid"].isna().sum())          if not df.empty else 0
        filter_note = f" (filtered from {len(full_df)})" if (status_filter or paid_filter != "All") else ""
        _cnt_col, _ref_col = st.columns([8, 1])
        _cnt_col.markdown(
            f"**{len(df)}** {'entry' if len(df) == 1 else 'entries'}{filter_note}"
            f"&ensp;·&ensp;{paid_ct} paid&ensp;·&ensp;{unpaid_ct} unpaid&ensp;·&ensp;{non_maa_ct} non-MAA"
        )
        if _ref_col.button("🔄", key="ds_refresh", help="Refresh entries"):
            st.rerun()

        df_r = df.reset_index(drop=True)
        _multi = len(selected_months) > 1
        _PAGE_SIZE = 25
        _total_pages = max(1, (len(df_r) - 1) // _PAGE_SIZE + 1)

        if "ds_page_num" not in st.session_state:
            st.session_state["ds_page_num"] = 1
        ds_page = max(1, min(st.session_state["ds_page_num"], _total_pages))
        _start = (ds_page - 1) * _PAGE_SIZE
        _page_rows = df_r.iloc[_start : _start + _PAGE_SIZE]

        if _multi:
            _COLS = [0.22, 0.85, 2.9, 1.25, 1.15, 1.15, 1.15, 2.4, 0.6]
            _hdrs = ["", "Month", "Patient", "Date", "Total Ex", "MAA Pmt", "Dr Share", "Status", ""]
        else:
            _COLS = [0.22, 3.2, 1.25, 1.15, 1.15, 1.15, 2.4, 0.6]
            _hdrs = ["", "Patient", "Date", "Total Ex", "MAA Pmt", "Dr Share", "Status", ""]

        _hrow = st.columns(_COLS)
        for _hi, _hl in enumerate(_hdrs[1:], start=1):
            if _hl:
                _hrow[_hi].markdown(
                    f"<p style='margin:0;padding:2px 0 4px;color:#888;"
                    f"font-size:0.78rem;font-weight:600;letter-spacing:0.03em'>{_hl}</p>",
                    unsafe_allow_html=True,
                )
        st.divider()

        def _md_money(v):
            if pd.isna(v):
                return "<div style='text-align:right;color:#bbb;font-size:0.9rem'>—</div>"
            return f"<div style='text-align:right;font-size:0.9rem'>₹{v:,.0f}</div>"

        _STATUS_STYLE = {
            "Claim Paid":     "color:#2e7d32;font-weight:600",
            "Claim Approved": "color:#e65100",
            "Claim Raised":   "color:#e65100",
            "Query Raised":   "color:#b71c1c",
            "Rejected":       "color:#9e9e9e",
            "Non-MAA":        "color:#9e9e9e",
        }

        for _, _row in _page_rows.iterrows():
            _rid = int(_row["id"])
            _c = st.columns(_COLS)
            _c[0].checkbox("Select", key=f"chk_{_rid}", label_visibility="collapsed")
            _ci = 1
            if _multi:
                _c[_ci].markdown(
                    f"<p style='margin:0;font-size:0.8rem;color:#999;padding-top:6px'>{_row['month']}</p>",
                    unsafe_allow_html=True,
                )
                _ci += 1
            if _c[_ci].button(_row["patient_name"] or "—", key=f"btn_{_rid}", width="stretch"):
                _entry_detail_dialog(_rid, conn)
            _ci += 1
            _c[_ci].markdown(
                f"<p style='margin:0;font-size:0.9rem;padding-top:6px'>{_row['admission_date'] or '—'}</p>",
                unsafe_allow_html=True,
            ); _ci += 1
            _c[_ci].markdown(_md_money(_row["total_ex"]), unsafe_allow_html=True); _ci += 1
            _c[_ci].markdown(_md_money(_row["maa_payment"]), unsafe_allow_html=True); _ci += 1
            _c[_ci].markdown(_md_money(_row["doctor_share"]), unsafe_allow_html=True); _ci += 1
            _status_val = _row["maa_status"] or "Non-MAA"
            _sstyle = _STATUS_STYLE.get(_status_val, "color:#333")
            _c[_ci].markdown(
                f"<p style='margin:0;font-size:0.85rem;padding-top:6px;{_sstyle}'>{_status_val}</p>",
                unsafe_allow_html=True,
            ); _ci += 1
            if _row["doctor_paid"] == 1:
                _pay_mo = _row.get("doctor_payment_month") or ""
                _pay_mo_html = (
                    f"<div style='font-size:0.72rem;color:#999;line-height:1.2'>{_pay_mo}</div>"
                    if _pay_mo else ""
                )
                _c[_ci].markdown(
                    f"<div style='padding-top:4px;font-size:1rem'>🟢</div>{_pay_mo_html}",
                    unsafe_allow_html=True,
                )
            elif _status_val == "Claim Paid":
                _c[_ci].markdown("<div style='padding-top:4px;font-size:1rem'>🟡</div>", unsafe_allow_html=True)

        st.divider()

        if _total_pages > 1:
            _pc1, _pc2, _pc3 = st.columns([1, 4, 1])
            if _pc1.button("← Prev", disabled=(ds_page <= 1), key="ds_prev", width="stretch"):
                st.session_state["ds_page_num"] = ds_page - 1
                st.rerun()
            _row_end = min(_start + _PAGE_SIZE, len(df_r))
            _pc2.markdown(
                f"<div style='text-align:center;padding-top:6px;color:#666;font-size:0.9rem'>"
                f"Page {ds_page} of {_total_pages} &ensp;·&ensp; rows {_start + 1}–{_row_end} of {len(df_r)}"
                f"</div>",
                unsafe_allow_html=True,
            )
            if _pc3.button("Next →", disabled=(ds_page >= _total_pages), key="ds_next", width="stretch"):
                st.session_state["ds_page_num"] = ds_page + 1
                st.rerun()

        selected_ids = [
            int(_row["id"])
            for _, _row in df_r.iterrows()
            if st.session_state.get(f"chk_{int(_row['id'])}", False)
        ]

        if selected_ids:
            n = len(selected_ids)
            _ba1, _ba2, _ba3, _ba4 = st.columns([2.5, 1.5, 2.5, 1])

            with _ba1:
                default_pay = selected_months[-1] if len(selected_months) == 1 else ""
                pay_month_input = st.text_input(
                    "Payment month (YYYY-MM)", value=default_pay,
                    placeholder="YYYY-MM", key="pay_month_input",
                    help="The month in which the doctor's share was actually paid out (may differ from filing month).",
                )
                if st.button(f"✔ Mark {n} Paid", width="stretch", key="bulk_mark_paid"):
                    if pay_month_input:
                        db.mark_doctor_paid(conn, [int(i) for i in selected_ids], pay_month_input)
                        st.success(f"Marked {n} row(s) paid ({pay_month_input}).")
                        st.rerun()
                    else:
                        st.error("Enter a payment month first.")

            with _ba2:
                st.write("")
                st.write("")
                st.write("")
                if st.button(f"✖ Unmark {n}", width="stretch", key="bulk_unmark_paid",
                             help=f"Unmark {n} selected {'entry' if n==1 else 'entries'} as paid"):
                    db.unmark_doctor_paid(conn, [int(i) for i in selected_ids])
                    st.success(f"Unmarked {n} row(s).")
                    st.rerun()

            with _ba3:
                move_month_input = st.text_input(
                    "Move to month (YYYY-MM)", placeholder="YYYY-MM", key="move_month_input",
                    help="Reassign the selected entries to a different filing month (e.g. to correct a wrong month).",
                )
                if st.button(f"📅 Change Month ({n})", width="stretch", key="bulk_change_month"):
                    if not move_month_input or not re.fullmatch(r"\d{4}-\d{2}", move_month_input):
                        st.error("Enter a valid month (YYYY-MM) before changing.")
                    else:
                        for _id in selected_ids:
                            db.update_doctor_expense(conn, int(_id), {"month": move_month_input})
                        st.success(f"Moved {n} row(s) to {move_month_input}.")
                        st.rerun()

            with _ba4:
                st.write("")
                st.write("")
                st.write("")
                if st.button(f"🗑 {n}", type="secondary", width="stretch",
                             help=f"Delete {n} selected {'entry' if n==1 else 'entries'}",
                             key="bulk_delete"):
                    _confirm_delete_dialog([int(i) for i in selected_ids], conn)
        else:
            st.caption("Click a patient name to open · check rows for bulk actions")

        st.divider()
        m1, m2, m3, m4, m5 = st.columns(5)
        total_paid_ct = int((full_df["doctor_paid"] == 1).sum())
        m1.metric("Total Entries",      f"{len(full_df)} ({total_paid_ct} paid)")
        m2.metric("Total MAA Payment",  fmt_inr(full_df["maa_payment"].fillna(0).sum()))
        m3.metric("Total Doctor Share", fmt_inr(full_df["doctor_share"].fillna(0).sum()))
        m4.metric("Total Hosp Share",   fmt_inr(full_df["hospital_share"].fillna(0).sum()))
        m5.metric("Total Expenses",     fmt_inr(full_df["total_ex"].sum()))

    if not full_df.empty:
        st.divider()
        _sorted_months = sorted(selected_months)
        month_label = " · ".join(reports.month_label(m) for m in _sorted_months)
        _sheet_label = reports.compact_month_range(_sorted_months)
        col_int, col_doc = st.columns(2)
        with col_int:
            st.download_button(
                label=f"Download Internal Export — {month_label}",
                data=reports.generate_doctor_internal(full_df, _sheet_label),
                file_name=f"DoctorShare_Internal_{'_'.join(_sorted_months)}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        with col_doc:
            st.download_button(
                label=f"Download Doctor Copy — {month_label}",
                data=reports.generate_doctor_copy(full_df, _sheet_label),
                file_name=f"DoctorShare_DrKavesh_{'_'.join(_sorted_months)}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
