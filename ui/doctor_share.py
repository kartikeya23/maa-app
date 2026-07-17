# ui/doctor_share.py
import re
from datetime import date as _date

import pandas as pd
import streamlit as st
import streamlit.components.v1 as _components

import db
import reports
from utils import fmt_inr, load_doctors

DOCTORS: dict[str, float] = load_doctors()

_MONTH_RE = re.compile(r"\d{4}-(0[1-9]|1[0-2])")


def _valid_month(s: str | None) -> bool:
    return bool(s) and bool(_MONTH_RE.fullmatch(s))


def _clear_row_selection(ids: list[int]) -> None:
    """Uncheck the given rows' 'Select' checkboxes so a completed bulk action
    doesn't leave the action bar open with the same rows still selected."""
    for id_ in ids:
        st.session_state.pop(f"chk_{id_}", None)


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
    # Use row_id-prefixed keys so each patient has its own isolated session
    # state namespace. This prevents stale values from a previously-open dialog
    # bleeding into the next one regardless of how the previous dialog was closed.
    _p = f"d{row_id}_"

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

    tab_edit, tab_maa, tab_history = st.tabs(["✏️ Edit Entry", "🏥 MAA Claim", "🕘 History"])

    with tab_edit:
        n1, n2 = st.columns(2)
        new_patient_name = n1.text_input(
            "Patient Name", value=str(r["patient_name"] or ""), key=f"{_p}pname"
        )
        _adm_default = None
        try:
            _adm_default = _date.fromisoformat(str(r["admission_date"]))
        except Exception:
            pass
        new_admission_date = n2.date_input(
            "Admission Date", value=_adm_default, key=f"{_p}adm"
        )

        c1, c2, c3 = st.columns(3)
        new_hosp     = c1.number_input("Hospital Ex ₹",  value=int(r["hosp_ex"] or 0),     min_value=0, step=100, format="%d", key=f"{_p}hosp",     help="Hospital expenses paid by the hospital (e.g. implants, consumables) — deducted before calculating doctor share.")
        new_pharma   = c2.number_input("Pharmacy Ex ₹",  value=int(r["pharma_ex"] or 0),   min_value=0, step=100, format="%d", key=f"{_p}pharma",   help="Pharmacy / medicine costs borne by the hospital — deducted before calculating doctor share.")
        new_dialysis = c3.number_input("Dialysis Ex ₹",  value=int(r["dialysis_ex"] or 0), min_value=0, step=100, format="%d", key=f"{_p}dialysis", help="Dialysis session costs borne by the hospital — deducted before calculating doctor share.")

        d1, d2 = st.columns(2)
        _pct_default = r["doctor_pct"] if pd.notna(r["doctor_pct"]) else 0.4
        new_pct      = d1.number_input("Doctor %", value=float(_pct_default) * 100,
                                       min_value=0.0, max_value=100.0, step=5.0, key=f"{_p}pct",
                                       help="Percentage of (MAA payment − expenses) paid to the doctor; used only when no flat override is set.") / 100.0
        new_flat_raw = d2.number_input("Flat Override ₹ (0 = use %)",
                                       value=int(r["doctor_flat"] or 0), min_value=0, step=500, format="%d", key=f"{_p}flat",
                                       help="Fixed rupee amount for doctor; if set, this replaces the percentage calculation entirely.")
        new_flat = new_flat_raw if new_flat_raw > 0 else None

        maa_status_opts = ["", "Claim Paid", "Claim Approved", "Claim Raised", "Query Raised", "Rejected"]
        cur_status = r["maa_status"] or ""
        if cur_status not in maa_status_opts:
            maa_status_opts.append(cur_status)
        new_maa_status = st.selectbox("MAA Status", maa_status_opts,
                                      index=maa_status_opts.index(cur_status), key=f"{_p}status",
                                      help="Current stage of the MAA claim: Claim Raised → Claim Approved → Claim Paid.")
        new_comments   = st.text_input("Comments", value=r["comments"] or "", key=f"{_p}comments",
                                       help="Any additional notes about this entry (e.g. pending documents, special arrangements).")

        with st.expander("⚙️ Advanced"):
            adv1, adv2 = st.columns(2)
            new_filing_month = adv1.text_input(
                "Filing Month (YYYY-MM)", value=str(r["month"]), key=f"{_p}month",
                help="Moves this entry to a different month bucket.",
            )
            new_doctor_paid = adv2.checkbox(
                "Paid to Doctor", value=bool(r["doctor_paid"]), key=f"{_p}paid",
                help="Tick once the doctor's share has been physically paid out.",
            )
            new_pay_month = st.text_input(
                "Payment Month (YYYY-MM)", value=r["doctor_payment_month"] or "",
                key=f"{_p}paymonth", help="Month in which payment was made to the doctor.",
            )
            _doctor_opts = list(DOCTORS.keys())
            _cur_doctor = r["doctor_name"] or _doctor_opts[0]
            if _cur_doctor not in _doctor_opts:
                _doctor_opts.append(_cur_doctor)
            new_doctor_name = st.selectbox(
                "Doctor", _doctor_opts, index=_doctor_opts.index(_cur_doctor), key=f"{_p}doctor",
                help="Reassign this entry to a different doctor (e.g. it was filed under the wrong one).",
            )

        tot_ex = new_hosp + new_pharma + new_dialysis
        _is_rejected = bool(new_maa_status) and "rejected" in new_maa_status.lower()
        share, hosp_s = db.compute_doctor_share(maa_pmt, tot_ex, new_pct, new_flat, _is_rejected)

        p1, p2, p3 = st.columns(3)
        p1.metric("Total Ex",       fmt_inr(tot_ex))
        p2.metric("Doctor Share",   fmt_inr(share)  if share  is not None else "—")
        p3.metric("Hospital Share", fmt_inr(hosp_s) if hosp_s is not None else "—")
        if share is not None and share < 0:
            st.warning("Doctor share is negative — expenses exceed MAA payment.")
        elif hosp_s == 0 and maa_pmt is not None and (tot_ex + (share or 0)) > maa_pmt:
            st.warning("Hospital share floored at ₹0 — expenses + doctor share exceed the MAA payment.")

        if st.button("💾 Save Changes", type="primary", key=f"{_p}save"):
            if not _valid_month(new_filing_month):
                st.error("Filing Month must be a valid YYYY-MM.")
            elif new_pay_month and not _valid_month(new_pay_month):
                st.error("Payment Month must be a valid YYYY-MM, or left blank.")
            else:
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
                    "doctor_name":          new_doctor_name,
                })
                for _sk in [k for k in st.session_state if k.startswith(_p)]:
                    st.session_state.pop(_sk, None)
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
                    st.session_state[f"{_p}status"] = inferred
            _ba1.button("🔄 Auto-detect Status", key=f"{_p}autodetect", on_click=_auto_detect_status)
            _ba2.button(
                "🔗 Unlink TID", key=f"{_p}unlink",
                on_click=db.update_doctor_expense,
                args=(conn, row_id, {"tid": None, "maa_status": None}),
            )
        else:
            st.info("No MAA claim linked. Search below to find and link a matching admission.")
            src_name   = st.text_input("Search by name", value=str(r["patient_name"] or ""), key=f"{_p}src",
                                       help="Type the patient's name as it appears in the MAA portal to find their TID.")
            src_expand = st.checkbox("Expand search to ±1 month", key=f"{_p}expand",
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
                                   format_func=lambda i: lbl[i], key=f"{_p}cand")
                    chosen = candidates[idx]
                    st.button(
                        "🔗 Link to this admission", type="primary", key=f"{_p}link",
                        on_click=_link_and_infer_status,
                        args=(conn, row_id, chosen["tid"]),
                    )
                else:
                    st.warning("No unlinked admissions found. Try expanding to ±1 month.")

    with tab_history:
        log = db.get_doctor_expense_log(conn, row_id)
        if log.empty:
            st.caption("No edits recorded yet.")
        else:
            st.dataframe(
                log.rename(columns={
                    "field": "Field", "old_value": "Old Value",
                    "new_value": "New Value", "changed_at": "Changed At",
                }),
                hide_index=True, width="stretch",
            )


@st.dialog("Confirm Delete", width="small")
def _confirm_delete_dialog(ids: list[int], conn):
    n = len(ids)
    st.warning(f"Permanently delete **{n}** {'entry' if n == 1 else 'entries'}?")
    st.caption("This cannot be undone.")
    c1, c2 = st.columns(2)
    if c1.button("🗑 Delete", type="primary", width="stretch", key="dlg_del_confirm"):
        for id_ in ids:
            db.delete_doctor_expense(conn, int(id_))
        _clear_row_selection(ids)
        st.rerun()
    if c2.button("Cancel", width="stretch", key="dlg_del_cancel"):
        st.rerun()


def render(conn) -> None:
    global DOCTORS
    DOCTORS = load_doctors()  # cheap re-read so doctors.toml edits don't need an app restart

    with st.sidebar:
        st.subheader("Filters")
        selected_doctor = st.selectbox(
            "Doctor", list(DOCTORS.keys()), key="ds_doctor",
        )
        available_months = db.get_doctor_expense_months(conn, selected_doctor)
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

    st.title(f"Doctor Share — {selected_doctor}")

    full_df = db.get_doctor_expenses(conn, selected_months, selected_doctor) if selected_months else pd.DataFrame()

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

        if add_month and not _valid_month(add_month):
            st.error("Enter month as YYYY-MM (e.g. 2025-06).")
            add_month = ""

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
                inferred_status = db.infer_maa_status(conn, chosen["tid"])
                st.info(f"Selected: **{chosen['patient_name']}** (TID: {chosen['tid']}, Adm: {chosen['date_of_admission']})")

                c1, c2, c3 = st.columns(3)
                hosp_ex     = c1.number_input("Hospital Ex ₹",  min_value=0, value=0, step=100, format="%d", key="ae_hosp",
                                              help="Hospital expenses borne by the hospital (e.g. implants, consumables) — deducted before calculating doctor share.")
                pharma_ex   = c2.number_input("Pharmacy Ex ₹",  min_value=0, value=0, step=100, format="%d", key="ae_pharma",
                                              help="Pharmacy / medicine costs borne by the hospital — deducted before calculating doctor share.")
                dialysis_ex = c3.number_input("Dialysis Ex ₹",  min_value=0, value=0, step=100, format="%d", key="ae_dialysis",
                                              help="Dialysis session costs borne by the hospital — deducted before calculating doctor share.")
                doctor_pct_input = st.number_input(
                    "Doctor % (default)", min_value=0.0, max_value=100.0,
                    value=DOCTORS[selected_doctor] * 100,
                    step=5.0, key="ae_pct",
                    help="Percentage of (MAA payment − expenses) that goes to the doctor; overridden if a flat amount is entered below.",
                ) / 100.0
                doctor_flat_raw = st.number_input("Flat override ₹ (0 = use %)", min_value=0, value=0, step=500, format="%d", key="ae_flat",
                                                  help="Fixed rupee amount for the doctor; leave 0 to use the percentage above.")
                comments_input  = st.text_input("Comments", key="ae_comments",
                                                help="Any additional notes about this entry (e.g. pending documents, special arrangements).")

                flat_val         = doctor_flat_raw if doctor_flat_raw > 0 else None
                total_ex_preview = hosp_ex + pharma_ex + dialysis_ex
                maa_preview      = chosen["maa_paid"] or 0
                _is_rejected     = bool(inferred_status) and "rejected" in inferred_status.lower()
                share_preview, hosp_preview = db.compute_doctor_share(
                    maa_preview, total_ex_preview, doctor_pct_input, flat_val, _is_rejected,
                )
                st.caption(
                    f"Preview → Total Ex: {fmt_inr(total_ex_preview)} | "
                    f"MAA Payment: {fmt_inr(maa_preview)} | "
                    f"Doctor Share: {fmt_inr(share_preview)}"
                )
                if share_preview is not None and share_preview < 0:
                    st.warning("Doctor share is negative — expenses exceed MAA payment.")
                elif hosp_preview == 0 and (total_ex_preview + (share_preview or 0)) > maa_preview:
                    st.warning("Hospital share floored at ₹0 — expenses + doctor share exceed the MAA payment.")

                if st.button("Save Entry", type="primary", key="ae_save_maa"):
                    db.save_doctor_expense(
                        conn, month=add_month,
                        patient_name=chosen["patient_name"],
                        admission_date=chosen["date_of_admission"],
                        hosp_ex=hosp_ex, pharma_ex=pharma_ex, dialysis_ex=dialysis_ex,
                        doctor_pct=doctor_pct_input, doctor_flat=flat_val,
                        comments=comments_input or None,
                        maa_status=inferred_status, tid=chosen["tid"],
                        doctor_name=selected_doctor,
                    )
                    st.success(f"Added entry for {chosen['patient_name']}.")
                    for _k in ["ae_search", "ae_hosp", "ae_pharma", "ae_dialysis",
                               "ae_pct", "ae_flat", "ae_comments", "expand_search"]:
                        st.session_state.pop(_k, None)
                    st.rerun()
            elif search_name:
                st.warning("No matching admissions found. Try a partial name or expand to ±1 month.")

        else:  # Non-MAA
            with st.form("nm_form", clear_on_submit=True):
                nm_name     = st.text_input("Patient Name")
                nm_date     = st.date_input("Admission Date")
                c1, c2, c3  = st.columns(3)
                nm_hosp     = c1.number_input("Hospital Ex ₹",  min_value=0, value=0, step=100, format="%d",
                                              help="Hospital expenses borne by the hospital — recorded for reference, not used in any automatic calculation.")
                nm_pharma   = c2.number_input("Pharmacy Ex ₹",  min_value=0, value=0, step=100, format="%d",
                                              help="Pharmacy / medicine costs borne by the hospital — recorded for reference.")
                nm_dialysis = c3.number_input("Dialysis Ex ₹",  min_value=0, value=0, step=100, format="%d",
                                              help="Dialysis session costs borne by the hospital — recorded for reference.")
                nm_share    = st.number_input("Doctor Share ₹", min_value=0, value=0, step=500, format="%d",
                                              help="The fixed rupee amount agreed as the doctor's fee for this non-MAA patient.")
                nm_comments = st.text_input("Comments",
                                            help="Any additional notes about this entry (e.g. pending documents, special arrangements).")
                if st.form_submit_button("Save Entry", type="primary"):
                    if not nm_name:
                        st.error("Patient name is required.")
                    elif not add_month:
                        st.error("Enter a valid month (YYYY-MM) above before saving.")
                    else:
                        db.save_doctor_expense(
                            conn, month=add_month, patient_name=nm_name,
                            admission_date=str(nm_date),
                            hosp_ex=nm_hosp, pharma_ex=nm_pharma, dialysis_ex=nm_dialysis,
                            doctor_flat=nm_share, comments=nm_comments or None, tid=None,
                            doctor_name=selected_doctor,
                        )
                        st.success(f"Added non-MAA entry for {nm_name}.")
                        st.rerun()

    _components.html("""
<script>
(function() {
    var doc = window.parent.document;
    var win = window.parent;

    // ── help-icon tab-skip ────────────────────────────────────────────────────
    // Streamlit renders help icons as <button> inside <span class="stTooltipIcon">.
    // Remove them from the tab order so Tab moves between inputs only.
    function _disableHelpTabIndex() {
        doc.querySelectorAll('.stTooltipIcon button').forEach(function(btn) {
            btn.setAttribute('tabindex', '-1');
        });
    }

    // ── post-save refocus ─────────────────────────────────────────────────────
    // After Cmd/Ctrl+Enter fires a save and the page rerenders, focus the
    // "Search patient name" input so the next record can be typed immediately.
    function _refocusSearch() {
        if (!win._maa_focus_search) return;
        var labels = doc.querySelectorAll('[data-testid="stTextInput"] label');
        for (var i = 0; i < labels.length; i++) {
            if (labels[i].textContent.includes('Search patient name')) {
                var inp = labels[i].closest('[data-testid="stTextInput"]').querySelector('input');
                if (inp) { inp.focus(); win._maa_focus_search = false; }
                break;
            }
        }
    }

    // Debounced MutationObserver runs both tasks on every DOM change.
    if (win._maa_dom_observer) { win._maa_dom_observer.disconnect(); }
    var _timer;
    win._maa_dom_observer = new MutationObserver(function() {
        clearTimeout(_timer);
        _timer = setTimeout(function() {
            _disableHelpTabIndex();
            _refocusSearch();
        }, 80);
    });
    win._maa_dom_observer.observe(doc.body, { childList: true, subtree: true });
    _disableHelpTabIndex();  // run once immediately for elements already in DOM

    // ── Cmd/Ctrl+Enter → Save Entry ──────────────────────────────────────────
    if (win._maa_save_handler) {
        doc.removeEventListener('keydown', win._maa_save_handler);
    }
    win._maa_save_handler = function(e) {
        if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
            var btn = Array.from(doc.querySelectorAll('button'))
                          .find(function(b) { return b.innerText.trim() === 'Save Entry'; });
            if (btn && !btn.disabled) {
                e.preventDefault();
                win._maa_focus_search = true;
                btn.click();
            }
        }
    };
    doc.addEventListener('keydown', win._maa_save_handler);
})();
</script>
""", height=0)

    if not selected_months:
        st.info("Select at least one month from the sidebar.")
    elif full_df.empty:
        st.info("No entries for the selected month(s). Use '➕ Add Entry' above.")
    else:
        _view_key = (selected_months, selected_doctor)
        if st.session_state.get("ds_view_prev") != _view_key:
            st.session_state["ds_view_prev"] = _view_key
            st.session_state["ds_page_num"] = 1
            st.session_state.pop("ds_batch_results", None)
        elif "ds_page_num" not in st.session_state:
            st.session_state["ds_page_num"] = 1

        _search_col, _sort_col = st.columns([2, 1])
        name_search = _search_col.text_input(
            "🔍 Search in list", key="ds_name_search", placeholder="Filter by patient name…",
        )
        if name_search:
            df = df[df["patient_name"].str.contains(name_search, case=False, na=False, regex=False)]

        _SORT_OPTS = {
            "Original order":     None,
            "Admission Date ↓":   ("admission_date", False),
            "Admission Date ↑":   ("admission_date", True),
            "Dr Share ↓":         ("doctor_share", False),
            "Dr Share ↑":         ("doctor_share", True),
            "Patient Name (A–Z)": ("patient_name", True),
        }
        sort_choice = _sort_col.selectbox("Sort by", list(_SORT_OPTS.keys()), key="ds_sort")
        _sort_spec = _SORT_OPTS[sort_choice]
        if _sort_spec and not df.empty:
            sort_col, ascending = _sort_spec
            df = df.sort_values(sort_col, ascending=ascending, na_position="last")

        paid_ct    = int((df["doctor_paid"] == 1).sum()) if not df.empty else 0
        unpaid_ct  = int((df["doctor_paid"] == 0).sum()) if not df.empty else 0
        non_maa_ct = int(df["tid"].isna().sum())          if not df.empty else 0
        filter_note = f" (filtered from {len(full_df)})" if (status_filter or paid_filter != "All" or name_search) else ""
        _cnt_col, _batch_col, _ref_col = st.columns([6, 2.4, 0.6])
        _cnt_col.markdown(
            f"**{len(df)}** {'entry' if len(df) == 1 else 'entries'}{filter_note}"
            f"&ensp;·&ensp;{paid_ct} paid&ensp;·&ensp;{unpaid_ct} unpaid&ensp;·&ensp;{non_maa_ct} non-MAA"
        )
        if _batch_col.button(
            "🔄 Refresh MAA Statuses", key="ds_batch_refresh",
            help="Re-infer MAA status from ingested claims for every linked, unpaid entry "
                 "in the selected month(s). Ignores the view filters. Does not fetch from "
                 "the portal — ingest fresh data first.",
        ):
            _eligible = full_df[
                full_df["tid"].notna() & (full_df["maa_status"].fillna("") != "Claim Paid")
            ]
            if _eligible.empty:
                st.toast("No unpaid linked entries to refresh.")
            else:
                _batch_entries = [
                    {
                        "id": int(_r["id"]),
                        "tid": _r["tid"],
                        "maa_status": _r["maa_status"] if pd.notna(_r["maa_status"]) else None,
                        "patient_name": _r["patient_name"],
                    }
                    for _, _r in _eligible.iterrows()
                ]
                st.session_state["ds_batch_results"] = db.batch_refresh_maa_status(conn, _batch_entries)
                st.rerun()
        if _ref_col.button("🔄", key="ds_refresh", help="Refresh entries"):
            st.rerun()

        if "ds_batch_results" in st.session_state:
            _res = st.session_state["ds_batch_results"]
            _res_changed = [r for r in _res if r["changed"]]
            with st.container(border=True):
                _sum_col, _x_col = st.columns([11, 0.6])
                _sum_col.markdown(
                    f"✅ **{len(_res_changed)} updated** &ensp;·&ensp; "
                    f"{len(_res) - len(_res_changed)} unchanged"
                )
                if _x_col.button("✕", key="ds_batch_dismiss", help="Dismiss results"):
                    del st.session_state["ds_batch_results"]
                    st.rerun()
                if _res_changed:
                    st.table(pd.DataFrame([
                        {
                            "Patient": r["patient_name"],
                            "Status": f"{r['old_status'] or '—'} → {r['new_status']}",
                        }
                        for r in _res_changed
                    ]))

        df_r = df.reset_index(drop=True)
        _multi = len(selected_months) > 1
        _PAGE_SIZE = 25
        _total_pages = max(1, (len(df_r) - 1) // _PAGE_SIZE + 1)

        ds_page = max(1, min(st.session_state["ds_page_num"], _total_pages))
        _start = (ds_page - 1) * _PAGE_SIZE
        _page_rows = df_r.iloc[_start : _start + _PAGE_SIZE]

        if _multi:
            _COLS = [0.18, 0.65, 2.6, 0.78, 0.78, 0.78, 0.78, 1.1, 0.3]
            _hdrs = ["", "Month", "Patient", "Date", "Total Ex", "MAA Pmt", "Dr Share", "Status", ""]
        else:
            _COLS = [0.18, 2.8, 0.78, 0.78, 0.78, 0.78, 1.1, 0.3]
            _hdrs = ["", "Patient", "Date", "Total Ex", "MAA Pmt", "Dr Share", "Status", ""]

        _RIGHT_HDRS  = {"Total Ex", "MAA Pmt", "Dr Share"}
        _CENTER_HDRS = {"Date"}
        _hrow = st.columns(_COLS)
        for _hi, _hl in enumerate(_hdrs[1:], start=1):
            if _hl:
                _align = "right" if _hl in _RIGHT_HDRS else ("center" if _hl in _CENTER_HDRS else "left")
                _hrow[_hi].markdown(
                    f"<p style='margin:0;padding:2px 0 4px;color:#888;text-align:{_align};"
                    f"font-size:0.78rem;font-weight:600;letter-spacing:0.03em'>{_hl}</p>",
                    unsafe_allow_html=True,
                )
        st.divider()

        def _md_money(v, warn: bool = False, flat: bool = False):
            if pd.isna(v):
                return "<div style='text-align:right;color:#bbb;font-size:0.9rem;padding-top:9px'>—</div>"
            _mark = (
                " <span title='Hospital share was floored at ₹0 — expenses + doctor share "
                "exceed the MAA payment on this entry.'>⚠️</span>" if warn else ""
            )
            _flat_badge = (
                " <span title='Flat override — not calculated from Doctor %' "
                "style='font-size:0.68rem;color:#888;border:1px solid #ccc;border-radius:3px;"
                "padding:0 3px;vertical-align:middle'>F</span>" if flat else ""
            )
            return f"<div style='text-align:right;font-size:0.9rem;padding-top:9px'>₹{v:,.0f}{_mark}{_flat_badge}</div>"

        _STATUS_STYLE = {
            "Claim Paid":     "color:#2e7d32;font-weight:600",
            "Claim Approved": "color:#e65100",
            "Claim Raised":   "color:#b8860b",
            "Query Raised":   "color:#b8860b",
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
                    f"<p style='margin:0;font-size:0.8rem;color:#999;padding-top:9px'>{_row['month']}</p>",
                    unsafe_allow_html=True,
                )
                _ci += 1
            if _c[_ci].button(_row["patient_name"] or "—", key=f"btn_{_rid}", width="stretch"):
                _entry_detail_dialog(_rid, conn)
            _ci += 1
            _c[_ci].markdown(
                f"<p style='margin:0;font-size:0.9rem;padding-top:9px;text-align:center'>{_row['admission_date'] or '—'}</p>",
                unsafe_allow_html=True,
            ); _ci += 1
            _c[_ci].markdown(_md_money(_row["total_ex"]), unsafe_allow_html=True); _ci += 1
            _c[_ci].markdown(_md_money(_row["maa_payment"]), unsafe_allow_html=True); _ci += 1
            _c[_ci].markdown(
                _md_money(
                    _row["doctor_share"],
                    warn=not _row["shares_reconcile"],
                    flat=pd.notna(_row["doctor_flat"]),
                ),
                unsafe_allow_html=True,
            ); _ci += 1
            _status_val = _row["maa_status"] or "Non-MAA"
            _sstyle = _STATUS_STYLE.get(_status_val, "color:#333")
            _c[_ci].markdown(
                f"<p style='margin:0;font-size:0.85rem;padding-top:9px;{_sstyle}'>{_status_val}</p>",
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
                    if _valid_month(pay_month_input):
                        db.mark_doctor_paid(conn, [int(i) for i in selected_ids], pay_month_input)
                        _clear_row_selection(selected_ids)
                        st.success(f"Marked {n} row(s) paid ({pay_month_input}).")
                        st.rerun()
                    else:
                        st.error("Enter a valid payment month (YYYY-MM) first.")

            with _ba2:
                st.write("")
                st.write("")
                st.write("")
                if st.button(f"✖ Unmark {n}", width="stretch", key="bulk_unmark_paid",
                             help=f"Unmark {n} selected {'entry' if n==1 else 'entries'} as paid"):
                    db.unmark_doctor_paid(conn, [int(i) for i in selected_ids])
                    _clear_row_selection(selected_ids)
                    st.success(f"Unmarked {n} row(s).")
                    st.rerun()

            with _ba3:
                move_month_input = st.text_input(
                    "Move to month (YYYY-MM)", placeholder="YYYY-MM", key="move_month_input",
                    help="Reassign the selected entries to a different filing month (e.g. to correct a wrong month).",
                )
                if st.button(f"📅 Change Month ({n})", width="stretch", key="bulk_change_month"):
                    if not _valid_month(move_month_input):
                        st.error("Enter a valid month (YYYY-MM) before changing.")
                    else:
                        for _id in selected_ids:
                            db.update_doctor_expense(conn, int(_id), {"month": move_month_input})
                        _clear_row_selection(selected_ids)
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
        _metrics_label = "Month Totals" + (" (ignores filters above)" if (status_filter or paid_filter != "All") else "")
        st.caption(_metrics_label)
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
        _doc_slug = selected_doctor.replace("Dr. ", "Dr").replace(" ", "")
        if df.empty:
            st.caption("No entries match the current filters — nothing to download.")
        else:
            col_int, col_doc = st.columns(2)
            with col_int:
                st.download_button(
                    label=f"Download Internal Export — {month_label}",
                    data=reports.generate_doctor_internal(df, _sheet_label),
                    file_name=f"DoctorShare_Internal_{_doc_slug}_{'_'.join(_sorted_months)}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            with col_doc:
                st.download_button(
                    label=f"Download Doctor Copy — {month_label}",
                    data=reports.generate_doctor_copy(df, _sheet_label),
                    file_name=f"DoctorShare_{_doc_slug}_{'_'.join(_sorted_months)}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
