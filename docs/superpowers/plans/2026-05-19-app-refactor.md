# app.py Refactor + README Update Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the 910-line `app.py` into a `pages/` Python package (one module per page) and extract shared UI helpers into `utils.py`, leaving `app.py` as a ~55-line orchestrator.

**Architecture:** Custom `st.session_state["_page"]` navigation is preserved unchanged. Each page module exposes a single `render(conn)` function. `utils.py` holds `fmt_inr`; `reports.month_label` (renamed from `_month_label`) is used directly by `pages/doctor_share.py`.

**Tech Stack:** Python 3, Streamlit, SQLite (via `db.py`), pandas, openpyxl (via `reports.py`), Plotly.

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Modify | `reports.py:219` | Rename `_month_label` → `month_label`; update internal call at line 252 |
| Create | `utils.py` | `fmt_inr` helper |
| Create | `pages/__init__.py` | Empty package marker |
| Create | `pages/dashboard.py` | Dashboard page → `render(conn)` |
| Create | `pages/ingest.py` | Ingest page → `render(conn)` |
| Create | `pages/admissions.py` | Admissions page → `render(conn)` |
| Create | `pages/reports.py` | Reports page → `render(conn)` |
| Create | `pages/doctor_share.py` | Doctor Share page + its two dialogs → `render(conn)` |
| Rewrite | `app.py` | Page config, DB, sidebar nav, routing only (~55 lines) |
| Modify | `README.md` | Add Doctor Share feature + Architecture section |

---

### Task 1: Rename `_month_label` → `month_label` in `reports.py`

**Files:**
- Modify: `reports.py:219,252`

- [ ] **Step 1: Rename the function definition and its one internal call**

In `reports.py`, make two changes:

Line 219 — change function definition:
```python
# before
def _month_label(ym: str) -> str:
# after
def month_label(ym: str) -> str:
```

Line 252 — change internal call (inside `_generate_detail_report` or nearby):
```python
# before
label = _month_label(month_ym)
# after
label = month_label(month_ym)
```

- [ ] **Step 2: Verify tests still pass**

```bash
python -m pytest tests/ -q
```
Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add reports.py
git commit -m "refactor: make month_label public in reports"
```

---

### Task 2: Create `utils.py`

**Files:**
- Create: `utils.py`

- [ ] **Step 1: Create the file**

```python
# utils.py


def fmt_inr(val: float) -> str:
    if val is None:
        return "₹0"
    return f"₹{val:,.0f}"
```

- [ ] **Step 2: Verify import**

```bash
python -c "from utils import fmt_inr; assert fmt_inr(1234567.0) == '₹12,34,567'; print('OK')"
```

Wait — Python's `,` format specifier uses Western grouping (1,234,567), not Indian (12,34,567). The original `fmt_inr` in `app.py` also uses `f"₹{val:,.0f}"` which produces Western grouping. Keep it identical to the original:

```bash
python -c "from utils import fmt_inr; assert fmt_inr(1234.5) == '₹1,235'; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add utils.py
git commit -m "refactor: extract fmt_inr into utils.py"
```

---

### Task 3: Create `pages/__init__.py`

**Files:**
- Create: `pages/__init__.py`

- [ ] **Step 1: Create empty package marker**

```python
# pages/__init__.py
```

(Empty file — just needs to exist.)

- [ ] **Step 2: Verify**

```bash
python -c "import pages; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add pages/__init__.py
git commit -m "refactor: add pages/ package"
```

---

### Task 4: Create `pages/dashboard.py`

**Files:**
- Create: `pages/dashboard.py`

Source: `app.py` lines 312–367.

- [ ] **Step 1: Create the file**

```python
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
```

- [ ] **Step 2: Verify import**

```bash
python -c "from pages import dashboard; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Run tests**

```bash
python -m pytest tests/ -q
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add pages/dashboard.py
git commit -m "refactor: extract Dashboard page into pages/dashboard.py"
```

---

### Task 5: Create `pages/ingest.py`

**Files:**
- Create: `pages/ingest.py`

Source: `app.py` lines 235–305.

Note: `__file__` is now `maa_app/pages/ingest.py`, so auto-detect needs one extra `.parent` to reach the directory above `maa_app/`.

- [ ] **Step 1: Create the file**

```python
# pages/ingest.py
import glob
from pathlib import Path

import streamlit as st

import db
import ingest as ingest_module


def render(conn) -> None:
    st.title("Ingest CSV Files")

    uploaded = st.file_uploader(
        "Upload GenericSearchReport CSV files",
        type=["csv"],
        accept_multiple_files=True,
    )
    auto_detect = st.button("Auto-detect CSVs in current directory")
    dry_run     = st.checkbox("Dry Run (parse only, don't write to DB)")

    csv_paths: list[str] = []

    if uploaded:
        tmp_dir = Path("/tmp/maa_uploads")
        tmp_dir.mkdir(exist_ok=True)
        for f in uploaded:
            dest = tmp_dir / f.name
            dest.write_bytes(f.read())
            csv_paths.append(str(dest))

    if auto_detect:
        # pages/ingest.py lives two levels below the project root's parent
        csv_dir = Path(__file__).parent.parent.parent
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
            st.cache_data.clear()
```

- [ ] **Step 2: Verify import**

```bash
python -c "from pages import ingest; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Run tests**

```bash
python -m pytest tests/ -q
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add pages/ingest.py
git commit -m "refactor: extract Ingest page into pages/ingest.py"
```

---

### Task 6: Create `pages/admissions.py`

**Files:**
- Create: `pages/admissions.py`

Source: `app.py` lines 373–431.

- [ ] **Step 1: Create the file**

```python
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
```

- [ ] **Step 2: Verify import**

```bash
python -c "from pages import admissions; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Run tests**

```bash
python -m pytest tests/ -q
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add pages/admissions.py
git commit -m "refactor: extract Admissions page into pages/admissions.py"
```

---

### Task 7: Create `pages/reports.py`

**Files:**
- Create: `pages/reports.py`

Source: `app.py` lines 438–571.

Note: inside `pages/reports.py`, `import reports as _reports` resolves to the top-level `reports.py` (the Excel generator) — Python's absolute import finds it by name, not by proximity. The alias `_reports` is used throughout this file to avoid any ambiguity.

- [ ] **Step 1: Create the file**

```python
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
```

- [ ] **Step 2: Verify import**

```bash
python -c "from pages import reports as rp; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Run tests**

```bash
python -m pytest tests/ -q
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add pages/reports.py
git commit -m "refactor: extract Reports page into pages/reports.py"
```

---

### Task 8: Create `pages/doctor_share.py`

**Files:**
- Create: `pages/doctor_share.py`

Source: `app.py` lines 70–215 (helpers + dialogs) and lines 577–910 (page render).

The two `@st.dialog` functions and `_link_and_infer_status` live at module level so Streamlit registers them once at import time.

- [ ] **Step 1: Create the file**

```python
# pages/doctor_share.py
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
    st.caption(f"Month: {r['month']} · Adm: {r['admission_date']}{tid_badge}")

    tab_edit, tab_maa = st.tabs(["✏️ Edit Entry", "🏥 MAA Claim"])

    with tab_edit:
        c1, c2, c3 = st.columns(3)
        new_hosp     = c1.number_input("Hospital Ex ₹",  value=float(r["hosp_ex"] or 0),     min_value=0.0, step=100.0, key="d_hosp")
        new_pharma   = c2.number_input("Pharmacy Ex ₹",  value=float(r["pharma_ex"] or 0),   min_value=0.0, step=100.0, key="d_pharma")
        new_dialysis = c3.number_input("Dialysis Ex ₹",  value=float(r["dialysis_ex"] or 0), min_value=0.0, step=100.0, key="d_dialysis")

        d1, d2 = st.columns(2)
        new_pct      = d1.number_input("Doctor %", value=float(r["doctor_pct"] or 0.4) * 100,
                                       min_value=0.0, max_value=100.0, step=5.0, key="d_pct") / 100.0
        new_flat_raw = d2.number_input("Flat Override ₹ (0 = use %)",
                                       value=float(r["doctor_flat"] or 0), min_value=0.0, step=500.0, key="d_flat")
        new_flat = new_flat_raw if new_flat_raw > 0 else None

        maa_status_opts = ["", "Claim Paid", "Claim Approved", "Claim Raised", "Query Raised", "Rejected"]
        cur_status = r["maa_status"] or ""
        if cur_status not in maa_status_opts:
            maa_status_opts.append(cur_status)
        e1, e2 = st.columns(2)
        new_maa_status = e1.selectbox("MAA Status", maa_status_opts,
                                      index=maa_status_opts.index(cur_status), key="d_maa_status")
        new_pay_month  = e2.text_input("Payment Month (YYYY-MM)",
                                       value=r["doctor_payment_month"] or "", key="d_pay_month")
        new_comments   = st.text_input("Comments", value=r["comments"] or "", key="d_comments")

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
                "hosp_ex":              new_hosp,
                "pharma_ex":            new_pharma,
                "dialysis_ex":          new_dialysis,
                "doctor_pct":           new_pct,
                "doctor_flat":          new_flat,
                "comments":             new_comments or None,
                "maa_status":           new_maa_status or None,
                "doctor_payment_month": new_pay_month or None,
            })
            for _k in ["d_hosp", "d_pharma", "d_dialysis", "d_pct", "d_flat",
                        "d_maa_status", "d_pay_month", "d_comments"]:
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
                _paid_amt   = pkgs.loc[pkgs["status"] == "Claim Paid", "approved_amount"].sum()
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
            src_name   = st.text_input("Search by name", value=str(r["patient_name"] or ""), key="d_src")
            src_expand = st.checkbox("Expand search to ±1 month", key="d_expand")
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
        )
        paid_filter = st.selectbox("Doctor Paid", ["All", "Paid", "Unpaid"])

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
            add_month = st.selectbox(
                "Add to month", available_months, key="add_entry_month",
                help="Which month this entry belongs to",
            )
        else:
            add_month = st.text_input("Month (YYYY-MM)", key="add_entry_month", placeholder="2025-06")

        entry_type = st.radio("Patient type", ["MAA Patient", "Non-MAA Patient"], horizontal=True)

        if entry_type == "MAA Patient":
            search_name = st.text_input("Search patient name (from physical bill)", key="ae_search")
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
                hosp_ex     = c1.number_input("Hospital Ex ₹",  min_value=0.0, step=100.0, key="ae_hosp")
                pharma_ex   = c2.number_input("Pharmacy Ex ₹",  min_value=0.0, step=100.0, key="ae_pharma")
                dialysis_ex = c3.number_input("Dialysis Ex ₹",  min_value=0.0, step=100.0, key="ae_dialysis")
                doctor_pct_input = st.number_input(
                    "Doctor % (default 40%)", min_value=0.0, max_value=100.0,
                    value=40.0, step=5.0, key="ae_pct",
                ) / 100.0
                doctor_flat_raw = st.number_input("Flat override ₹ (0 = use %)", min_value=0.0, step=500.0, key="ae_flat")
                comments_input  = st.text_input("Comments", key="ae_comments")

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
                        maa_status=chosen["status"], tid=chosen["tid"],
                    )
                    st.success(f"Added entry for {chosen['patient_name']}.")
                    st.rerun()
            elif search_name:
                st.warning("No matching admissions found. Try a partial name or expand to ±1 month.")

        else:  # Non-MAA
            nm_name     = st.text_input("Patient Name", key="nm_name")
            nm_date     = st.date_input("Admission Date", key="nm_date")
            c1, c2, c3  = st.columns(3)
            nm_hosp     = c1.number_input("Hospital Ex ₹",  min_value=0.0, step=100.0, key="nm_hosp")
            nm_pharma   = c2.number_input("Pharmacy Ex ₹",  min_value=0.0, step=100.0, key="nm_pharma")
            nm_dialysis = c3.number_input("Dialysis Ex ₹",  min_value=0.0, step=100.0, key="nm_dialysis")
            nm_share    = st.number_input("Doctor Share ₹", min_value=0.0, step=500.0,  key="nm_share")
            nm_comments = st.text_input("Comments", key="nm_comments")

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
        st.markdown(
            f"**{len(df)}** {'entry' if len(df) == 1 else 'entries'}{filter_note}"
            f"&ensp;·&ensp;{paid_ct} paid&ensp;·&ensp;{unpaid_ct} unpaid&ensp;·&ensp;{non_maa_ct} non-MAA"
        )

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
                _c[_ci].markdown("<div style='padding-top:4px;font-size:1rem'>🟢</div>", unsafe_allow_html=True)
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
            act_cols = st.columns([2, 2, 1])
            with act_cols[0]:
                default_pay = selected_months[-1] if len(selected_months) == 1 else ""
                pay_month_input = st.text_input(
                    "Payment month", value=default_pay,
                    placeholder="YYYY-MM", key="pay_month_input",
                )
            with act_cols[1]:
                st.write("")
                st.write("")
                if st.button(f"✔ Mark {n} Paid", width="stretch"):
                    if pay_month_input:
                        db.mark_doctor_paid(conn, [int(i) for i in selected_ids], pay_month_input)
                        st.success(f"Marked {n} row(s) paid ({pay_month_input}).")
                        st.rerun()
                    else:
                        st.error("Enter a payment month first.")
            with act_cols[2]:
                st.write("")
                st.write("")
                if st.button(f"🗑 {n}", type="secondary", width="stretch",
                             help=f"Delete {n} selected {'entry' if n==1 else 'entries'}"):
                    _confirm_delete_dialog([int(i) for i in selected_ids], conn)
        else:
            st.caption("Click a patient name to open · check rows for bulk Mark Paid / Delete")

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
        month_label = " · ".join(reports.month_label(m) for m in sorted(selected_months))
        col_int, col_doc = st.columns(2)
        with col_int:
            st.download_button(
                label=f"Download Internal Export — {month_label}",
                data=reports.generate_doctor_internal(full_df, month_label),
                file_name=f"DoctorShare_Internal_{'_'.join(sorted(selected_months))}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        with col_doc:
            st.download_button(
                label=f"Download Doctor Copy — {month_label}",
                data=reports.generate_doctor_copy(full_df, month_label),
                file_name=f"DoctorShare_DrKavesh_{'_'.join(sorted(selected_months))}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
```

- [ ] **Step 2: Verify import**

```bash
python -c "from pages import doctor_share; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Run tests**

```bash
python -m pytest tests/ -q
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add pages/doctor_share.py
git commit -m "refactor: extract Doctor Share page into pages/doctor_share.py"
```

---

### Task 9: Rewrite `app.py` as thin orchestrator

**Files:**
- Modify: `app.py` (full rewrite — replace all 910 lines)

This is the step where the old page code is removed from `app.py`. All page modules already exist from Tasks 4–8; this just wires them up and removes the now-duplicated code.

- [ ] **Step 1: Replace `app.py` entirely**

```python
"""
MAA Payment Record Management System — Streamlit Web UI.
Run with: streamlit run app.py
"""

import streamlit as st

import db
from pages import admissions, dashboard, doctor_share
from pages import ingest as ingest_page
from pages import reports as reports_page

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

if "_page" not in st.session_state:
    st.session_state["_page"] = "Dashboard"

with st.sidebar:
    st.title("🏥 MAA Records")
    for _p in ["Dashboard", "Ingest", "Admissions", "Reports"]:
        if st.button(
            _p, key=f"nav_{_p}", width="stretch",
            type="primary" if st.session_state["_page"] == _p else "secondary",
        ):
            st.session_state["_page"] = _p

    st.divider()
    st.caption("CLINICAL")
    if st.button(
        "🩺 Doctor Share", key="nav_ds", width="stretch",
        type="primary" if st.session_state["_page"] == "Doctor Share" else "secondary",
    ):
        st.session_state["_page"] = "Doctor Share"

_PAGE_MAP = {
    "Dashboard":    dashboard,
    "Ingest":       ingest_page,
    "Admissions":   admissions,
    "Reports":      reports_page,
    "Doctor Share": doctor_share,
}

_PAGE_MAP[st.session_state["_page"]].render(conn)
```

- [ ] **Step 2: Verify syntax compiles**

```bash
python -m py_compile app.py && echo "syntax OK"
python -m py_compile pages/dashboard.py pages/ingest.py pages/admissions.py pages/reports.py pages/doctor_share.py utils.py && echo "all pages syntax OK"
```

Expected: `syntax OK` / `all pages syntax OK`

- [ ] **Step 3: Run tests**

```bash
python -m pytest tests/ -q
```

Expected: all tests pass.

- [ ] **Step 4: Verify line count**

```bash
wc -l app.py
```

Expected: ≤ 60 lines.

- [ ] **Step 5: Commit**

```bash
git add app.py
git commit -m "refactor: slim app.py to thin orchestrator (~55 lines)"
```

---

### Task 10: Update README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add Doctor Share to Features section**

After the existing "Reports" bullet, add:

```markdown
- **Doctor Share** — Per-doctor expense tracking with MAA claim linking, monthly filters, bulk mark-paid, and internal/doctor-copy Excel exports
```

- [ ] **Step 2: Add Architecture section after the Features section**

Insert before `## Setup`:

```markdown
## Architecture

```
app.py          Entry point: page config, DB connection, sidebar nav, routing
utils.py        Shared UI helpers (fmt_inr)
db.py           All database access (SQLite). Schema: claims + doctor_expenses
ingest.py       CSV parsing and ingestion (CLI and library)
reports.py      In-memory .xlsx generation via openpyxl
pages/
  dashboard.py  Dashboard page
  ingest.py     Ingest page
  admissions.py Admissions page
  reports.py    Reports page
  doctor_share.py  Doctor Share page + entry/delete dialogs
```
```

- [ ] **Step 3: Run tests to confirm nothing broke**

```bash
python -m pytest tests/ -q
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: update README — add Doctor Share feature + Architecture section"
```

---

## Done

After Task 10, verify the full success criteria from the spec:

```bash
# All tests green
python -m pytest tests/ -q

# app.py is ≤ 60 lines
wc -l app.py

# All page modules importable
python -c "from pages import dashboard, ingest, admissions, reports as rp, doctor_share; print('all pages OK')"
```
