# Doctor Share QoL Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Advanced edit fields (filing month, doctor_paid), bulk Unmark Paid + Change Month actions, a refresh button, teal Doctor Copy export colour, and QoL polish to the Doctor Share module.

**Architecture:** Changes land across three files: `db.py` (allowlist extension + new function), `reports.py` (colour constant + optional param), and `pages/doctor_share.py` (all UI changes). `pages/doctor_share.py` is the post-refactor home of the Doctor Share page — **do not edit `app.py`**. Tasks are sequenced so DB and report changes are tested first, then the UI builds on them.

**Tech Stack:** Python 3, Streamlit, SQLite (`db.py`), pandas, openpyxl (`reports.py`).

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Modify | `db.py:682-699` | Add `month`, `doctor_paid` to `update_doctor_expense` allowlist |
| Modify | `db.py` | Add `unmark_doctor_paid(conn, ids)` function |
| Modify | `tests/test_doctor_share_db.py:210-222` | Update disallowed-key test; add new tests for `month`, `doctor_paid`, `unmark_doctor_paid` |
| Modify | `reports.py:15` | Add `DOCTOR_COPY_HEADER_FILL` constant |
| Modify | `reports.py:133` | Add optional `header_fill` param to `_write_sheet` |
| Modify | `reports.py:427-434` | Pass `DOCTOR_COPY_HEADER_FILL` to `generate_doctor_copy` |
| Modify | `tests/test_doctor_share_reports.py` | Add test for teal header colour in Doctor Copy |
| Modify | `pages/doctor_share.py` | All UI changes: dialog fields, Advanced expander, refresh button, bulk actions, table polish |

---

### Task 1: Update `update_doctor_expense` allowlist + add `unmark_doctor_paid`

**Files:**
- Modify: `db.py:682-699`
- Modify: `db.py` (append `unmark_doctor_paid` after `mark_doctor_paid`)
- Test: `tests/test_doctor_share_db.py:210-222`

**Background:** The existing `test_update_doctor_expense_disallowed_key` test (line 210) explicitly asserts that `doctor_paid` cannot be written. We are intentionally adding it to the allowlist, so that test must be updated first to avoid a false failure after the DB change.

- [ ] **Step 1: Update the disallowed-key test — remove `doctor_paid` from it, add `month` + `doctor_paid` writable tests, add `unmark_doctor_paid` test**

Replace the entire block from line 210 to the end of `tests/test_doctor_share_db.py` with:

```python
def test_update_doctor_expense_disallowed_key(mem_db):
    """created_at must not be writable via update_doctor_expense."""
    conn = mem_db
    conn.execute(
        "INSERT INTO doctor_expenses (tid, patient_name, admission_date, month) VALUES (?, ?, ?, ?)",
        ("T001", "Patient A", "2025-06-01", "2025-06"),
    )
    conn.commit()
    row_id = conn.execute("SELECT id FROM doctor_expenses").fetchone()[0]
    db.update_doctor_expense(conn, row_id, {"created_at": "1970-01-01"})
    row = conn.execute("SELECT created_at FROM doctor_expenses WHERE id=?", (row_id,)).fetchone()
    assert row[0] != "1970-01-01", "created_at must not be updated via update_doctor_expense"


def test_update_doctor_expense_month(mem_db):
    conn = mem_db
    conn.execute(
        "INSERT INTO doctor_expenses (tid, patient_name, admission_date, month) VALUES (?, ?, ?, ?)",
        ("T001", "Patient A", "2025-06-01", "2025-06"),
    )
    conn.commit()
    row_id = conn.execute("SELECT id FROM doctor_expenses").fetchone()[0]
    db.update_doctor_expense(conn, row_id, {"month": "2025-07"})
    month = conn.execute("SELECT month FROM doctor_expenses WHERE id=?", (row_id,)).fetchone()[0]
    assert month == "2025-07"


def test_update_doctor_expense_doctor_paid(mem_db):
    conn = mem_db
    conn.execute(
        "INSERT INTO doctor_expenses (tid, patient_name, admission_date, month, doctor_paid) VALUES (?, ?, ?, ?, ?)",
        ("T001", "Patient A", "2025-06-01", "2025-06", 0),
    )
    conn.commit()
    row_id = conn.execute("SELECT id FROM doctor_expenses").fetchone()[0]
    db.update_doctor_expense(conn, row_id, {"doctor_paid": 1})
    paid = conn.execute("SELECT doctor_paid FROM doctor_expenses WHERE id=?", (row_id,)).fetchone()[0]
    assert paid == 1


def test_unmark_doctor_paid(mem_db):
    conn = mem_db
    conn.executemany(
        "INSERT INTO doctor_expenses (tid, patient_name, admission_date, month, doctor_paid, doctor_payment_month) VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("T001", "P1", "2025-06-01", "2025-06", 1, "2025-06"),
            ("T002", "P2", "2025-06-02", "2025-06", 1, "2025-06"),
        ],
    )
    conn.commit()
    ids = [r[0] for r in conn.execute("SELECT id FROM doctor_expenses").fetchall()]
    db.unmark_doctor_paid(conn, ids)
    rows = conn.execute("SELECT doctor_paid, doctor_payment_month FROM doctor_expenses").fetchall()
    assert all(r[0] == 0 for r in rows)
    assert all(r[1] is None for r in rows)
```

- [ ] **Step 2: Run tests — expect the new tests to fail (functions not yet updated)**

```bash
cd /Users/kartikeya/Tech/maa_app && source .venv/bin/activate && python -m pytest tests/test_doctor_share_db.py -q
```

Expected failures: `test_update_doctor_expense_month`, `test_update_doctor_expense_doctor_paid`, `test_unmark_doctor_paid`.

- [ ] **Step 3: Update `db.py` — extend allowlist + add `unmark_doctor_paid`**

In `db.py`, change the `allowed` set inside `update_doctor_expense` (currently at line 687):

```python
    allowed = {
        "hosp_ex", "pharma_ex", "dialysis_ex", "doctor_pct", "doctor_flat", "comments",
        "doctor_payment_month", "maa_status", "tid", "patient_name", "admission_date",
        "month", "doctor_paid",
    }
```

Then add `unmark_doctor_paid` immediately after `mark_doctor_paid` (currently the last function in the file):

```python
def unmark_doctor_paid(conn: sqlite3.Connection, ids: list[int]) -> None:
    """Bulk-clear doctor_paid and doctor_payment_month for the given ids."""
    if not ids:
        return
    placeholders = ",".join("?" for _ in ids)
    conn.execute(
        f"UPDATE doctor_expenses SET doctor_paid = 0, doctor_payment_month = NULL, updated_at = datetime('now') WHERE id IN ({placeholders})",
        ids,
    )
    conn.commit()
```

- [ ] **Step 4: Run tests — all should pass**

```bash
python -m pytest tests/test_doctor_share_db.py -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add db.py tests/test_doctor_share_db.py
git commit -m "feat: add month+doctor_paid to update allowlist; add unmark_doctor_paid"
```

---

### Task 2: Teal header colour for Doctor Copy export

**Files:**
- Modify: `reports.py:15` (colour constants block)
- Modify: `reports.py:133` (`_write_sheet` signature)
- Modify: `reports.py:427-434` (`generate_doctor_copy`)
- Test: `tests/test_doctor_share_reports.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_doctor_share_reports.py`:

```python
def test_generate_doctor_copy_header_is_teal(sample_entries):
    wb = load_workbook(io.BytesIO(reports.generate_doctor_copy(sample_entries, "June 2025")))
    ws = wb.active
    header_fill = ws.cell(1, 1).fill.fgColor.rgb
    assert header_fill == "FF00695C", f"Expected teal FF00695C, got {header_fill}"


def test_generate_doctor_internal_header_is_navy(sample_entries):
    wb = load_workbook(io.BytesIO(reports.generate_doctor_internal(sample_entries, "June 2025")))
    ws = wb.active
    header_fill = ws.cell(1, 1).fill.fgColor.rgb
    assert header_fill == "FF1F3864", f"Expected navy FF1F3864, got {header_fill}"
```

- [ ] **Step 2: Run to verify they fail**

```bash
python -m pytest tests/test_doctor_share_reports.py::test_generate_doctor_copy_header_is_teal tests/test_doctor_share_reports.py::test_generate_doctor_internal_header_is_navy -v
```

Expected: `test_generate_doctor_copy_header_is_teal` FAILS (still navy); `test_generate_doctor_internal_header_is_navy` PASSES.

- [ ] **Step 3: Add `DOCTOR_COPY_HEADER_FILL` constant to `reports.py`**

In `reports.py`, after line 16 (`HEADER_FONT = Font(...)`), add:

```python
DOCTOR_COPY_HEADER_FILL = StylePatternFill(fill_type="solid", fgColor="00695C")
DOCTOR_COPY_HEADER_FONT = Font(bold=True, color="FFFFFF", name="Calibri")
```

- [ ] **Step 4: Add `header_fill` and `header_font` params to `_write_sheet`**

Change the `_write_sheet` signature (currently at line 133):

```python
def _write_sheet(ws, df: pd.DataFrame, col_defs: list[tuple], title: str,
                 status_col_idx: int | None = None,
                 header_fill=None, header_font=None):
```

At the top of the function body, resolve defaults:

```python
    _hfill = header_fill if header_fill is not None else HEADER_FILL
    _hfont = header_font if header_font is not None else HEADER_FONT
```

Replace all uses of `HEADER_FILL` and `HEADER_FONT` inside `_write_sheet`'s header-row loop with `_hfill` and `_hfont`. The loop currently reads:

```python
    for ci, hdr in enumerate(headers, 1):
        cell = ws.cell(row=1, column=ci, value=hdr)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
```

Change to:

```python
    for ci, hdr in enumerate(headers, 1):
        cell = ws.cell(row=1, column=ci, value=hdr)
        cell.fill = _hfill
        cell.font = _hfont
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
```

- [ ] **Step 5: Pass teal fill into `generate_doctor_copy`**

In `generate_doctor_copy` (currently at line 427), change the `_write_sheet` call:

```python
def generate_doctor_copy(df: pd.DataFrame, month_label: str) -> bytes:
    """Doctor-facing report: no payment tracking columns."""
    wb = Workbook()
    ws = wb.active
    _write_sheet(
        ws, _prepare_doctor_df(df), DOCTOR_COPY_COLS, f"{month_label} (Dr Copy)",
        header_fill=DOCTOR_COPY_HEADER_FILL, header_font=DOCTOR_COPY_HEADER_FONT,
    )
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
```

- [ ] **Step 6: Run all tests**

```bash
python -m pytest tests/ -q
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add reports.py tests/test_doctor_share_reports.py
git commit -m "feat: teal header for Doctor Copy export; parameterise _write_sheet header colour"
```

---

### Task 3: Edit dialog — expose patient_name and admission_date

**Files:**
- Modify: `pages/doctor_share.py` — `_entry_detail_dialog` function

**Background:** These fields are already writable in `update_doctor_expense` (from Task 1) but not shown in the UI.

- [ ] **Step 1: Add patient_name and admission_date inputs at the top of the Edit tab**

In `pages/doctor_share.py`, inside `_entry_detail_dialog`, find the `with tab_edit:` block. The block currently starts with:

```python
    with tab_edit:
        c1, c2, c3 = st.columns(3)
        new_hosp     = c1.number_input("Hospital Ex ₹", ...
```

Insert a two-column row **before** that `c1, c2, c3` line:

```python
    with tab_edit:
        n1, n2 = st.columns(2)
        new_patient_name = n1.text_input(
            "Patient Name", value=str(r["patient_name"] or ""), key="d_patient_name"
        )
        _adm_default = None
        try:
            from datetime import date as _date
            _adm_default = _date.fromisoformat(str(r["admission_date"]))
        except Exception:
            pass
        new_admission_date = n2.date_input(
            "Admission Date", value=_adm_default, key="d_admission_date"
        )

        c1, c2, c3 = st.columns(3)
        new_hosp     = c1.number_input("Hospital Ex ₹", ...
```

- [ ] **Step 2: Include the new fields in the Save Changes call**

Find the `db.update_doctor_expense(conn, row_id, {...})` call inside the `if st.button("💾 Save Changes", ...)` block. Add the two new fields:

```python
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
                "doctor_payment_month": new_pay_month or None,
            })
            for _k in ["d_patient_name", "d_admission_date", "d_hosp", "d_pharma",
                        "d_dialysis", "d_pct", "d_flat", "d_maa_status",
                        "d_pay_month", "d_comments"]:
                st.session_state.pop(_k, None)
            st.success("Saved.")
```

- [ ] **Step 3: Verify syntax**

```bash
python -m py_compile pages/doctor_share.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 4: Run tests**

```bash
python -m pytest tests/ -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add pages/doctor_share.py
git commit -m "feat: expose patient_name and admission_date in edit dialog"
```

---

### Task 4: Edit dialog — Advanced expander

**Files:**
- Modify: `pages/doctor_share.py` — `_entry_detail_dialog` function

**What moves:** The "Payment Month (YYYY-MM)" input currently in the regular section is **removed** from there and placed inside the new Advanced expander alongside the filing month and doctor_paid toggle.

- [ ] **Step 1: Remove the payment month field from the regular section**

In the `with tab_edit:` block, find and remove these two lines:

```python
        new_pay_month  = e2.text_input("Payment Month (YYYY-MM)",
                                       value=r["doctor_payment_month"] or "", key="d_pay_month")
```

Also remove `e2` from the column unpacking. Change:

```python
        e1, e2 = st.columns(2)
        new_maa_status = e1.selectbox("MAA Status", maa_status_opts, ...
        new_pay_month  = e2.text_input("Payment Month (YYYY-MM)", ...
```

to:

```python
        new_maa_status = st.selectbox("MAA Status", maa_status_opts,
                                      index=maa_status_opts.index(cur_status), key="d_maa_status")
```

- [ ] **Step 2: Add the Advanced expander after `st.text_input("Comments", ...)` and before the live preview metrics**

Place the expander between `new_comments = st.text_input(...)` and the `tot_ex = ...` preview block:

```python
        new_comments = st.text_input("Comments", value=r["comments"] or "", key="d_comments")

        with st.expander("⚙️ Advanced"):
            adv1, adv2 = st.columns(2)
            new_filing_month = adv1.text_input(
                "Filing Month (YYYY-MM)",
                value=str(r["month"]),
                key="d_filing_month",
                help="Moves this entry to a different month bucket.",
            )
            new_doctor_paid = adv2.checkbox(
                "Paid to Doctor",
                value=bool(r["doctor_paid"]),
                key="d_doctor_paid",
            )
            new_pay_month = st.text_input(
                "Payment Month (YYYY-MM)",
                value=r["doctor_payment_month"] or "",
                key="d_pay_month",
                help="Month in which payment was made to the doctor.",
            )

        tot_ex = new_hosp + new_pharma + new_dialysis
```

- [ ] **Step 3: Add the new advanced fields to the Save Changes call**

The `db.update_doctor_expense` dict should now also include:

```python
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
```

- [ ] **Step 4: Update dialog caption to show `updated_at`**

Find the caption line (currently):

```python
    st.caption(f"Month: {r['month']} · Adm: {r['admission_date']}{tid_badge}")
```

Change to:

```python
    _updated = str(r.get("updated_at", "") or "")[:10]
    st.caption(f"Month: {r['month']} · Adm: {r['admission_date']} · Updated: {_updated}{tid_badge}")
```

- [ ] **Step 5: Verify syntax**

```bash
python -m py_compile pages/doctor_share.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 6: Run tests**

```bash
python -m pytest tests/ -q
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add pages/doctor_share.py
git commit -m "feat: Advanced expander in edit dialog (filing month, doctor_paid, updated_at caption)"
```

---

### Task 5: Refresh button

**Files:**
- Modify: `pages/doctor_share.py` — `render` function, table section

- [ ] **Step 1: Add refresh button inline with the entry-count line**

In `render`, find the `st.markdown(...)` call that shows the entry count (currently inside the `else:` branch of `elif full_df.empty:`):

```python
        st.markdown(
            f"**{len(df)}** {'entry' if len(df) == 1 else 'entries'}{filter_note}"
            f"&ensp;·&ensp;{paid_ct} paid&ensp;·&ensp;{unpaid_ct} unpaid&ensp;·&ensp;{non_maa_ct} non-MAA"
        )
```

Replace with a two-column layout:

```python
        _cnt_col, _ref_col = st.columns([8, 1])
        _cnt_col.markdown(
            f"**{len(df)}** {'entry' if len(df) == 1 else 'entries'}{filter_note}"
            f"&ensp;·&ensp;{paid_ct} paid&ensp;·&ensp;{unpaid_ct} unpaid&ensp;·&ensp;{non_maa_ct} non-MAA"
        )
        if _ref_col.button("🔄", key="ds_refresh", help="Refresh entries"):
            st.rerun()
```

- [ ] **Step 2: Verify syntax**

```bash
python -m py_compile pages/doctor_share.py && echo "OK"
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
git commit -m "feat: add refresh button to Doctor Share entry list"
```

---

### Task 6: Bulk actions — Unmark Paid + Change Month

**Files:**
- Modify: `pages/doctor_share.py` — `render` function, bulk action bar

**Current layout:** `[pay month input + ✔ Mark Paid] | [✖ Unmark Paid] | [🗑 Delete]` — wait, the current layout is `[2, 2, 1]` columns with Mark Paid and Delete.

**New layout:** 4 groups across a wider column set: Mark Paid | Unmark Paid | Change Month | Delete.

- [ ] **Step 1: Replace the bulk action bar**

Find the `if selected_ids:` block in `render`. It currently reads:

```python
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
```

Replace the entire block with:

```python
        if selected_ids:
            n = len(selected_ids)
            _ba1, _ba2, _ba3, _ba4 = st.columns([2.5, 1.5, 2.5, 1])

            with _ba1:
                default_pay = selected_months[-1] if len(selected_months) == 1 else ""
                pay_month_input = st.text_input(
                    "Payment month (YYYY-MM)", value=default_pay,
                    placeholder="YYYY-MM", key="pay_month_input",
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
                    "Move to month (YYYY-MM)",
                    placeholder="YYYY-MM", key="move_month_input",
                )
                if st.button(f"📅 Change Month ({n})", width="stretch", key="bulk_change_month"):
                    import re as _re
                    if not move_month_input or not _re.fullmatch(r"\d{4}-\d{2}", move_month_input):
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
```

- [ ] **Step 2: Verify syntax**

```bash
python -m py_compile pages/doctor_share.py && echo "OK"
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
git commit -m "feat: add Unmark Paid and Change Month bulk actions"
```

---

### Task 7: QoL polish — payment month in table row

**Files:**
- Modify: `pages/doctor_share.py` — `render` function, table row loop

**What:** When `doctor_paid == 1` and `doctor_payment_month` is set, show the payment month as small grey text beneath the 🟢 dot in the last column.

- [ ] **Step 1: Update the paid indicator cell in the row loop**

Find the block at the bottom of the `for _, _row in _page_rows.iterrows():` loop:

```python
            if _row["doctor_paid"] == 1:
                _c[_ci].markdown("<div style='padding-top:4px;font-size:1rem'>🟢</div>", unsafe_allow_html=True)
            elif _status_val == "Claim Paid":
                _c[_ci].markdown("<div style='padding-top:4px;font-size:1rem'>🟡</div>", unsafe_allow_html=True)
```

Replace with:

```python
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
```

- [ ] **Step 2: Verify syntax**

```bash
python -m py_compile pages/doctor_share.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 3: Run all tests**

```bash
python -m pytest tests/ -q
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add pages/doctor_share.py
git commit -m "feat: show payment month under paid indicator in Doctor Share table"
```

---

## Done

Final verification:

```bash
# All tests green
python -m pytest tests/ -q

# All page modules importable
python -c "from pages import doctor_share; import db, reports; print('all imports OK')"

# app.py unchanged
wc -l app.py
```
