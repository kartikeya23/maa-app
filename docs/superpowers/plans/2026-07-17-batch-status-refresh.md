# Batch MAA Status Refresh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One-click re-inference of MAA payment statuses for all linked, unpaid entries in the Doctor Share month view, with a results summary panel.

**Architecture:** A new pure-DB helper `batch_refresh_maa_status` in `db.py` loops entries through the existing `infer_maa_status` and writes changes via the existing `update_doctor_expense` (which preserves `doctor_expenses_log` history). The UI adds a button to the counts row of `ui/doctor_share.py` that collects eligible entries from the unfiltered month set, runs the helper, stashes results in session state, and reruns; a dismissible panel renders the results.

**Tech Stack:** Python 3, sqlite3, pandas, Streamlit, pytest.

**Spec:** `docs/superpowers/specs/2026-07-17-batch-status-refresh-design.md`

## Global Constraints

- Batch refresh is **local re-infer only** — no portal fetching.
- Eligible set: entries with a TID link and `maa_status != 'Claim Paid'`, from the **unfiltered** month set (`full_df`), ignoring status/paid/name-search view filters.
- No cross-TID matching or relink suggestions (multiple TIDs for the same patient+date are separate admissions).
- Writes must go through `update_doctor_expense` so `doctor_expenses_log` history is kept.
- `infer_maa_status` returning `None` (TID absent from `claims`) leaves the status untouched.
- Single full test run at the end of implementation (per project preference: no intermediate full-suite runs; per-test runs during TDD are fine).

---

### Task 1: `batch_refresh_maa_status` DB helper

**Files:**
- Modify: `db.py` (add function after `update_doctor_expense`, ~line 898)
- Test: `tests/test_doctor_share_db.py` (append at end)

**Interfaces:**
- Consumes (already in `db.py`):
  - `infer_maa_status(conn: sqlite3.Connection, tid: str) -> str | None`
  - `update_doctor_expense(conn: sqlite3.Connection, row_id: int, fields: dict) -> None`
  - `save_doctor_expense(conn, month, patient_name, admission_date, ..., maa_status=None, tid=None) -> int` (used by tests)
- Produces (Task 2 relies on this exact signature):
  - `batch_refresh_maa_status(conn: sqlite3.Connection, entries: list[dict]) -> list[dict]`
    - each input dict: `{"id": int, "tid": str, "maa_status": str | None, "patient_name": str}`
    - each result dict: `{"id": int, "patient_name": str, "tid": str, "old_status": str | None, "new_status": str | None, "changed": bool}`
    - when unchanged (including `None` inference), `new_status` equals `old_status`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_doctor_share_db.py`:

```python
# ── batch_refresh_maa_status ──────────────────────────────────────────────────

def _insert_claim(conn, tid, status, approved, paid, pkg_code="PKG1", claim_number="CLM1"):
    conn.execute(
        """INSERT INTO claims (
               tid, patient_name, date_of_admission, date_of_discharge,
               pkg_code, pkg_name, pkg_rate, status, approved_amount, paid_amount, claim_number,
               hospital_name, hospital_code, hospital_type, time_of_admission, time_of_discharge,
               modified_date, id_type, id_number, district_name, aadhaar_number, aadhaar_name,
               policy_year, mobile_no, payment_type, query_raised, gender, age, payment_date,
               bank_utr_number, tpa_name, claim_processor_name, claim_processor_ssoid,
               pkg_speciality_name, package_remark, claim_submission_dt, last_ingested_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   '', '', '', '', '', '', '', '', '', '', '',
                   '2025-2026', '', '', 0, 'M', 45, '', '', '', '', '', '', '', '', '')""",
        (tid, "Batch Patient", "2025-06-01", "2025-06-04",
         pkg_code, "Pkg", 10000.0, status, approved, paid, claim_number),
    )
    conn.commit()


def test_batch_refresh_writes_and_logs_changed_status(mem_db):
    conn = mem_db
    _insert_claim(conn, "TB01", "Claim Paid", 10000.0, 10000.0)
    row_id = db.save_doctor_expense(
        conn, "2025-06", "Batch Patient", "2025-06-01",
        maa_status="Claim Raised", tid="TB01",
    )

    results = db.batch_refresh_maa_status(conn, [
        {"id": row_id, "tid": "TB01", "maa_status": "Claim Raised", "patient_name": "Batch Patient"},
    ])

    assert results == [{
        "id": row_id, "patient_name": "Batch Patient", "tid": "TB01",
        "old_status": "Claim Raised", "new_status": "Claim Paid", "changed": True,
    }]
    status = conn.execute(
        "SELECT maa_status FROM doctor_expenses WHERE id = ?", (row_id,)
    ).fetchone()[0]
    assert status == "Claim Paid"
    log = conn.execute(
        """SELECT field, old_value, new_value FROM doctor_expenses_log
           WHERE expense_id = ?""", (row_id,)
    ).fetchall()
    assert ("maa_status", "Claim Raised", "Claim Paid") in log


def test_batch_refresh_unchanged_row_not_rewritten(mem_db):
    conn = mem_db
    _insert_claim(conn, "TB02", "Claim Raised", 0.0, 0.0)
    row_id = db.save_doctor_expense(
        conn, "2025-06", "Batch Patient", "2025-06-01",
        maa_status="Claim Raised", tid="TB02",
    )
    conn.execute(
        "UPDATE doctor_expenses SET updated_at = '2000-01-01 00:00:00' WHERE id = ?",
        (row_id,),
    )
    conn.commit()

    results = db.batch_refresh_maa_status(conn, [
        {"id": row_id, "tid": "TB02", "maa_status": "Claim Raised", "patient_name": "Batch Patient"},
    ])

    assert results[0]["changed"] is False
    assert results[0]["new_status"] == "Claim Raised"
    updated_at = conn.execute(
        "SELECT updated_at FROM doctor_expenses WHERE id = ?", (row_id,)
    ).fetchone()[0]
    assert updated_at == "2000-01-01 00:00:00", "unchanged row must not be rewritten"
    log_count = conn.execute(
        "SELECT COUNT(*) FROM doctor_expenses_log WHERE expense_id = ?", (row_id,)
    ).fetchone()[0]
    assert log_count == 0


def test_batch_refresh_none_inference_leaves_status_untouched(mem_db):
    conn = mem_db
    # No claims rows for this TID at all → infer_maa_status returns None.
    row_id = db.save_doctor_expense(
        conn, "2025-06", "Batch Patient", "2025-06-01",
        maa_status="Claim Raised", tid="TGONE",
    )

    results = db.batch_refresh_maa_status(conn, [
        {"id": row_id, "tid": "TGONE", "maa_status": "Claim Raised", "patient_name": "Batch Patient"},
    ])

    assert results[0]["changed"] is False
    assert results[0]["new_status"] == "Claim Raised"
    status = conn.execute(
        "SELECT maa_status FROM doctor_expenses WHERE id = ?", (row_id,)
    ).fetchone()[0]
    assert status == "Claim Raised"


def test_batch_refresh_mixed_entries(mem_db):
    conn = mem_db
    _insert_claim(conn, "TB03", "Claim Paid", 10000.0, 10000.0)
    id_change = db.save_doctor_expense(
        conn, "2025-06", "Changer", "2025-06-01", maa_status="Claim Raised", tid="TB03",
    )
    id_gone = db.save_doctor_expense(
        conn, "2025-06", "Goner", "2025-06-02", maa_status=None, tid="TGONE2",
    )

    results = db.batch_refresh_maa_status(conn, [
        {"id": id_change, "tid": "TB03", "maa_status": "Claim Raised", "patient_name": "Changer"},
        {"id": id_gone, "tid": "TGONE2", "maa_status": None, "patient_name": "Goner"},
    ])

    assert [r["changed"] for r in results] == [True, False]
    assert results[1]["old_status"] is None
    assert results[1]["new_status"] is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source .venv/bin/activate && pytest tests/test_doctor_share_db.py -k batch_refresh -v`
Expected: 4 FAILs with `AttributeError: module 'db' has no attribute 'batch_refresh_maa_status'`

- [ ] **Step 3: Implement `batch_refresh_maa_status`**

Add to `db.py`, directly after `update_doctor_expense` (end of the "Doctor Share write operations" section, ~line 898):

```python
def batch_refresh_maa_status(conn: sqlite3.Connection, entries: list[dict]) -> list[dict]:
    """Re-infer maa_status from ingested claims for a batch of doctor_expenses entries.

    entries: dicts with id, tid, maa_status (current), patient_name.
    Writes via update_doctor_expense (logged) only when the inferred status
    exists and differs; a None inference (TID absent from claims) leaves the
    row untouched. Returns per-entry results:
    {id, patient_name, tid, old_status, new_status, changed}.
    """
    results = []
    for entry in entries:
        old_status = entry.get("maa_status")
        inferred = infer_maa_status(conn, entry["tid"])
        changed = inferred is not None and inferred != old_status
        if changed:
            update_doctor_expense(conn, entry["id"], {"maa_status": inferred})
        results.append({
            "id": entry["id"],
            "patient_name": entry.get("patient_name"),
            "tid": entry["tid"],
            "old_status": old_status,
            "new_status": inferred if changed else old_status,
            "changed": changed,
        })
    n_changed = sum(1 for r in results if r["changed"])
    logger.info("Batch MAA status refresh: %d/%d entries updated", n_changed, len(results))
    return results
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_doctor_share_db.py -k batch_refresh -v`
Expected: 4 PASS

- [ ] **Step 5: Commit**

```bash
git add db.py tests/test_doctor_share_db.py
git commit -m "feat: batch_refresh_maa_status DB helper for bulk status re-inference"
```

---

### Task 2: Doctor Share UI — refresh button and results panel

**Files:**
- Modify: `ui/doctor_share.py:554-560` (counts row) and directly below it (results panel)

**Interfaces:**
- Consumes (from Task 1):
  - `db.batch_refresh_maa_status(conn, entries: list[dict]) -> list[dict]` — input dicts `{"id": int, "tid": str, "maa_status": str | None, "patient_name": str}`; result dicts `{"id", "patient_name", "tid", "old_status", "new_status", "changed"}`.
- Produces: session-state key `st.session_state["ds_batch_results"]` holding the results list; no other module reads it.

**Context:** `full_df` (the unfiltered month set) is built at `ui/doctor_share.py:308` and is in scope at the counts row. This whole block is inside the `else:` branch guarded by `not full_df.empty`, so `full_df` always has rows here. `pd` and `db` are already imported at the top of the module.

- [ ] **Step 1: Replace the counts row with a three-column version including the batch button**

Current code at `ui/doctor_share.py:554-560`:

```python
        _cnt_col, _ref_col = st.columns([8, 1])
        _cnt_col.markdown(
            f"**{len(df)}** {'entry' if len(df) == 1 else 'entries'}{filter_note}"
            f"&ensp;·&ensp;{paid_ct} paid&ensp;·&ensp;{unpaid_ct} unpaid&ensp;·&ensp;{non_maa_ct} non-MAA"
        )
        if _ref_col.button("🔄", key="ds_refresh", help="Refresh entries"):
            st.rerun()
```

Replace with:

```python
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
```

- [ ] **Step 2: Add the dismissible results panel directly below the counts row**

Insert immediately after the block above (before `df_r = df.reset_index(drop=True)`):

```python
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
```

- [ ] **Step 3: Smoke-check the module imports cleanly**

Run: `source .venv/bin/activate && python -c "import ui.doctor_share"`
Expected: no output, exit 0. (Full app behavior is verified in Task 3.)

- [ ] **Step 4: Commit**

```bash
git add ui/doctor_share.py
git commit -m "feat: batch MAA status refresh button + results panel in Doctor Share"
```

---

### Task 3: Final verification

**Files:** none new — verification only.

- [ ] **Step 1: Run the full test suite once (project preference: single run at the end)**

Run: `source .venv/bin/activate && pytest -v`
Expected: all tests PASS, including the 4 new `batch_refresh` tests.

- [ ] **Step 2: Manual UI verification**

Run: `streamlit run app.py`, open the Doctor Share page, then verify:
1. The "🔄 Refresh MAA Statuses" button appears in the counts row next to the 🔄 icon.
2. Clicking it with eligible entries shows the results panel with a "N updated · M unchanged" summary and, if anything changed, a Patient / Status table with `Old → New` values.
3. The ✕ button dismisses the panel.
4. With no eligible entries (e.g. a month where everything is Claim Paid or unlinked), a toast "No unpaid linked entries to refresh." appears and no panel renders.

- [ ] **Step 3: Fix anything found, re-run affected tests, commit fixes if any**
