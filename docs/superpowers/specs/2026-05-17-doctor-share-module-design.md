# Doctor Share Module — Design Spec

**Date:** 2026-05-17  
**Status:** Draft  

---

## Context

The hospital currently tracks MAA insurance claim payments in the MAA Payment Record Manager (Streamlit + SQLite). Separately, a monthly Excel spreadsheet is maintained to calculate the consulting doctor's (Dr. Kavesh) share of each admission's revenue — a process that involves re-entering patient names, admission dates, and MAA payment amounts that are already in the system, then manually entering expenses from physical bills.

This module integrates the doctor share workflow directly into the existing app, eliminating the re-entry of MAA data and providing a structured expense entry and payment tracking interface.

---

## Scope

- Single consulting doctor: Dr. Kavesh
- Integrated as a new page in the existing Streamlit app
- New `doctor_expenses` table in the existing `maa.db`
- Two Excel exports per month: internal (full) and doctor-facing copy

Out of scope: multi-doctor support, non-expense revenue tracking, PDF generation.

---

## Data Model

### New table: `doctor_expenses`

```sql
CREATE TABLE doctor_expenses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tid TEXT UNIQUE,                   -- NULL for non-MAA patients
    patient_name TEXT,                 -- stored for non-MAA; snapshot from claims at match time for MAA (not auto-synced)
    admission_date TEXT,               -- same
    month TEXT NOT NULL,               -- YYYY-MM (which report month this entry belongs to)
    hosp_ex REAL DEFAULT 0,
    pharma_ex REAL DEFAULT 0,
    dialysis_ex REAL DEFAULT 0,
    doctor_pct REAL DEFAULT 0.4,       -- share percentage (MAA patients only)
    doctor_flat REAL,                  -- if set: overrides pct for MAA; IS the share for non-MAA
    comments TEXT,
    maa_status TEXT,                   -- cached from claims at time of entry; NULL for non-MAA
    doctor_paid INTEGER DEFAULT 0,     -- 0 = unpaid, 1 = paid to doctor
    doctor_payment_month TEXT,         -- YYYY-MM when doctor was actually paid
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
```

### Calculated fields (never stored, derived at query time)

| Field | Formula |
|---|---|
| `total_ex` | `hosp_ex + pharma_ex + dialysis_ex` |
| `maa_payment` | `SUM(paid_amount for TID) × 0.9` (net after TDS); `NULL` for non-MAA |
| `doctor_share` | If `doctor_flat` set → `doctor_flat`; else `doctor_pct × (maa_payment − total_ex)` |
| `hospital_share` | `maa_payment − doctor_share − total_ex`; `NULL` for non-MAA |

### Default calculation rules

- **Default:** 40% of `(maa_payment − total_ex)`
- **Permanent catheter cases:** flat ₹11,000 (enter via `doctor_flat`)
- **Manual override at month-end:** set `doctor_flat` to any value; clears formula-based calculation

---

## New DB helper functions (in `db.py`)

- `init_doctor_expenses_table()` — create table if not exists (called on startup)
- `get_doctor_expenses(month)` → list of rows joined with claims data
- `upsert_doctor_expense(tid, month, hosp_ex, pharma_ex, dialysis_ex, doctor_pct, doctor_flat, comments, maa_status, patient_name, admission_date)`
- `add_non_maa_expense(patient_name, admission_date, month, hosp_ex, pharma_ex, dialysis_ex, doctor_flat, comments)`
- `mark_doctor_paid(ids: list[int], payment_month: str)` — bulk update
- `delete_doctor_expense(id)`
- `search_claims_for_matching(name: str, month: str, expand: bool)` — fuzzy name search within month (or ±1 month if `expand=True`), returns TID, patient_name, admission_date, discharge_date, maa_paid_amount, status

---

## UI: "Doctor Share" page (`pages/doctor_share.py`)

### Layout

```
[Month picker]  [Status filter: All | MAA Paid | Approved | Query | Unpaid to Doctor]

[Add Entry]  [Mark selected as paid ▾]

┌─────────────────────────────────────────────────────────────────────────────────┐
│ ☐ │ Patient │ Adm. Date │ Hosp │ Pharma │ Dial │ Total │ MAA Pmt │ Dr Share │ … │
├─────────────────────────────────────────────────────────────────────────────────┤
│ ☐ │ ...     │ ...       │      │        │      │       │         │          │   │
│ ☐ │ [Non-MAA] ...                                                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│ TOTALS                                                                          │
└─────────────────────────────────────────────────────────────────────────────────┘

[Internal Export]  [Doctor Copy Export]
```

Table columns:
- Checkbox (for bulk select)
- Patient Name, Admission Date
- Hosp Ex, Pharma Ex, Dialysis Ex (inline-editable)
- Total Ex (calculated, read-only)
- MAA Payment (read-only; blank for non-MAA)
- Doctor Share (inline-editable; shows formula result unless overridden)
- Hospital Share (read-only; blank for non-MAA)
- MAA Status
- Doctor Paid (icon/badge)
- Comments (inline-editable)
- Delete (row action)

### Add Entry panel (expander or side panel)

Toggle: **[MAA Patient ●] [ Non-MAA Patient ]** (MAA is default)

**MAA Patient flow:**
1. Patient name input → live search within selected month's admissions  
   - Default: exact month only  
   - If no results: "Expand search to ±1 month?" checkbox auto-appears  
2. Candidate list: Patient Name | TID | Adm. Date | Disch. Date | MAA Paid | Status  
3. Select candidate → fields lock in  
4. Enter: Hosp Ex, Pharma Ex, Dialysis Ex, Comments  
5. Doctor share auto-previews  
6. [Save]

**Non-MAA Patient flow:**
1. Enter: Patient Name, Admission Date
2. Enter: Hosp Ex, Pharma Ex, Dialysis Ex (optional), Comments
3. Enter: Doctor Share (flat, required)
4. [Save]

**Both flows:**
- Validate inputs (e.g. expenses ≥ 0, doctor share ≤ total ex for MAA)
- On save: upsert into `doctor_expenses` with appropriate fields; for MAA also cache `maa_status` from claims at time of entry
- Option to save and add another (resets form but keeps month and MAA/non-MAA selection)

### Bulk "Mark as paid" action

Select rows → click "Mark selected as paid ▾" → dropdown asks for payment month (YYYY-MM) → confirms count → updates `doctor_paid = 1` and `doctor_payment_month` for all selected rows.

---

## Reports (`reports.py` additions)

Two new report functions following existing openpyxl style (dark blue header, alternating rows, ₹ formatting, totals row):

### `generate_doctor_internal(entries)` → bytes
Columns: No. | Patient Name | Admission Date | Hosp Ex | Pharma Ex | Dialysis Ex | Total Ex | MAA Payment | Doctor Share | Hospital Share | MAA Status | Doctor Paid | Dr Payment Month | Comments

### `generate_doctor_copy(entries)` → bytes
Columns: No. | Patient Name | Admission Date | Hosp Ex | Pharma Ex | Dialysis Ex | Total Ex | MAA Payment | Doctor Share | Hospital Share | Comments

Both accept the same `entries` list; the doctor copy simply omits the payment tracking columns. Non-MAA rows show blank for MAA Payment and Hospital Share. Both have totals rows (per status filter) at the bottom for expenses, MAA payment, doctor share, hospital share.

---

## File changes

| File | Change |
|---|---|
| `db.py` | Add `doctor_expenses` table init + 6 new helper functions |
| `reports.py` | Add `generate_doctor_internal()` and `generate_doctor_copy()` |
| `app.py` | Add "Doctor Share" to sidebar navigation; call `init_doctor_expenses_table()` on startup |
| `pages/doctor_share.py` | New file — full page implementation |

---

## Verification

1. Run app: `source .venv/bin/activate && streamlit run app.py`
2. Navigate to "Doctor Share" page
3. Select current month → confirm existing MAA admissions are searchable
4. Add an MAA entry: search by name, select match, enter expenses → verify calculated doctor share and hospital share are correct
5. Add a non-MAA entry → verify it appears with blank MAA Payment/Hospital Share
6. Edit a row inline (change expense value) → verify totals update
7. Select multiple rows → mark as paid → verify badge and payment month update
8. Change a row's Doctor Share to a flat override → verify formula is bypassed
9. Download Internal Export → verify all columns, ₹ formatting, totals row
10. Download Doctor Copy → verify payment tracking columns are absent
11. Apply status filter → verify table filters correctly
