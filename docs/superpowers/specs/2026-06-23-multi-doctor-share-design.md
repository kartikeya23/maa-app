# Multi-Doctor Share Design

**Date:** 2026-06-23
**Scope:** Expand the Doctor Share page to support multiple visiting consultants, one doctor per case.

---

## Context

The current Doctor Share page is hardcoded to a single doctor (Dr. Kavesh). The `doctor_expenses` table has no doctor identity field. The hospital now works with multiple visiting consultants, each with their own default share percentage. Each admission still belongs to exactly one doctor.

---

## Approach

A `doctor_name` column is added to `doctor_expenses`. A doctor selectbox in the sidebar scopes the entire page — months list, entries table, bulk actions, metrics, and downloads — to the selected doctor. The list of doctors and their default percentages is a hardcoded dict in `ui/doctor_share.py`.

---

## Section 1: Data Layer (`db.py`)

### Migration

`init_db()` runs this migration on every startup, safe to re-run:

```python
try:
    conn.execute(
        "ALTER TABLE doctor_expenses ADD COLUMN doctor_name TEXT NOT NULL DEFAULT 'Dr. Kavesh'"
    )
    conn.commit()
except sqlite3.OperationalError:
    pass  # column already exists
```

SQLite backfills all existing rows with `'Dr. Kavesh'`.

### Function changes

| Function | Change |
|---|---|
| `get_doctor_expenses(conn, months, doctor_name)` | Add `WHERE de.doctor_name = ?` |
| `get_doctor_expense_months(conn, doctor_name)` | Scope to selected doctor |
| `save_doctor_expense(..., doctor_name)` | Accept and store `doctor_name` |
| `update_doctor_expense(...)` | Add `doctor_name` to allowed fields |

---

## Section 2: Doctor Registry (`ui/doctor_share.py`)

Hardcoded dict at the top of the file — single place to add/rename doctors:

```python
DOCTORS = {
    "Dr. Kavesh": 0.40,
    "Dr. X":      0.35,
}
```

The dict value is the default `doctor_pct` used to pre-fill the "Doctor %" input when creating a new entry. Users can still override it per entry; the actual value is stored in the DB row as always.

---

## Section 3: UI Changes (`ui/doctor_share.py`)

### Sidebar
- Doctor selectbox is the first sidebar control, above the month filter.
- Selecting a doctor re-scopes: month list (`get_doctor_expense_months`), entries query (`get_doctor_expenses`), metrics, and download buttons all operate on that doctor's data only.

### Page title
`st.title(f"Doctor Share — {selected_doctor}")` replaces the hardcoded string.

### Add Entry form
- The selected doctor is shown as read-only info (sidebar already controls which doctor you're adding for).
- "Doctor %" input pre-fills with `DOCTORS[selected_doctor]` instead of hardcoded 0.40.
- `save_doctor_expense()` called with `doctor_name=selected_doctor`.

### Report filenames
- Slug: display name with spaces removed, e.g. `"Dr. Kavesh"` → `"DrKavesh"`, `"Dr. X"` → `"DrX"`.
- Pattern: `DoctorShare_Internal_<Slug>_YYYY-MM.xlsx` and `DoctorShare_<Slug>_YYYY-MM.xlsx`.

---

## Out of Scope

- Managing doctors from the UI (add/remove/rename) — hardcoded list only for now.
- Multiple doctors per case — one doctor per entry only.
- Per-doctor reporting on a single combined sheet.
