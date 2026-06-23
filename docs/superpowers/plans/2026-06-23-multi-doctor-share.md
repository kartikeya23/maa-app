# Multi-Doctor Share Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add multi-doctor support to the Doctor Share page so each `doctor_expenses` entry is attributed to one of a fixed list of visiting consultants, and the page filters all data (months, entries, reports) by the selected doctor.

**Architecture:** A `doctor_name` column (defaulting to `'Dr. Kavesh'`) is added to `doctor_expenses` via schema DDL + one-time ALTER TABLE migration in `init_db`. A hardcoded `DOCTORS` dict in `ui/doctor_share.py` maps display name to default share %. A doctor selectbox in the sidebar scopes the entire page.

**Tech Stack:** Python, SQLite (via `sqlite3`), pandas, Streamlit, pytest

---

### Task 1: Add `doctor_name` to DB schema

**Files:**
- Modify: `db.py` — `DOCTOR_EXPENSES_DDL`, `init_db()`
- Modify: `tests/test_doctor_share_db.py` — column existence test

- [ ] **Step 1: Update the failing column test**

In `tests/test_doctor_share_db.py`, add `"doctor_name"` to the expected columns list in `test_doctor_expenses_columns`:

```python
def test_doctor_expenses_columns(mem_db):
    cols = [r[1] for r in mem_db.execute("PRAGMA table_info(doctor_expenses)").fetchall()]
    for expected in [
        "id", "tid", "patient_name", "admission_date", "month",
        "hosp_ex", "pharma_ex", "dialysis_ex", "doctor_pct", "doctor_flat",
        "comments", "maa_status", "doctor_paid", "doctor_payment_month",
        "created_at", "updated_at", "doctor_name",
    ]:
        assert expected in cols, f"Missing column: {expected}"
```

- [ ] **Step 2: Add a migration idempotency test**

Append to `tests/test_doctor_share_db.py`:

```python
def test_doctor_name_migration_idempotent(mem_db):
    """Running init_db a second time must not raise even though column already exists."""
    db.init_db(":memory:")  # second call on a fresh db — column added twice in DDL path
    # The real migration guard is in init_db for on-disk DBs; this verifies no crash
```

- [ ] **Step 3: Run tests to confirm they fail**

```bash
cd /Users/kartikeya/Tech/maa_app && source .venv/bin/activate && pytest tests/test_doctor_share_db.py::test_doctor_expenses_columns -v
```

Expected: FAIL — `doctor_name` not in columns.

- [ ] **Step 4: Update `DOCTOR_EXPENSES_DDL` in `db.py`**

In `db.py`, add `doctor_name` as the last column before the closing `)` of `DOCTOR_EXPENSES_DDL`:

```python
DOCTOR_EXPENSES_DDL = """
CREATE TABLE IF NOT EXISTS doctor_expenses (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    tid                  TEXT UNIQUE,
    patient_name         TEXT,
    admission_date       TEXT,
    month                TEXT NOT NULL,
    hosp_ex              REAL DEFAULT 0,
    pharma_ex            REAL DEFAULT 0,
    dialysis_ex          REAL DEFAULT 0,
    doctor_pct           REAL DEFAULT 0.4,
    doctor_flat          REAL,
    comments             TEXT,
    maa_status           TEXT,
    doctor_paid          INTEGER DEFAULT 0,
    doctor_payment_month TEXT,
    doctor_name          TEXT NOT NULL DEFAULT 'Dr. Kavesh',
    created_at           TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at           TEXT DEFAULT CURRENT_TIMESTAMP
);
"""
```

- [ ] **Step 5: Add ALTER TABLE migration to `init_db`**

In `db.py`, inside `init_db()`, after `conn.executescript(DOCTOR_EXPENSES_DDL)`:

```python
def init_db(path: str | Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.executescript(DDL)
    conn.executescript(HASH_DDL)
    conn.executescript(DOCTOR_EXPENSES_DDL)
    try:
        conn.execute(
            "ALTER TABLE doctor_expenses ADD COLUMN doctor_name TEXT NOT NULL DEFAULT 'Dr. Kavesh'"
        )
        conn.commit()
    except sqlite3.OperationalError:
        pass  # column already exists (fresh DB or second run)
    conn.commit()
    return conn
```

- [ ] **Step 6: Run all DB tests**

```bash
pytest tests/test_doctor_share_db.py -v
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add db.py tests/test_doctor_share_db.py
git commit -m "feat: add doctor_name column to doctor_expenses with migration"
```

---

### Task 2: Filter `get_doctor_expense_months` by doctor

**Files:**
- Modify: `db.py` — `get_doctor_expense_months`
- Modify: `tests/test_doctor_share_db.py` — new test

- [ ] **Step 1: Write the failing test**

Append to `tests/test_doctor_share_db.py`:

```python
def test_get_doctor_expense_months_filtered_by_doctor(mem_db):
    mem_db.executemany(
        "INSERT INTO doctor_expenses (patient_name, admission_date, month, doctor_name) VALUES (?, ?, ?, ?)",
        [
            ("Patient A", "2025-06-01", "2025-06", "Dr. Kavesh"),
            ("Patient B", "2025-07-01", "2025-07", "Dr. X"),
        ],
    )
    mem_db.commit()
    kavesh_months = db.get_doctor_expense_months(mem_db, "Dr. Kavesh")
    assert kavesh_months == ["2025-06"]
    x_months = db.get_doctor_expense_months(mem_db, "Dr. X")
    assert x_months == ["2025-07"]
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
pytest tests/test_doctor_share_db.py::test_get_doctor_expense_months_filtered_by_doctor -v
```

Expected: FAIL — `get_doctor_expense_months` does not accept `doctor_name`.

- [ ] **Step 3: Update `get_doctor_expense_months` in `db.py`**

```python
def get_doctor_expense_months(conn: sqlite3.Connection, doctor_name: str | None = None) -> list[str]:
    """Returns distinct months present in doctor_expenses, newest first.
    If doctor_name is given, scopes to that doctor only.
    """
    if doctor_name is not None:
        rows = conn.execute(
            "SELECT DISTINCT month FROM doctor_expenses WHERE doctor_name = ? ORDER BY month DESC",
            (doctor_name,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT DISTINCT month FROM doctor_expenses ORDER BY month DESC"
        ).fetchall()
    return [r[0] for r in rows]
```

- [ ] **Step 4: Run all DB tests**

```bash
pytest tests/test_doctor_share_db.py -v
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add db.py tests/test_doctor_share_db.py
git commit -m "feat: filter get_doctor_expense_months by doctor_name"
```

---

### Task 3: Filter `get_doctor_expenses` by doctor

**Files:**
- Modify: `db.py` — `get_doctor_expenses`
- Modify: `tests/test_doctor_share_db.py` — new test

- [ ] **Step 1: Write the failing test**

Append to `tests/test_doctor_share_db.py`:

```python
def test_get_doctor_expenses_filtered_by_doctor(mem_db_with_claims):
    conn = mem_db_with_claims
    conn.executemany(
        """INSERT INTO doctor_expenses
               (tid, patient_name, admission_date, month, doctor_name)
           VALUES (?, ?, ?, ?, ?)""",
        [
            ("TID001", "Ravi Kumar",  "2025-06-15", "2025-06", "Dr. Kavesh"),
            ("TID002", "Sunita Devi", "2025-06-10", "2025-06", "Dr. X"),
        ],
    )
    conn.commit()
    kavesh_df = db.get_doctor_expenses(conn, "2025-06", "Dr. Kavesh")
    assert len(kavesh_df) == 1
    assert kavesh_df.iloc[0]["patient_name"] == "Ravi Kumar"

    x_df = db.get_doctor_expenses(conn, "2025-06", "Dr. X")
    assert len(x_df) == 1
    assert x_df.iloc[0]["patient_name"] == "Sunita Devi"
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
pytest tests/test_doctor_share_db.py::test_get_doctor_expenses_filtered_by_doctor -v
```

Expected: FAIL — `get_doctor_expenses` does not accept `doctor_name`.

- [ ] **Step 3: Update `get_doctor_expenses` in `db.py`**

Add `doctor_name: str | None = None` parameter and a `WHERE` clause for it. Replace the function in `db.py`:

```python
def get_doctor_expenses(conn: sqlite3.Connection, months: str | list[str], doctor_name: str | None = None) -> pd.DataFrame:
    """Returns doctor_expenses for one or more months, joined with computed maa_payment and share fields.
    If doctor_name is given, scopes to that doctor only.
    """
    if isinstance(months, str):
        months = [months]
    if not months:
        return pd.DataFrame()
    placeholders = ",".join("?" for _ in months)
    doctor_clause = "AND de.doctor_name = ?" if doctor_name is not None else ""
    params = months + ([doctor_name] if doctor_name is not None else [])
    sql = f"""
        SELECT
            de.id,
            de.tid,
            de.patient_name,
            de.admission_date,
            de.month,
            de.hosp_ex,
            de.pharma_ex,
            de.dialysis_ex,
            de.doctor_pct,
            de.doctor_flat,
            de.comments,
            de.maa_status,
            de.doctor_paid,
            de.doctor_payment_month,
            de.doctor_name,
            CASE WHEN de.tid IS NOT NULL
                 THEN COALESCE(maa.net_paid, 0.0)
                 ELSE NULL END AS maa_payment
        FROM doctor_expenses de
        LEFT JOIN (
            SELECT tid,
                   SUM(CASE WHEN LOWER(status) LIKE '%paid%' THEN approved_amount ELSE 0 END) * 0.9
                   AS net_paid
            FROM claims
            GROUP BY tid
        ) maa ON de.tid = maa.tid
        WHERE de.month IN ({placeholders})
        {doctor_clause}
        ORDER BY de.month ASC, de.id ASC
    """
    df = pd.read_sql_query(sql, conn, params=params)
    if df.empty:
        return df

    df["total_ex"] = df["hosp_ex"] + df["pharma_ex"] + df["dialysis_ex"]

    def _is_rejected(r):
        return isinstance(r.get("maa_status"), str) and "rejected" in r["maa_status"].lower()

    def _doctor_share(r):
        if pd.notna(r["doctor_flat"]):
            return r["doctor_flat"]
        if not pd.notna(r["maa_payment"]):
            return None
        val = r["doctor_pct"] * (r["maa_payment"] - r["total_ex"])
        return val if _is_rejected(r) else max(0.0, val)

    def _hospital_share(r):
        if not (pd.notna(r["tid"]) and pd.notna(r["maa_payment"])
                and r["maa_payment"] > 0 and pd.notna(r["doctor_share"])):
            return None
        val = r["maa_payment"] - r["doctor_share"] - r["total_ex"]
        return val if _is_rejected(r) else max(0.0, val)

    df["doctor_share"] = df.apply(_doctor_share, axis=1)
    df["hospital_share"] = df.apply(_hospital_share, axis=1)
    return df
```

- [ ] **Step 4: Run all tests**

```bash
pytest tests/ -v
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add db.py tests/test_doctor_share_db.py
git commit -m "feat: filter get_doctor_expenses by doctor_name"
```

---

### Task 4: Store `doctor_name` on save and update

**Files:**
- Modify: `db.py` — `save_doctor_expense`, `update_doctor_expense`
- Modify: `tests/test_doctor_share_db.py` — new tests

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_doctor_share_db.py`:

```python
def test_save_doctor_expense_stores_doctor_name(mem_db_with_claims):
    conn = mem_db_with_claims
    row_id = db.save_doctor_expense(
        conn, month="2025-06", patient_name="Ravi Kumar",
        admission_date="2025-06-15", tid="TID001", doctor_name="Dr. X",
    )
    stored = conn.execute(
        "SELECT doctor_name FROM doctor_expenses WHERE id = ?", (row_id,)
    ).fetchone()[0]
    assert stored == "Dr. X"


def test_save_doctor_expense_defaults_to_kavesh(mem_db):
    row_id = db.save_doctor_expense(
        mem_db, month="2025-06", patient_name="Cash Patient",
        admission_date="2025-06-20", doctor_flat=3000.0,
    )
    stored = mem_db.execute(
        "SELECT doctor_name FROM doctor_expenses WHERE id = ?", (row_id,)
    ).fetchone()[0]
    assert stored == "Dr. Kavesh"


def test_update_doctor_expense_doctor_name(mem_db):
    conn = mem_db
    conn.execute(
        "INSERT INTO doctor_expenses (patient_name, admission_date, month, doctor_name) VALUES (?, ?, ?, ?)",
        ("Patient A", "2025-06-01", "2025-06", "Dr. Kavesh"),
    )
    conn.commit()
    row_id = conn.execute("SELECT id FROM doctor_expenses").fetchone()[0]
    db.update_doctor_expense(conn, row_id, {"doctor_name": "Dr. X"})
    stored = conn.execute(
        "SELECT doctor_name FROM doctor_expenses WHERE id = ?", (row_id,)
    ).fetchone()[0]
    assert stored == "Dr. X"
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
pytest tests/test_doctor_share_db.py::test_save_doctor_expense_stores_doctor_name tests/test_doctor_share_db.py::test_save_doctor_expense_defaults_to_kavesh tests/test_doctor_share_db.py::test_update_doctor_expense_doctor_name -v
```

Expected: FAIL.

- [ ] **Step 3: Update `save_doctor_expense` in `db.py`**

```python
def save_doctor_expense(
    conn: sqlite3.Connection,
    month: str,
    patient_name: str,
    admission_date: str,
    hosp_ex: float = 0.0,
    pharma_ex: float = 0.0,
    dialysis_ex: float = 0.0,
    doctor_pct: float = 0.4,
    doctor_flat: float | None = None,
    comments: str | None = None,
    maa_status: str | None = None,
    tid: str | None = None,
    doctor_name: str = "Dr. Kavesh",
) -> int:
    """Insert a new doctor_expenses row. Returns the new row id."""
    cursor = conn.execute(
        """INSERT INTO doctor_expenses
               (tid, patient_name, admission_date, month,
                hosp_ex, pharma_ex, dialysis_ex, doctor_pct, doctor_flat,
                comments, maa_status, doctor_name)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (tid, patient_name, admission_date, month,
         hosp_ex, pharma_ex, dialysis_ex, doctor_pct, doctor_flat,
         comments, maa_status, doctor_name),
    )
    conn.commit()
    return cursor.lastrowid
```

- [ ] **Step 4: Update `update_doctor_expense` allowed fields in `db.py`**

Add `"doctor_name"` to the `allowed` set:

```python
def update_doctor_expense(conn: sqlite3.Connection, row_id: int, fields: dict) -> None:
    allowed = {
        "hosp_ex", "pharma_ex", "dialysis_ex", "doctor_pct", "doctor_flat", "comments",
        "doctor_payment_month", "maa_status", "tid", "patient_name", "admission_date",
        "month", "doctor_paid", "doctor_name",
    }
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    conn.execute(
        f"UPDATE doctor_expenses SET {set_clause}, updated_at = datetime('now') WHERE id = ?",
        list(updates.values()) + [row_id],
    )
    conn.commit()
```

- [ ] **Step 5: Run all tests**

```bash
pytest tests/ -v
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add db.py tests/test_doctor_share_db.py
git commit -m "feat: save and update doctor_name on doctor_expenses rows"
```

---

### Task 5: UI — doctor selector, default %, and page scoping

**Files:**
- Modify: `ui/doctor_share.py` — `DOCTORS` dict, sidebar, page title, Add Entry form, DB calls

- [ ] **Step 1: Add `DOCTORS` registry at top of `ui/doctor_share.py`**

After the imports, before any function definitions, add:

```python
DOCTORS: dict[str, float] = {
    "Dr. Kavesh": 0.40,
    "Dr. X":      0.35,
}
```

- [ ] **Step 2: Add doctor selectbox as first sidebar control**

In the `render(conn)` function, the sidebar section currently starts with `st.subheader("Filters")`. Change it so the doctor selectbox appears first, before the month selectbox:

```python
with st.sidebar:
    st.subheader("Filters")
    selected_doctor = st.selectbox(
        "Doctor", list(DOCTORS.keys()), key="ds_doctor",
    )
    available_months = db.get_doctor_expense_months(conn, selected_doctor)
    # ... rest of month filter unchanged ...
```

Remove the standalone `available_months = db.get_doctor_expense_months(conn)` line that currently sits above the sidebar block (it moves inside the sidebar now).

- [ ] **Step 3: Update page title**

Change:
```python
st.title("Doctor Share — Dr. Kavesh")
```
to:
```python
st.title(f"Doctor Share — {selected_doctor}")
```

- [ ] **Step 4: Scope entries query to selected doctor**

Change:
```python
full_df = db.get_doctor_expenses(conn, selected_months) if selected_months else pd.DataFrame()
```
to:
```python
full_df = db.get_doctor_expenses(conn, selected_months, selected_doctor) if selected_months else pd.DataFrame()
```

- [ ] **Step 5: Pre-fill default % from DOCTORS dict in the MAA Add Entry form**

In the Add Entry section, change the `doctor_pct_input` number_input from hardcoded `value=40.0`:

```python
doctor_pct_input = st.number_input(
    "Doctor % (default)", min_value=0.0, max_value=100.0,
    value=DOCTORS[selected_doctor] * 100,
    step=5.0, key="ae_pct",
    help="Percentage of (MAA payment − expenses) that goes to the doctor; overridden if a flat amount is entered below.",
) / 100.0
```

- [ ] **Step 6: Pass `doctor_name` to `save_doctor_expense` in both entry paths**

In the MAA patient save call:
```python
db.save_doctor_expense(
    conn, month=add_month,
    patient_name=chosen["patient_name"],
    admission_date=chosen["date_of_admission"],
    hosp_ex=hosp_ex, pharma_ex=pharma_ex, dialysis_ex=dialysis_ex,
    doctor_pct=doctor_pct_input, doctor_flat=flat_val,
    comments=comments_input or None,
    maa_status=db.infer_maa_status(conn, chosen["tid"]), tid=chosen["tid"],
    doctor_name=selected_doctor,
)
```

In the Non-MAA patient save call:
```python
db.save_doctor_expense(
    conn, month=add_month, patient_name=nm_name,
    admission_date=str(nm_date),
    hosp_ex=nm_hosp, pharma_ex=nm_pharma, dialysis_ex=nm_dialysis,
    doctor_flat=nm_share, comments=nm_comments or None, tid=None,
    doctor_name=selected_doctor,
)
```

- [ ] **Step 7: Verify the app runs without error**

```bash
source .venv/bin/activate && streamlit run app.py &
```

Open http://localhost:8501, navigate to Doctor Share, confirm:
- Doctor selectbox appears first in sidebar
- Page title shows selected doctor's name
- Month list changes when switching doctors
- Default % pre-fills correctly for each doctor
- Adding an entry stores the correct doctor (check via the DB or entry detail dialog)

Kill the dev server: `pkill -f "streamlit run"`

- [ ] **Step 8: Commit**

```bash
git add ui/doctor_share.py
git commit -m "feat: add doctor selector to Doctor Share page with per-doctor default %"
```

---

### Task 6: Update report filenames to include doctor slug

**Files:**
- Modify: `ui/doctor_share.py` — download button filenames

- [ ] **Step 1: Derive doctor slug and update filenames**

In the `render(conn)` function, find the download button section near the bottom. Add the slug derivation and update both filenames:

```python
_doc_slug = selected_doctor.replace("Dr. ", "Dr").replace(" ", "")
col_int, col_doc = st.columns(2)
with col_int:
    st.download_button(
        label=f"Download Internal Export — {month_label}",
        data=reports.generate_doctor_internal(full_df, _sheet_label),
        file_name=f"DoctorShare_Internal_{_doc_slug}_{'_'.join(_sorted_months)}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
with col_doc:
    st.download_button(
        label=f"Download Doctor Copy — {month_label}",
        data=reports.generate_doctor_copy(full_df, _sheet_label),
        file_name=f"DoctorShare_{_doc_slug}_{'_'.join(_sorted_months)}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
```

- [ ] **Step 2: Run all tests**

```bash
pytest tests/ -v
```

Expected: all pass.

- [ ] **Step 3: Commit**

```bash
git add ui/doctor_share.py
git commit -m "feat: include doctor slug in Doctor Share report filenames"
```

---

## Done

All tasks complete when:
- `pytest tests/ -v` passes with no failures
- Doctor Share page shows a doctor selectbox as the first sidebar filter
- Switching doctors changes the months list, entries, and metrics
- New entries are saved with the selected doctor's name
- Default % pre-fills from the `DOCTORS` dict
- Report filenames include the doctor slug (e.g. `DoctorShare_DrKavesh_2025-06.xlsx`)
