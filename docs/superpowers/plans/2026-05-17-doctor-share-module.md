# Doctor Share Module — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a "Doctor Share" page to the MAA app that auto-populates MAA payment data, allows expense entry with fuzzy patient matching, calculates Dr. Kavesh's monthly share, and generates internal and doctor-facing Excel exports.

**Architecture:** Extend the existing single-file Streamlit app by adding a `doctor_expenses` table to `maa.db`, new query/mutation functions in `db.py`, two new report functions in `reports.py`, and a new `elif page == "Doctor Share":` section in `app.py`. No new top-level Python files except tests.

**Tech Stack:** Python 3.11+, SQLite (sqlite3), Streamlit, pandas, openpyxl, pytest

---

## File Map

| File | Change |
|---|---|
| `db.py` | Add `DOCTOR_EXPENSES_DDL`, extend `init_db()`, add 6 new functions |
| `reports.py` | Add 2 column-def lists, extend `SUMMABLE_COLS`/`AMOUNT_COLS`, add 2 functions |
| `app.py` | Add `import pandas as pd`, add "Doctor Share" to sidebar, add page section |
| `requirements.txt` | Add `pytest>=8.0` |
| `tests/__init__.py` | New (empty) |
| `tests/conftest.py` | New — pytest fixtures for in-memory DB |
| `tests/test_doctor_share_db.py` | New — DB function tests |
| `tests/test_doctor_share_reports.py` | New — report generation tests |

---

## Task 1: Test Infrastructure

**Files:**
- Modify: `requirements.txt`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`

- [ ] **Step 1: Add pytest to requirements.txt**

Replace the full file contents with:
```
openpyxl>=3.1
streamlit>=1.35
pandas>=2.0
plotly>=5.0
pytest>=8.0
```

- [ ] **Step 2: Create tests/\_\_init\_\_.py** (empty file)

- [ ] **Step 3: Create tests/conftest.py**

```python
import pytest
import db


@pytest.fixture
def mem_db():
    conn = db.init_db(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def mem_db_with_claims(mem_db):
    mem_db.executemany(
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
                   '2025-2026', '', '', 0, 'M', 45, '', '', '', '', '', '', '', '')""",
        [
            ("TID001", "Ravi Kumar", "2025-06-15", "2025-06-18",
             "PKG001", "Test Package", 30000.0, "Claim Paid", 27000.0, 27000.0, "CLM001"),
            ("TID002", "Sunita Devi", "2025-06-10", "2025-06-12",
             "PKG002", "Other Package", 20000.0, "Claim Approved", 18000.0, 0.0, "CLM002"),
        ],
    )
    mem_db.commit()
    return mem_db
```

- [ ] **Step 4: Install pytest and verify infrastructure runs**

```bash
cd /Users/kartikeya/Tech/maa_app && source .venv/bin/activate && pip install "pytest>=8.0" && pytest tests/ -v
```
Expected: `no tests ran` or exit 5 (no test files yet). No errors.

- [ ] **Step 5: Commit**

```bash
git add requirements.txt tests/
git commit -m "chore: add pytest and test infrastructure"
```

---

## Task 2: doctor\_expenses Schema + Init

**Files:**
- Modify: `db.py`
- Create: `tests/test_doctor_share_db.py`

- [ ] **Step 1: Write failing tests — create tests/test\_doctor\_share\_db.py**

```python
import pytest
import db


def test_doctor_expenses_table_created(mem_db):
    tables = [r[0] for r in mem_db.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    assert "doctor_expenses" in tables


def test_doctor_expenses_columns(mem_db):
    cols = [r[1] for r in mem_db.execute("PRAGMA table_info(doctor_expenses)").fetchall()]
    for expected in [
        "id", "tid", "patient_name", "admission_date", "month",
        "hosp_ex", "pharma_ex", "dialysis_ex", "doctor_pct", "doctor_flat",
        "comments", "maa_status", "doctor_paid", "doctor_payment_month",
        "created_at", "updated_at",
    ]:
        assert expected in cols, f"Missing column: {expected}"


def test_doctor_expenses_tid_unique_constraint(mem_db):
    mem_db.execute(
        "INSERT INTO doctor_expenses (tid, patient_name, admission_date, month) VALUES (?, ?, ?, ?)",
        ("T001", "Alice", "2025-06-01", "2025-06"),
    )
    mem_db.commit()
    with pytest.raises(Exception):
        mem_db.execute(
            "INSERT INTO doctor_expenses (tid, patient_name, admission_date, month) VALUES (?, ?, ?, ?)",
            ("T001", "Alice Duplicate", "2025-06-05", "2025-06"),
        )
        mem_db.commit()


def test_doctor_expenses_non_maa_allows_null_tid(mem_db):
    mem_db.execute(
        "INSERT INTO doctor_expenses (tid, patient_name, admission_date, month, doctor_flat) VALUES (NULL, ?, ?, ?, ?)",
        ("Non MAA Patient", "2025-06-20", "2025-06", 5000.0),
    )
    mem_db.commit()
    row = mem_db.execute("SELECT tid, patient_name FROM doctor_expenses").fetchone()
    assert row[0] is None
    assert row[1] == "Non MAA Patient"
```

- [ ] **Step 2: Run to confirm they fail**

```bash
pytest tests/test_doctor_share_db.py -v
```
Expected: FAIL — `doctor_expenses` table does not exist.

- [ ] **Step 3: Add DOCTOR\_EXPENSES\_DDL constant to db.py**

Add after the `HASH_DDL` string (after line 26), before `DDL`:
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
    created_at           TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at           TEXT DEFAULT CURRENT_TIMESTAMP
);
"""
```

- [ ] **Step 4: Extend init\_db() to run the new DDL**

Find the `init_db` function (currently ends with `conn.commit()` / `return conn`) and add one line:
```python
def init_db(path: str | Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.executescript(DDL)
    conn.executescript(HASH_DDL)
    conn.executescript(DOCTOR_EXPENSES_DDL)
    conn.commit()
    return conn
```

- [ ] **Step 5: Run tests to confirm they pass**

```bash
pytest tests/test_doctor_share_db.py -v
```
Expected: 4 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add db.py tests/test_doctor_share_db.py
git commit -m "feat: add doctor_expenses schema"
```

---

## Task 3: DB Read Queries

**Files:**
- Modify: `db.py`
- Modify: `tests/test_doctor_share_db.py`

- [ ] **Step 1: Append failing tests to tests/test\_doctor\_share\_db.py**

```python
import pandas as pd


def test_get_doctor_expenses_empty(mem_db):
    df = db.get_doctor_expenses(mem_db, "2025-06")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


def test_get_doctor_expenses_maa_patient(mem_db_with_claims):
    conn = mem_db_with_claims
    conn.execute(
        """INSERT INTO doctor_expenses
               (tid, patient_name, admission_date, month, hosp_ex, pharma_ex, dialysis_ex, maa_status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        ("TID001", "Ravi Kumar", "2025-06-15", "2025-06", 500.0, 1000.0, 0.0, "Claim Paid"),
    )
    conn.commit()
    df = db.get_doctor_expenses(conn, "2025-06")
    assert len(df) == 1
    row = df.iloc[0]
    assert row["total_ex"] == pytest.approx(1500.0)
    assert row["maa_payment"] == pytest.approx(27000.0 * 0.9)   # 24300.0
    assert row["doctor_share"] == pytest.approx(0.4 * (24300.0 - 1500.0))  # 9120.0
    assert row["hospital_share"] == pytest.approx(24300.0 - 9120.0 - 1500.0)  # 13680.0


def test_get_doctor_expenses_flat_override(mem_db_with_claims):
    conn = mem_db_with_claims
    conn.execute(
        """INSERT INTO doctor_expenses
               (tid, patient_name, admission_date, month, hosp_ex, pharma_ex, dialysis_ex, doctor_flat)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        ("TID001", "Ravi Kumar", "2025-06-15", "2025-06", 500.0, 1000.0, 0.0, 11000.0),
    )
    conn.commit()
    df = db.get_doctor_expenses(conn, "2025-06")
    row = df.iloc[0]
    assert row["doctor_share"] == pytest.approx(11000.0)
    assert row["hospital_share"] == pytest.approx(24300.0 - 11000.0 - 1500.0)  # 11800.0


def test_get_doctor_expenses_non_maa(mem_db):
    mem_db.execute(
        """INSERT INTO doctor_expenses (tid, patient_name, admission_date, month, doctor_flat)
           VALUES (NULL, ?, ?, ?, ?)""",
        ("Cash Patient", "2025-06-20", "2025-06", 3000.0),
    )
    mem_db.commit()
    df = db.get_doctor_expenses(mem_db, "2025-06")
    row = df.iloc[0]
    assert pd.isna(row["maa_payment"])
    assert row["doctor_share"] == pytest.approx(3000.0)
    assert pd.isna(row["hospital_share"])


def test_search_claims_for_matching_exact_month(mem_db_with_claims):
    results = db.search_claims_for_matching(mem_db_with_claims, "ravi", "2025-06", expand=False)
    assert len(results) == 1
    assert results[0]["tid"] == "TID001"
    assert results[0]["patient_name"] == "Ravi Kumar"


def test_search_claims_for_matching_expand(mem_db_with_claims):
    results = db.search_claims_for_matching(mem_db_with_claims, "ravi", "2025-07", expand=True)
    assert len(results) == 1
    assert results[0]["tid"] == "TID001"


def test_search_claims_excludes_already_matched(mem_db_with_claims):
    conn = mem_db_with_claims
    conn.execute(
        "INSERT INTO doctor_expenses (tid, patient_name, admission_date, month) VALUES (?, ?, ?, ?)",
        ("TID001", "Ravi Kumar", "2025-06-15", "2025-06"),
    )
    conn.commit()
    results = db.search_claims_for_matching(conn, "ravi", "2025-06", expand=False)
    assert len(results) == 0
```

- [ ] **Step 2: Run to confirm they fail**

```bash
pytest tests/test_doctor_share_db.py -v -k "get_doctor_expenses or search_claims"
```
Expected: FAIL — functions not defined.

- [ ] **Step 3: Add get\_doctor\_expenses to db.py**

Append after `get_total_record_count` (end of file):
```python
# ── Doctor Share queries ──────────────────────────────────────────────────────

def get_doctor_expenses(conn: sqlite3.Connection, month: str) -> pd.DataFrame:
    """Returns all doctor_expenses for the month joined with computed maa_payment and share fields."""
    sql = """
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
        WHERE de.month = ?
        ORDER BY de.id ASC
    """
    df = pd.read_sql_query(sql, conn, params=[month])
    if df.empty:
        return df

    df["total_ex"] = df["hosp_ex"] + df["pharma_ex"] + df["dialysis_ex"]
    df["doctor_share"] = df.apply(
        lambda r: r["doctor_flat"]
        if pd.notna(r["doctor_flat"])
        else r["doctor_pct"] * (r["maa_payment"] - r["total_ex"]),
        axis=1,
    )
    df["hospital_share"] = df.apply(
        lambda r: r["maa_payment"] - r["doctor_share"] - r["total_ex"]
        if pd.notna(r["tid"])
        else None,
        axis=1,
    )
    return df
```

- [ ] **Step 4: Add search\_claims\_for\_matching to db.py**

Append immediately after `get_doctor_expenses`:
```python
def search_claims_for_matching(
    conn: sqlite3.Connection,
    name: str,
    month: str,
    expand: bool = False,
) -> list[dict]:
    """
    Fuzzy-search MAA claims by patient name within the given month.
    If expand=True, also includes the previous and next months.
    Excludes TIDs already present in doctor_expenses.
    """
    months = [month]
    if expand:
        from datetime import timedelta
        dt = datetime.strptime(month + "-01", "%Y-%m-%d")
        prev = (dt.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
        nxt = (dt.replace(day=28) + timedelta(days=4)).strftime("%Y-%m")
        months = [prev, month, nxt]

    placeholders = ",".join("?" for _ in months)
    sql = f"""
        SELECT DISTINCT
            tid,
            patient_name,
            date_of_admission,
            date_of_discharge,
            SUM(CASE WHEN LOWER(status) LIKE '%paid%' THEN approved_amount ELSE 0 END) * 0.9
                AS maa_paid,
            GROUP_CONCAT(DISTINCT status) AS status
        FROM claims
        WHERE LOWER(patient_name) LIKE LOWER(?)
          AND strftime('%Y-%m', date_of_admission) IN ({placeholders})
          AND tid NOT IN (SELECT tid FROM doctor_expenses WHERE tid IS NOT NULL)
        GROUP BY tid
        ORDER BY date_of_admission DESC
    """
    rows = conn.execute(sql, [f"%{name}%"] + months).fetchall()
    cols = ["tid", "patient_name", "date_of_admission", "date_of_discharge", "maa_paid", "status"]
    return [dict(zip(cols, r)) for r in rows]
```

- [ ] **Step 5: Run all tests**

```bash
pytest tests/test_doctor_share_db.py -v
```
Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add db.py tests/test_doctor_share_db.py
git commit -m "feat: add get_doctor_expenses and search_claims_for_matching"
```

---

## Task 4: DB Write Operations

**Files:**
- Modify: `db.py`
- Modify: `tests/test_doctor_share_db.py`

- [ ] **Step 1: Append failing tests to tests/test\_doctor\_share\_db.py**

```python
def test_save_doctor_expense_maa(mem_db_with_claims):
    conn = mem_db_with_claims
    row_id = db.save_doctor_expense(
        conn, month="2025-06", patient_name="Ravi Kumar",
        admission_date="2025-06-15", hosp_ex=500.0, pharma_ex=1000.0, dialysis_ex=0.0,
        doctor_pct=0.4, doctor_flat=None, comments="Test", maa_status="Claim Paid", tid="TID001",
    )
    assert isinstance(row_id, int)
    row = conn.execute(
        "SELECT tid, hosp_ex, pharma_ex, doctor_pct FROM doctor_expenses WHERE id=?", (row_id,)
    ).fetchone()
    assert row == ("TID001", 500.0, 1000.0, 0.4)


def test_save_doctor_expense_non_maa(mem_db):
    row_id = db.save_doctor_expense(
        mem_db, month="2025-06", patient_name="Cash Patient",
        admission_date="2025-06-20", doctor_flat=3000.0,
    )
    assert isinstance(row_id, int)
    row = mem_db.execute(
        "SELECT tid, doctor_flat FROM doctor_expenses WHERE id=?", (row_id,)
    ).fetchone()
    assert row == (None, 3000.0)


def test_update_doctor_expense(mem_db):
    conn = mem_db
    conn.execute(
        "INSERT INTO doctor_expenses (tid, patient_name, admission_date, month, hosp_ex) VALUES (?, ?, ?, ?, ?)",
        ("T001", "Patient A", "2025-06-01", "2025-06", 100.0),
    )
    conn.commit()
    row_id = conn.execute("SELECT id FROM doctor_expenses").fetchone()[0]
    db.update_doctor_expense(conn, row_id, {"hosp_ex": 200.0, "comments": "Updated"})
    row = conn.execute(
        "SELECT hosp_ex, comments FROM doctor_expenses WHERE id=?", (row_id,)
    ).fetchone()
    assert row == (200.0, "Updated")


def test_delete_doctor_expense(mem_db):
    conn = mem_db
    conn.execute(
        "INSERT INTO doctor_expenses (tid, patient_name, admission_date, month) VALUES (?, ?, ?, ?)",
        ("T001", "Patient A", "2025-06-01", "2025-06"),
    )
    conn.commit()
    row_id = conn.execute("SELECT id FROM doctor_expenses").fetchone()[0]
    db.delete_doctor_expense(conn, row_id)
    count = conn.execute("SELECT COUNT(*) FROM doctor_expenses").fetchone()[0]
    assert count == 0


def test_mark_doctor_paid(mem_db):
    conn = mem_db
    conn.executemany(
        "INSERT INTO doctor_expenses (tid, patient_name, admission_date, month) VALUES (?, ?, ?, ?)",
        [("T001", "P1", "2025-06-01", "2025-06"), ("T002", "P2", "2025-06-02", "2025-06")],
    )
    conn.commit()
    ids = [r[0] for r in conn.execute("SELECT id FROM doctor_expenses").fetchall()]
    db.mark_doctor_paid(conn, ids, "2025-06")
    rows = conn.execute("SELECT doctor_paid, doctor_payment_month FROM doctor_expenses").fetchall()
    assert all(r[0] == 1 for r in rows)
    assert all(r[1] == "2025-06" for r in rows)
```

- [ ] **Step 2: Run to confirm they fail**

```bash
pytest tests/test_doctor_share_db.py -v -k "save_doctor or update_doctor or delete_doctor or mark_doctor"
```
Expected: FAIL — functions not defined.

- [ ] **Step 3: Add write functions to db.py**

Append after `search_claims_for_matching`:
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
) -> int:
    """Insert a new doctor_expenses row. Returns the new row id."""
    cursor = conn.execute(
        """INSERT INTO doctor_expenses
               (tid, patient_name, admission_date, month,
                hosp_ex, pharma_ex, dialysis_ex, doctor_pct, doctor_flat,
                comments, maa_status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (tid, patient_name, admission_date, month,
         hosp_ex, pharma_ex, dialysis_ex, doctor_pct, doctor_flat,
         comments, maa_status),
    )
    conn.commit()
    return cursor.lastrowid


def update_doctor_expense(conn: sqlite3.Connection, id: int, fields: dict) -> None:
    """Update mutable fields on a doctor_expenses row. Allowed keys: hosp_ex, pharma_ex, dialysis_ex, doctor_pct, doctor_flat, comments."""
    allowed = {"hosp_ex", "pharma_ex", "dialysis_ex", "doctor_pct", "doctor_flat", "comments"}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    conn.execute(
        f"UPDATE doctor_expenses SET {set_clause}, updated_at = datetime('now') WHERE id = ?",
        list(updates.values()) + [id],
    )
    conn.commit()


def delete_doctor_expense(conn: sqlite3.Connection, id: int) -> None:
    conn.execute("DELETE FROM doctor_expenses WHERE id = ?", (id,))
    conn.commit()


def mark_doctor_paid(conn: sqlite3.Connection, ids: list[int], payment_month: str) -> None:
    """Bulk-mark doctor_expenses rows as paid to doctor."""
    placeholders = ",".join("?" for _ in ids)
    conn.execute(
        f"UPDATE doctor_expenses SET doctor_paid = 1, doctor_payment_month = ?, updated_at = datetime('now') WHERE id IN ({placeholders})",
        [payment_month] + ids,
    )
    conn.commit()
```

- [ ] **Step 4: Run all DB tests**

```bash
pytest tests/test_doctor_share_db.py -v
```
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add db.py tests/test_doctor_share_db.py
git commit -m "feat: add doctor_expenses write operations"
```

---

## Task 5: Report Generation

**Files:**
- Modify: `reports.py`
- Create: `tests/test_doctor_share_reports.py`

- [ ] **Step 1: Write failing tests — create tests/test\_doctor\_share\_reports.py**

```python
import io
import pandas as pd
import pytest
from openpyxl import load_workbook
import reports


@pytest.fixture
def sample_entries():
    return pd.DataFrame([
        {
            "id": 1, "tid": "T001", "patient_name": "Ravi Kumar",
            "admission_date": "2025-06-15", "month": "2025-06",
            "hosp_ex": 500.0, "pharma_ex": 1000.0, "dialysis_ex": 0.0,
            "total_ex": 1500.0, "maa_payment": 24300.0,
            "doctor_share": 9120.0, "hospital_share": 13680.0,
            "doctor_pct": 0.4, "doctor_flat": None,
            "maa_status": "Claim Paid", "doctor_paid": 1,
            "doctor_payment_month": "2025-06", "comments": None,
        },
        {
            "id": 2, "tid": None, "patient_name": "Cash Patient",
            "admission_date": "2025-06-20", "month": "2025-06",
            "hosp_ex": 0.0, "pharma_ex": 0.0, "dialysis_ex": 0.0,
            "total_ex": 0.0, "maa_payment": None,
            "doctor_share": 3000.0, "hospital_share": None,
            "doctor_pct": 0.4, "doctor_flat": 3000.0,
            "maa_status": None, "doctor_paid": 0,
            "doctor_payment_month": None, "comments": "Non-MAA",
        },
    ])


def test_generate_doctor_internal_returns_bytes(sample_entries):
    result = reports.generate_doctor_internal(sample_entries, "June 2025")
    assert isinstance(result, bytes)
    assert len(result) > 0


def test_generate_doctor_internal_sheet_name(sample_entries):
    wb = load_workbook(io.BytesIO(reports.generate_doctor_internal(sample_entries, "June 2025")))
    assert "June 2025 (Internal)" in wb.sheetnames


def test_generate_doctor_internal_column_headers(sample_entries):
    wb = load_workbook(io.BytesIO(reports.generate_doctor_internal(sample_entries, "June 2025")))
    ws = wb.active
    headers = [ws.cell(1, c).value for c in range(1, ws.max_column + 1)]
    for expected in ["No.", "Patient Name", "Doctor Share", "Doctor Paid", "Dr Payment Month"]:
        assert expected in headers


def test_generate_doctor_copy_omits_payment_tracking(sample_entries):
    wb = load_workbook(io.BytesIO(reports.generate_doctor_copy(sample_entries, "June 2025")))
    ws = wb.active
    headers = [ws.cell(1, c).value for c in range(1, ws.max_column + 1)]
    assert "Doctor Paid" not in headers
    assert "Dr Payment Month" not in headers
    assert "MAA Status" not in headers


def test_generate_doctor_copy_sheet_name(sample_entries):
    wb = load_workbook(io.BytesIO(reports.generate_doctor_copy(sample_entries, "June 2025")))
    assert "June 2025 (Dr Copy)" in wb.sheetnames


def test_report_row_count(sample_entries):
    wb = load_workbook(io.BytesIO(reports.generate_doctor_internal(sample_entries, "June 2025")))
    ws = wb.active
    # header row + 2 data rows + 1 total row = 4
    assert ws.max_row == 4
```

- [ ] **Step 2: Run to confirm they fail**

```bash
pytest tests/test_doctor_share_reports.py -v
```
Expected: FAIL — `reports.generate_doctor_internal` not defined.

- [ ] **Step 3: Extend AMOUNT\_COLS and SUMMABLE\_COLS in reports.py**

Find the two set definitions (lines 69–73) and replace them:
```python
AMOUNT_COLS = {
    "total_approved", "total_paid", "total_received", "outstanding",
    "approved_amount", "paid_amount", "pkg_rate",
    "hosp_ex", "pharma_ex", "dialysis_ex", "total_ex",
    "maa_payment", "doctor_share", "hospital_share",
}
SUMMABLE_COLS = {
    "total_approved", "total_paid", "total_received", "outstanding",
    "approved_amount", "paid_amount", "packages",
    "admissions", "queries", "query_raised", "days",
    "hosp_ex", "pharma_ex", "dialysis_ex", "total_ex",
    "maa_payment", "doctor_share", "hospital_share",
}
```

- [ ] **Step 4: Add DOCTOR\_INTERNAL\_COLS and DOCTOR\_COPY\_COLS to reports.py**

Add after the `FY_COLS` block (after line 67):
```python
DOCTOR_INTERNAL_COLS = [
    ("No.",               "no",                   NUMBER_FMT),
    ("Patient Name",      "patient_name",         None),
    ("Admission Date",    "admission_date",        DATE_FMT),
    ("Hosp Ex",           "hosp_ex",              RUPEE_FMT),
    ("Pharma Ex",         "pharma_ex",            RUPEE_FMT),
    ("Dialysis Ex",       "dialysis_ex",          RUPEE_FMT),
    ("Total Ex",          "total_ex",             RUPEE_FMT),
    ("MAA Payment",       "maa_payment",          RUPEE_FMT),
    ("Doctor Share",      "doctor_share",         RUPEE_FMT),
    ("Hospital Share",    "hospital_share",       RUPEE_FMT),
    ("MAA Status",        "maa_status",           None),
    ("Doctor Paid",       "doctor_paid_label",    None),
    ("Dr Payment Month",  "doctor_payment_month", None),
    ("Comments",          "comments",             None),
]

DOCTOR_COPY_COLS = [
    ("No.",               "no",                   NUMBER_FMT),
    ("Patient Name",      "patient_name",         None),
    ("Admission Date",    "admission_date",        DATE_FMT),
    ("Hosp Ex",           "hosp_ex",              RUPEE_FMT),
    ("Pharma Ex",         "pharma_ex",            RUPEE_FMT),
    ("Dialysis Ex",       "dialysis_ex",          RUPEE_FMT),
    ("Total Ex",          "total_ex",             RUPEE_FMT),
    ("MAA Payment",       "maa_payment",          RUPEE_FMT),
    ("Doctor Share",      "doctor_share",         RUPEE_FMT),
    ("Hospital Share",    "hospital_share",       RUPEE_FMT),
    ("Comments",          "comments",             None),
]
```

- [ ] **Step 5: Add generator functions at the end of reports.py**

```python
def _prepare_doctor_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy().reset_index(drop=True)
    out["no"] = range(1, len(out) + 1)
    out["doctor_paid_label"] = out["doctor_paid"].apply(lambda x: "Yes" if x else "No")
    return out


def generate_doctor_internal(df: pd.DataFrame, month_label: str) -> bytes:
    """Full internal report: all columns including payment tracking."""
    wb = Workbook()
    ws = wb.active
    _write_sheet(ws, _prepare_doctor_df(df), DOCTOR_INTERNAL_COLS, f"{month_label} (Internal)")
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def generate_doctor_copy(df: pd.DataFrame, month_label: str) -> bytes:
    """Doctor-facing report: no payment tracking columns."""
    wb = Workbook()
    ws = wb.active
    _write_sheet(ws, _prepare_doctor_df(df), DOCTOR_COPY_COLS, f"{month_label} (Dr Copy)")
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
```

- [ ] **Step 6: Run all report tests**

```bash
pytest tests/test_doctor_share_reports.py -v
```
Expected: 6 tests PASS.

- [ ] **Step 7: Run full test suite to confirm no regressions**

```bash
pytest tests/ -v
```
Expected: all tests PASS.

- [ ] **Step 8: Commit**

```bash
git add reports.py tests/test_doctor_share_reports.py
git commit -m "feat: add doctor share report generators"
```

---

## Task 6: Doctor Share Page in app.py

**Files:**
- Modify: `app.py`

No automated tests — manual verification checklist at end.

- [ ] **Step 1: Add `import pandas as pd` to app.py**

Find the imports block at the top of `app.py` and add:
```python
import pandas as pd
```
(after `from pathlib import Path`)

- [ ] **Step 2: Add "Doctor Share" to sidebar navigation**

Find the `st.sidebar.radio(...)` block and update:
```python
page = st.sidebar.radio(
    "Navigate",
    ["Dashboard", "Ingest", "Admissions", "Reports", "Doctor Share"],
    index=0,
)
```

- [ ] **Step 3: Add the Doctor Share page section at the end of app.py**

Add after the final `else:  # Raw Export` block:

```python
# ══════════════════════════════════════════════════════════════════════════════
# PAGE: DOCTOR SHARE
# ══════════════════════════════════════════════════════════════════════════════

elif page == "Doctor Share":
    from datetime import datetime as _dt

    st.title("Doctor Share — Dr. Kavesh")

    available_months = db.get_available_months(conn)
    if not available_months:
        st.info("No admission data found. Ingest CSV files first.")
        st.stop()

    col_month, col_filter = st.columns([1, 2])
    with col_month:
        selected_month = st.selectbox(
            "Month", available_months, index=len(available_months) - 1
        )
    with col_filter:
        STATUS_OPTIONS = ["All", "MAA Paid", "MAA Approved", "Query Raised", "Unpaid to Doctor"]
        status_filter = st.selectbox("Show", STATUS_OPTIONS)

    # ── Load + filter data ────────────────────────────────────────────────────
    df = db.get_doctor_expenses(conn, selected_month)

    if not df.empty:
        if status_filter == "MAA Paid":
            df = df[df["maa_status"].fillna("").str.lower().str.contains("paid")]
        elif status_filter == "MAA Approved":
            df = df[df["maa_status"].fillna("").str.lower().str.contains("approved")]
        elif status_filter == "Query Raised":
            df = df[df["maa_status"].fillna("").str.lower().str.contains("query")]
        elif status_filter == "Unpaid to Doctor":
            df = df[df["doctor_paid"] == 0]

    # ── Add Entry expander ────────────────────────────────────────────────────
    with st.expander("➕ Add Entry", expanded=False):
        entry_type = st.radio(
            "Patient type", ["MAA Patient", "Non-MAA Patient"], horizontal=True
        )

        if entry_type == "MAA Patient":
            search_name = st.text_input("Search patient name (from physical bill)")
            candidates = []

            if search_name:
                candidates = db.search_claims_for_matching(
                    conn, search_name, selected_month, expand=False
                )
                if not candidates:
                    if st.checkbox("No results — expand search to ±1 month?"):
                        candidates = db.search_claims_for_matching(
                            conn, search_name, selected_month, expand=True
                        )

            if candidates:
                cand_labels = [
                    f"{c['patient_name']} | TID: {c['tid']} | "
                    f"Adm: {c['date_of_admission']} | "
                    f"MAA Paid: {fmt_inr(c['maa_paid'] or 0)} | {c['status']}"
                    for c in candidates
                ]
                selected_idx = st.radio(
                    "Select matching admission",
                    range(len(cand_labels)),
                    format_func=lambda i: cand_labels[i],
                )
                chosen = candidates[selected_idx]
                st.info(
                    f"Selected: **{chosen['patient_name']}** "
                    f"(TID: {chosen['tid']}, Adm: {chosen['date_of_admission']})"
                )

                c1, c2, c3 = st.columns(3)
                hosp_ex     = c1.number_input("Hospital Ex (₹)",  min_value=0.0, step=100.0, key="ae_hosp")
                pharma_ex   = c2.number_input("Pharmacy Ex (₹)",  min_value=0.0, step=100.0, key="ae_pharma")
                dialysis_ex = c3.number_input("Dialysis Ex (₹)",  min_value=0.0, step=100.0, key="ae_dialysis")

                doctor_pct_input = (
                    st.number_input(
                        "Doctor % (default 40%)", min_value=0.0, max_value=100.0,
                        value=40.0, step=5.0, key="ae_pct"
                    ) / 100.0
                )
                doctor_flat_raw = st.number_input(
                    "Flat override ₹ (0 = use %)", min_value=0.0, step=500.0, key="ae_flat"
                )
                comments_input = st.text_input("Comments", key="ae_comments")

                total_ex_preview = hosp_ex + pharma_ex + dialysis_ex
                maa_preview = chosen["maa_paid"] or 0
                flat_val = doctor_flat_raw if doctor_flat_raw > 0 else None
                share_preview = flat_val if flat_val else doctor_pct_input * (maa_preview - total_ex_preview)
                st.caption(
                    f"Preview → Total Ex: {fmt_inr(total_ex_preview)} | "
                    f"MAA Payment: {fmt_inr(maa_preview)} | "
                    f"Doctor Share: {fmt_inr(share_preview)}"
                )

                if st.button("Save Entry", type="primary", key="ae_save_maa"):
                    db.save_doctor_expense(
                        conn,
                        month=selected_month,
                        patient_name=chosen["patient_name"],
                        admission_date=chosen["date_of_admission"],
                        hosp_ex=hosp_ex, pharma_ex=pharma_ex, dialysis_ex=dialysis_ex,
                        doctor_pct=doctor_pct_input,
                        doctor_flat=flat_val,
                        comments=comments_input or None,
                        maa_status=chosen["status"],
                        tid=chosen["tid"],
                    )
                    st.success(f"Added entry for {chosen['patient_name']}.")
                    st.rerun()
            elif search_name:
                st.warning("No matching admissions found.")

        else:  # Non-MAA Patient
            nm_name     = st.text_input("Patient Name", key="nm_name")
            nm_date     = st.date_input("Admission Date", key="nm_date")
            c1, c2, c3  = st.columns(3)
            nm_hosp     = c1.number_input("Hospital Ex (₹)",  min_value=0.0, step=100.0, key="nm_hosp")
            nm_pharma   = c2.number_input("Pharmacy Ex (₹)",  min_value=0.0, step=100.0, key="nm_pharma")
            nm_dialysis = c3.number_input("Dialysis Ex (₹)",  min_value=0.0, step=100.0, key="nm_dialysis")
            nm_share    = st.number_input("Doctor Share (₹)", min_value=0.0, step=500.0, key="nm_share")
            nm_comments = st.text_input("Comments", key="nm_comments")

            if st.button("Save Entry", type="primary", key="nm_save"):
                if not nm_name:
                    st.error("Patient name is required.")
                elif nm_share <= 0:
                    st.error("Doctor share must be greater than 0.")
                else:
                    db.save_doctor_expense(
                        conn,
                        month=selected_month,
                        patient_name=nm_name,
                        admission_date=str(nm_date),
                        hosp_ex=nm_hosp, pharma_ex=nm_pharma, dialysis_ex=nm_dialysis,
                        doctor_flat=nm_share,
                        comments=nm_comments or None,
                        tid=None,
                    )
                    st.success(f"Added non-MAA entry for {nm_name}.")
                    st.rerun()

    # ── Data table ────────────────────────────────────────────────────────────
    if df.empty:
        st.info("No entries for this month/filter. Use '➕ Add Entry' above.")
    else:
        display_df = df[[
            "id", "patient_name", "admission_date",
            "hosp_ex", "pharma_ex", "dialysis_ex", "total_ex",
            "maa_payment", "doctor_flat", "doctor_share", "hospital_share",
            "maa_status", "doctor_paid", "doctor_payment_month", "comments",
        ]].copy()
        display_df.insert(0, "_select", False)

        edited = st.data_editor(
            display_df,
            column_config={
                "_select":              st.column_config.CheckboxColumn("✓", default=False, width="small"),
                "id":                   st.column_config.NumberColumn("ID", disabled=True),
                "patient_name":         st.column_config.TextColumn("Patient", disabled=True),
                "admission_date":       st.column_config.TextColumn("Adm. Date", disabled=True),
                "hosp_ex":              st.column_config.NumberColumn("Hosp Ex ₹", min_value=0, format="₹%.0f"),
                "pharma_ex":            st.column_config.NumberColumn("Pharma Ex ₹", min_value=0, format="₹%.0f"),
                "dialysis_ex":          st.column_config.NumberColumn("Dialysis Ex ₹", min_value=0, format="₹%.0f"),
                "total_ex":             st.column_config.NumberColumn("Total Ex ₹", disabled=True, format="₹%.0f"),
                "maa_payment":          st.column_config.NumberColumn("MAA Pmt ₹", disabled=True, format="₹%.0f"),
                "doctor_flat":          st.column_config.NumberColumn("Dr Share Override ₹", min_value=0, format="₹%.0f"),
                "doctor_share":         st.column_config.NumberColumn("Doctor Share ₹", disabled=True, format="₹%.0f"),
                "hospital_share":       st.column_config.NumberColumn("Hospital Share ₹", disabled=True, format="₹%.0f"),
                "maa_status":           st.column_config.TextColumn("MAA Status", disabled=True),
                "doctor_paid":          st.column_config.CheckboxColumn("Dr Paid", disabled=True),
                "doctor_payment_month": st.column_config.TextColumn("Paid Month", disabled=True),
                "comments":             st.column_config.TextColumn("Comments"),
            },
            hide_index=True,
            use_container_width=True,
            key="doctor_expense_editor",
        )

        # Persist edits on any change to editable columns
        editable_cols = ["hosp_ex", "pharma_ex", "dialysis_ex", "doctor_flat", "comments"]
        changed_mask = (
            edited[editable_cols].astype(str) != display_df[editable_cols].astype(str)
        ).any(axis=1)
        if changed_mask.any():
            for _, row in edited[changed_mask].iterrows():
                flat_raw = row["doctor_flat"]
                flat_val = float(flat_raw) if pd.notna(flat_raw) and flat_raw > 0 else None
                db.update_doctor_expense(conn, int(row["id"]), {
                    "hosp_ex":     float(row["hosp_ex"]),
                    "pharma_ex":   float(row["pharma_ex"]),
                    "dialysis_ex": float(row["dialysis_ex"]),
                    "doctor_flat": flat_val,
                    "comments":    row["comments"] if pd.notna(row["comments"]) else None,
                })
            st.rerun()

        # Bulk actions for selected rows
        selected_ids = edited[edited["_select"]]["id"].tolist()
        if selected_ids:
            col_paid, col_del, _ = st.columns([2, 1, 2])
            with col_paid:
                pay_month = st.text_input(
                    f"Payment month for {len(selected_ids)} row(s) (YYYY-MM)",
                    value=selected_month,
                    key="pay_month_input",
                )
                if st.button(f"Mark {len(selected_ids)} as paid", type="primary"):
                    db.mark_doctor_paid(conn, [int(i) for i in selected_ids], pay_month)
                    st.success(f"Marked {len(selected_ids)} row(s) as paid ({pay_month}).")
                    st.rerun()
            with col_del:
                st.write("")
                st.write("")
                if st.button(f"🗑 Delete {len(selected_ids)}", type="secondary"):
                    for id_ in selected_ids:
                        db.delete_doctor_expense(conn, int(id_))
                    st.success(f"Deleted {len(selected_ids)} row(s).")
                    st.rerun()

        # Summary metrics
        st.divider()
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total MAA Payment",  fmt_inr(df["maa_payment"].fillna(0).sum()))
        m2.metric("Total Doctor Share", fmt_inr(df["doctor_share"].fillna(0).sum()))
        m3.metric("Total Hosp Share",   fmt_inr(df["hospital_share"].fillna(0).sum()))
        m4.metric("Total Expenses",     fmt_inr(df["total_ex"].sum()))

    # ── Exports (always full month, ignores status filter) ────────────────────
    export_df = db.get_doctor_expenses(conn, selected_month)
    if not export_df.empty:
        st.divider()
        month_label = reports._month_label(selected_month)
        col_int, col_doc = st.columns(2)
        with col_int:
            st.download_button(
                label="Download Internal Export",
                data=reports.generate_doctor_internal(export_df, month_label),
                file_name=f"DoctorShare_Internal_{selected_month}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        with col_doc:
            st.download_button(
                label="Download Doctor Copy",
                data=reports.generate_doctor_copy(export_df, month_label),
                file_name=f"DoctorShare_DrKavesh_{selected_month}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
```

- [ ] **Step 4: Run full test suite**

```bash
pytest tests/ -v
```
Expected: all tests PASS.

- [ ] **Step 5: Start the app and verify manually**

```bash
source .venv/bin/activate && streamlit run app.py
```

Manual checklist:
1. "Doctor Share" appears in sidebar — page loads without errors
2. Month picker shows months from existing MAA data
3. Open "Add Entry" → type a partial patient name → candidate list appears
4. Select a candidate, enter expenses, save → entry appears in table with correct doctor share
5. Edit Hosp Ex inline → table re-renders with updated Total Ex and Doctor Share
6. Enter a flat override in "Dr Share Override ₹" → Doctor Share column shows the override value
7. Add a Non-MAA entry → appears with blank MAA Payment and Hospital Share columns
8. Select rows via checkboxes → "Mark as paid" and "Delete" buttons appear
9. Mark as paid → doctor_paid checkbox turns checked, Paid Month populated
10. Delete → rows disappear
11. Apply each status filter → only matching rows shown
12. Download Internal Export → all 14 columns present, ₹ formatting, totals row
13. Download Doctor Copy → Doctor Paid / Dr Payment Month / MAA Status absent
14. Verify exports always contain all month entries regardless of status filter

- [ ] **Step 6: Commit**

```bash
git add app.py
git commit -m "feat: add Doctor Share page"
```
