"""
Database layer for MAA Payment Record Management System.
Schema, upsert, and query functions backed by SQLite.
"""

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

DB_PATH = Path(__file__).parent / "maa.db"

# ── Schema ────────────────────────────────────────────────────────────────────

HASH_DDL = """
CREATE TABLE IF NOT EXISTS claims_hash (
    tid          TEXT,
    pkg_code     TEXT,
    claim_number TEXT,
    md5_hash     TEXT,
    PRIMARY KEY (tid, pkg_code, claim_number)
);
"""

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

DDL = """
CREATE TABLE IF NOT EXISTS claims (
    tid                     TEXT,
    patient_name            TEXT,
    hospital_name           TEXT,
    hospital_code           TEXT,
    hospital_type           TEXT,
    date_of_admission       TEXT,
    time_of_admission       TEXT,
    date_of_discharge       TEXT,
    time_of_discharge       TEXT,
    modified_date           TEXT,
    pkg_code                TEXT,
    pkg_name                TEXT,
    pkg_rate                REAL,
    id_type                 TEXT,
    id_number               TEXT,
    district_name           TEXT,
    aadhaar_number          TEXT,
    aadhaar_name            TEXT,
    policy_year             TEXT,
    mobile_no               TEXT,
    status                  TEXT,
    payment_type            TEXT,
    query_raised            INTEGER,
    claim_number            TEXT,
    approved_amount         REAL,
    paid_amount             REAL,
    gender                  TEXT,
    age                     INTEGER,
    payment_date            TEXT,
    bank_utr_number         TEXT,
    tpa_name                TEXT,
    claim_processor_name    TEXT,
    claim_processor_ssoid   TEXT,
    pkg_speciality_name     TEXT,
    package_remark          TEXT,
    claim_submission_dt     TEXT,
    last_ingested_at        TEXT,
    PRIMARY KEY (tid, pkg_code, claim_number)
);

CREATE INDEX IF NOT EXISTS idx_date_of_admission ON claims (date_of_admission);
CREATE INDEX IF NOT EXISTS idx_status            ON claims (status);
CREATE INDEX IF NOT EXISTS idx_policy_year       ON claims (policy_year);
"""

# Mutable fields used for change detection (excludes PK and last_ingested_at)
MUTABLE_FIELDS = [
    "patient_name", "hospital_name", "hospital_code", "hospital_type",
    "date_of_admission", "time_of_admission", "date_of_discharge", "time_of_discharge",
    "modified_date", "pkg_name", "pkg_rate", "id_type", "id_number",
    "district_name", "aadhaar_number", "aadhaar_name", "policy_year", "mobile_no",
    "status", "payment_type", "query_raised", "approved_amount", "paid_amount",
    "gender", "age", "payment_date", "bank_utr_number", "tpa_name",
    "claim_processor_name", "claim_processor_ssoid", "pkg_speciality_name",
    "package_remark", "claim_submission_dt",
]

ALL_COLUMNS = [
    "tid", "patient_name", "hospital_name", "hospital_code", "hospital_type",
    "date_of_admission", "time_of_admission", "date_of_discharge", "time_of_discharge",
    "modified_date", "pkg_code", "pkg_name", "pkg_rate", "id_type", "id_number",
    "district_name", "aadhaar_number", "aadhaar_name", "policy_year", "mobile_no",
    "status", "payment_type", "query_raised", "claim_number", "approved_amount",
    "paid_amount", "gender", "age", "payment_date", "bank_utr_number", "tpa_name",
    "claim_processor_name", "claim_processor_ssoid", "pkg_speciality_name",
    "package_remark", "claim_submission_dt", "last_ingested_at",
]


# ── Status SQL fragments ──────────────────────────────────────────────────────
# Approved: approved OR paid, excluding pre-auth
_APPROVED_CASE = (
    "CASE WHEN (LOWER(status) LIKE '%approved%' OR LOWER(status) LIKE '%paid%')"
    "      AND LOWER(status) NOT LIKE '%pre%'"
    " THEN approved_amount ELSE 0 END"
)
# Paid: any paid status
_PAID_CASE = "CASE WHEN LOWER(status) LIKE '%paid%' THEN approved_amount ELSE 0 END"
# Rejected: any rejected status (uses pkg_rate since no approved_amount for rejections)
_REJECTED_CASE = "CASE WHEN LOWER(status) LIKE '%rejected%' THEN pkg_rate ELSE 0 END"
# Received: paid amount after 10% TDS deduction
_RECEIVED_CASE = f"({_PAID_CASE}) * 0.9"


def _row_hash(row: dict) -> str:
    payload = {k: row.get(k) for k in MUTABLE_FIELDS}
    return hashlib.md5(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


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


# ── Upsert ────────────────────────────────────────────────────────────────────

def upsert_claims(conn: sqlite3.Connection, rows: list[dict], dry_run: bool = False) -> tuple[int, int, int]:
    """
    Insert or update claim rows. Returns (new, updated, unchanged).

    Each row dict must have keys matching ALL_COLUMNS (except last_ingested_at).
    When dry_run=True, performs all classification logic but skips writes.
    """
    new = updated = unchanged = 0
    now = datetime.now().isoformat(timespec="seconds")

    # Fetch existing hashes for the PKs we're about to touch
    pks = [(r["tid"], r["pkg_code"], r["claim_number"]) for r in rows]
    if pks:
        placeholders = ",".join("(?,?,?)" for _ in pks)
        flat = [v for pk in pks for v in pk]
        existing = {
            (tid, pkg_code, claim_number): h
            for tid, pkg_code, claim_number, h in conn.execute(
                f"""
                SELECT tid, pkg_code, claim_number,
                       md5_hash
                FROM   claims_hash
                WHERE  (tid, pkg_code, claim_number) IN ({placeholders})
                """,
                flat,
            )
        }
    else:
        existing = {}

    upsert_sql = f"""
        INSERT OR REPLACE INTO claims ({', '.join(ALL_COLUMNS)})
        VALUES ({', '.join('?' for _ in ALL_COLUMNS)})
    """
    hash_upsert_sql = """
        INSERT OR REPLACE INTO claims_hash (tid, pkg_code, claim_number, md5_hash)
        VALUES (?, ?, ?, ?)
    """

    for row in rows:
        h = _row_hash(row)
        pk = (row["tid"], row["pkg_code"], row["claim_number"])

        if pk not in existing:
            new += 1
        elif existing[pk] != h:
            updated += 1
        else:
            unchanged += 1
            continue  # nothing to write

        if not dry_run:
            row["last_ingested_at"] = now
            values = [row.get(col) for col in ALL_COLUMNS]
            conn.execute(upsert_sql, values)
            conn.execute(hash_upsert_sql, (*pk, h))

    if not dry_run:
        conn.commit()
    return new, updated, unchanged


# ── Queries ───────────────────────────────────────────────────────────────────

def query_admissions(conn: sqlite3.Connection, filters: dict | None = None) -> pd.DataFrame:
    """
    Returns one row per TID with aggregated package/amount info.
    filters keys: date_from, date_to, policy_year, status, pkg_speciality_name
    """
    where_clauses = []
    params = []

    if filters:
        if filters.get("date_from"):
            where_clauses.append("date_of_admission >= ?")
            params.append(filters["date_from"])
        if filters.get("date_to"):
            where_clauses.append("date_of_admission <= ?")
            params.append(filters["date_to"])
        if filters.get("policy_year"):
            where_clauses.append("policy_year = ?")
            params.append(filters["policy_year"])
        if filters.get("status"):
            where_clauses.append("status = ?")
            params.append(filters["status"])
        if filters.get("pkg_speciality_name"):
            where_clauses.append("pkg_speciality_name = ?")
            params.append(filters["pkg_speciality_name"])

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    sql = f"""
        SELECT
            tid,
            patient_name,
            gender,
            age,
            date_of_admission,
            date_of_discharge,
            CAST(
                julianday(COALESCE(date_of_discharge, date('now'))) -
                julianday(date_of_admission)
                AS INTEGER
            ) + 1                        AS days,
            COUNT(*)                     AS packages,
            SUM({_APPROVED_CASE})        AS total_approved,
            SUM({_PAID_CASE})            AS total_paid,
            SUM({_RECEIVED_CASE})        AS total_received,
            SUM({_APPROVED_CASE}) - SUM({_PAID_CASE}) AS outstanding,
            SUM(query_raised)            AS queries,
            GROUP_CONCAT(DISTINCT status) AS statuses
        FROM claims
        {where_sql}
        GROUP BY tid
        ORDER BY date_of_admission DESC
    """
    return pd.read_sql_query(sql, conn, params=params)


def fy_of(date_str: str) -> str:
    """'2025-06-15' → '2025-2026', '2025-02-01' → '2024-2025'"""
    try:
        d = datetime.strptime(date_str[:10], "%Y-%m-%d")
        if d.month >= 4:
            return f"{d.year}-{d.year + 1}"
        else:
            return f"{d.year - 1}-{d.year}"
    except Exception:
        return "Unknown"


def query_monthly_summary(conn: sqlite3.Connection) -> pd.DataFrame:
    sql = f"""
        SELECT
            strftime('%Y-%m', date_of_admission)  AS month,
            COUNT(DISTINCT tid)                   AS admissions,
            COUNT(*)                              AS packages,
            SUM({_APPROVED_CASE})                 AS total_approved,
            SUM({_PAID_CASE})                     AS total_paid,
            SUM({_RECEIVED_CASE})                 AS total_received,
            SUM({_APPROVED_CASE}) - SUM({_PAID_CASE}) AS outstanding
        FROM claims
        WHERE date_of_admission IS NOT NULL
        GROUP BY month
        ORDER BY month
    """
    return pd.read_sql_query(sql, conn)


def query_fy_summary(conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql_query(
        """
        SELECT date_of_admission, tid, status, approved_amount
        FROM claims
        WHERE date_of_admission IS NOT NULL
        """,
        conn,
    )
    if df.empty:
        return pd.DataFrame(columns=["financial_year", "admissions", "packages",
                                     "total_approved", "total_paid", "total_received", "outstanding"])
    df["financial_year"] = df["date_of_admission"].apply(fy_of)
    s = df["status"].str.lower().fillna("")
    df["_approved_amt"] = df["approved_amount"].where(
        (s.str.contains("approved") | s.str.contains("paid")) & ~s.str.contains("pre"), 0
    )
    df["_paid_amt"] = df["approved_amount"].where(s.str.contains("paid"), 0)
    summary = (
        df.groupby("financial_year")
        .agg(
            admissions=("tid", "nunique"),
            packages=("tid", "count"),
            total_approved=("_approved_amt", "sum"),
            total_paid=("_paid_amt", "sum"),
        )
        .reset_index()
    )
    summary["total_received"] = summary["total_paid"] * 0.9
    summary["outstanding"] = summary["total_approved"] - summary["total_paid"]
    return summary.sort_values("financial_year")


def get_available_months(conn: sqlite3.Connection) -> list[str]:
    """Returns sorted list of unique YYYY-MM strings present in claims data."""
    rows = conn.execute(
        """SELECT DISTINCT strftime('%Y-%m', date_of_admission)
           FROM claims
           WHERE date_of_admission IS NOT NULL
           ORDER BY 1"""
    ).fetchall()
    return [r[0] for r in rows if r[0]]


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


def get_available_fys(conn: sqlite3.Connection) -> list[str]:
    """Returns sorted list of unique FY strings present in claims data."""
    rows = conn.execute(
        "SELECT DISTINCT date_of_admission FROM claims WHERE date_of_admission IS NOT NULL"
    ).fetchall()
    fys = sorted({fy_of(r[0]) for r in rows if fy_of(r[0]) != "Unknown"})
    return fys


def query_month_admission_detail(conn: sqlite3.Connection, months: list[str]) -> pd.DataFrame:
    """
    Returns one row per TID for the given month(s) (YYYY-MM), with paid/approved/rejected amounts.
    Uses the same status-based amount logic as query_fy_admission_detail.
    """
    if not months:
        return pd.DataFrame(columns=["month", "tid", "patient_name",
                                     "date_of_admission", "date_of_discharge",
                                     "paid", "received", "approved", "rejected"])
    placeholders = ",".join("?" for _ in months)
    sql = f"""
        SELECT
            strftime('%Y-%m', date_of_admission)  AS month,
            tid,
            patient_name,
            date_of_admission,
            date_of_discharge,
            SUM({_APPROVED_CASE})                 AS approved,
            SUM({_PAID_CASE})                     AS paid,
            SUM({_RECEIVED_CASE})                 AS received,
            SUM({_REJECTED_CASE})                 AS rejected
        FROM claims
        WHERE strftime('%Y-%m', date_of_admission) IN ({placeholders})
        GROUP BY tid
        ORDER BY month ASC, date_of_admission ASC
    """
    return pd.read_sql_query(sql, conn, params=months)


def query_fy_admission_detail(conn: sqlite3.Connection, fy: str) -> pd.DataFrame:
    """
    Returns one row per TID for the given FY, with paid/received/approved/rejected amounts.
    Amount rules:
      paid     = SUM(approved_amount) WHERE status contains 'paid'
      received = paid * 0.9 (after 10% TDS)
      approved = SUM(approved_amount) WHERE (approved OR paid) AND NOT pre-auth, minus paid
      rejected = SUM(pkg_rate)        WHERE status contains 'rejected'
    """
    start_year = int(fy.split("-")[0])
    date_from = f"{start_year}-04-01"
    date_to   = f"{start_year + 1}-03-31"

    sql = f"""
        SELECT
            strftime('%Y-%m', date_of_admission)  AS month,
            tid,
            patient_name,
            date_of_admission,
            date_of_discharge,
            SUM({_APPROVED_CASE})                 AS approved,
            SUM({_PAID_CASE})                     AS paid,
            SUM({_RECEIVED_CASE})                 AS received,
            SUM({_REJECTED_CASE})                 AS rejected
        FROM claims
        WHERE date_of_admission >= ? AND date_of_admission <= ?
        GROUP BY tid
        ORDER BY month ASC, date_of_admission ASC
    """
    return pd.read_sql_query(sql, conn, params=[date_from, date_to])


def query_total_stats(conn: sqlite3.Connection) -> dict:
    row = conn.execute(f"""
        SELECT
            COUNT(DISTINCT tid)                       AS admissions,
            SUM({_APPROVED_CASE})                     AS total_approved,
            SUM({_PAID_CASE})                         AS total_paid,
            SUM({_RECEIVED_CASE})                     AS total_received,
            SUM({_APPROVED_CASE}) - SUM({_PAID_CASE}) AS outstanding
        FROM claims
    """).fetchone()
    return {
        "admissions": row[0] or 0,
        "total_approved": row[1] or 0.0,
        "total_paid": row[2] or 0.0,
        "total_received": row[3] or 0.0,
        "outstanding": row[4] or 0.0,
    }


def query_status_breakdown(conn: sqlite3.Connection) -> pd.DataFrame:
    sql = """
        SELECT
            CASE
                WHEN LOWER(status) LIKE '%paid%'                                      THEN 'Paid'
                WHEN LOWER(status) LIKE '%approved%' AND LOWER(status) NOT LIKE '%pre%' THEN 'Approved'
                WHEN LOWER(status) LIKE '%rejected%'                                  THEN 'Rejected'
                ELSE 'Other'
            END AS status,
            COUNT(DISTINCT tid) AS count
        FROM claims
        GROUP BY 1
        ORDER BY count DESC
    """
    return pd.read_sql_query(sql, conn)


def query_recent_admissions(conn: sqlite3.Connection, n: int = 10) -> pd.DataFrame:
    sql = f"""
        SELECT tid, patient_name, gender, age, date_of_admission, date_of_discharge,
               COUNT(*) AS packages,
               SUM({_APPROVED_CASE}) AS total_approved,
               SUM({_PAID_CASE})     AS total_paid,
               GROUP_CONCAT(DISTINCT status) AS statuses
        FROM claims
        WHERE date_of_admission IS NOT NULL
        GROUP BY tid
        ORDER BY date_of_admission DESC
        LIMIT {n}
    """
    return pd.read_sql_query(sql, conn)


def query_packages_for_tid(conn: sqlite3.Connection, tid: str) -> pd.DataFrame:
    sql = """
        SELECT pkg_code, pkg_name, pkg_speciality_name, pkg_rate,
               approved_amount, paid_amount, status, query_raised,
               claim_number, payment_date, bank_utr_number, tpa_name
        FROM claims
        WHERE tid = ?
        ORDER BY pkg_code
    """
    return pd.read_sql_query(sql, conn, params=[tid])


def query_all_claims(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query("SELECT * FROM claims ORDER BY date_of_admission DESC", conn)


def get_filter_options(conn: sqlite3.Connection) -> dict:
    def distinct(col):
        rows = conn.execute(
            f"SELECT DISTINCT {col} FROM claims WHERE {col} IS NOT NULL AND {col} != '' ORDER BY {col}"
        ).fetchall()
        return [r[0] for r in rows]

    return {
        "status": distinct("status"),
        "policy_year": distinct("policy_year"),
        "pkg_speciality_name": distinct("pkg_speciality_name"),
    }


def get_total_record_count(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM claims").fetchone()[0]


# ── Doctor Share queries ──────────────────────────────────────────────────────

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
        dt = datetime.strptime(month + "-01", "%Y-%m-%d")
        prev = (dt.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
        nxt = (dt.replace(day=28) + timedelta(days=4)).strftime("%Y-%m")
        months = [prev, month, nxt]

    placeholders = ",".join("?" for _ in months)
    sql = f"""
        SELECT
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


def infer_maa_status(conn: sqlite3.Connection, tid: str) -> str | None:
    """Infer a single maa_status from all claims packages for a TID.

    Considers only packages with approved_amount > 0.
    Priority:
      paid + pending  → Claim Approved  (partial payment, more outstanding)
      pending only    → Claim Raised
      paid (any mix)  → Claim Paid
      approved (any)  → Claim Approved
      all rejected    → Rejected
    Returns None if no non-zero claims exist.
    """
    rows = conn.execute(
        "SELECT status, approved_amount FROM claims WHERE tid = ?", (tid,)
    ).fetchall()
    non_zero = [(s, a) for s, a in rows if (a or 0) > 0]
    if not non_zero:
        return None

    def _cat(s: str) -> str:
        sl = s.lower()
        if "paid" in sl:
            return "paid"
        if "rejected" in sl:
            return "rejected"
        if "pre" in sl:
            return "pending"
        if "approved" in sl:
            return "approved"
        return "pending"

    cats = {_cat(s) for s, _ in non_zero}
    if "paid" in cats and "pending" in cats:
        return "Claim Approved"
    if "pending" in cats:
        return "Claim Raised"
    if "paid" in cats:
        return "Claim Paid"
    if "approved" in cats:
        return "Claim Approved"
    return "Rejected"


# ── Doctor Share write operations ─────────────────────────────────────────────

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


def update_doctor_expense(conn: sqlite3.Connection, row_id: int, fields: dict) -> None:
    """Update fields on a doctor_expenses row.
    Allowed keys: hosp_ex, pharma_ex, dialysis_ex, doctor_pct, doctor_flat, comments,
                  doctor_payment_month, maa_status, tid, patient_name, admission_date,
                  month, doctor_paid, doctor_name.
    """
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


def delete_doctor_expense(conn: sqlite3.Connection, row_id: int) -> None:
    conn.execute("DELETE FROM doctor_expenses WHERE id = ?", (row_id,))
    conn.commit()


def mark_doctor_paid(conn: sqlite3.Connection, ids: list[int], payment_month: str) -> None:
    """Bulk-mark doctor_expenses rows as paid to doctor."""
    if not ids:
        return
    placeholders = ",".join("?" for _ in ids)
    conn.execute(
        f"UPDATE doctor_expenses SET doctor_paid = 1, doctor_payment_month = ?, updated_at = datetime('now') WHERE id IN ({placeholders})",
        [payment_month] + ids,
    )
    conn.commit()


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

