"""
Database layer for MAA Payment Record Management System.
Schema, upsert, and query functions backed by SQLite.
"""

import hashlib
import json
import sqlite3
from datetime import datetime
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
    conn = sqlite3.connect(str(path))
    conn.executescript(DDL)
    conn.executescript(HASH_DDL)
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


