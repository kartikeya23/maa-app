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
