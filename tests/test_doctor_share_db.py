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


def test_get_doctor_expenses_non_maa_no_flat(mem_db):
    mem_db.execute(
        """INSERT INTO doctor_expenses (tid, patient_name, admission_date, month)
           VALUES (NULL, ?, ?, ?)""",
        ("Pending Patient", "2025-06-20", "2025-06"),
    )
    mem_db.commit()
    df = db.get_doctor_expenses(mem_db, "2025-06")
    row = df.iloc[0]
    assert pd.isna(row["maa_payment"])
    assert pd.isna(row["doctor_share"])
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


def test_update_doctor_expense_disallowed_key(mem_db):
    conn = mem_db
    conn.execute(
        "INSERT INTO doctor_expenses (tid, patient_name, admission_date, month, maa_status) VALUES (?, ?, ?, ?, ?)",
        ("T001", "Patient A", "2025-06-01", "2025-06", "Claim Paid"),
    )
    conn.commit()
    row_id = conn.execute("SELECT id FROM doctor_expenses").fetchone()[0]
    db.update_doctor_expense(conn, row_id, {"maa_status": "Hacked"})
    row = conn.execute("SELECT maa_status FROM doctor_expenses WHERE id=?", (row_id,)).fetchone()
    assert row[0] == "Claim Paid"
