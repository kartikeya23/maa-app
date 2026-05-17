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
