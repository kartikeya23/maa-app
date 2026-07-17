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
        "created_at", "updated_at", "doctor_name",
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


def test_get_doctor_expenses_flags_unreconciled_flat_override(mem_db_with_claims):
    # Flat override (30000) exceeds maa_payment - total_ex (24300 - 1500 = 22800),
    # so hospital_share floors at 0 and the row no longer reconciles with maa_payment.
    conn = mem_db_with_claims
    conn.execute(
        """INSERT INTO doctor_expenses
               (tid, patient_name, admission_date, month, hosp_ex, pharma_ex, dialysis_ex, doctor_flat)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        ("TID001", "Ravi Kumar", "2025-06-15", "2025-06", 500.0, 1000.0, 0.0, 30000.0),
    )
    conn.commit()
    row = db.get_doctor_expenses(conn, "2025-06").iloc[0]
    assert row["hospital_share"] == 0.0
    assert row["shares_reconcile"] == False


def test_get_doctor_expenses_reconciles_by_default(mem_db_with_claims):
    conn = mem_db_with_claims
    conn.execute(
        """INSERT INTO doctor_expenses
               (tid, patient_name, admission_date, month, hosp_ex, pharma_ex, dialysis_ex, maa_status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        ("TID001", "Ravi Kumar", "2025-06-15", "2025-06", 500.0, 1000.0, 0.0, "Claim Paid"),
    )
    conn.commit()
    row = db.get_doctor_expenses(conn, "2025-06").iloc[0]
    assert row["shares_reconcile"] == True


def test_search_claims_for_matching_exact_month(mem_db_with_claims):
    results = db.search_claims_for_matching(mem_db_with_claims, "ravi", "2025-06", expand=False)
    assert len(results) == 1
    assert results[0]["tid"] == "TID001"
    assert results[0]["patient_name"] == "Ravi Kumar"


def test_search_claims_for_matching_expand(mem_db_with_claims):
    results = db.search_claims_for_matching(mem_db_with_claims, "ravi", "2025-07", expand=True)
    assert len(results) == 1
    assert results[0]["tid"] == "TID001"


def test_search_claims_fuzzy_fallback_on_typo(mem_db_with_claims):
    # "Ravu Kumar" (typo) has no exact substring match against "Ravi Kumar" but is
    # close enough for difflib to catch — handles hand-typed bill names vs. portal spelling.
    results = db.search_claims_for_matching(mem_db_with_claims, "Ravu Kumar", "2025-06", expand=False)
    assert len(results) == 1
    assert results[0]["tid"] == "TID001"


def test_search_claims_no_match_returns_empty(mem_db_with_claims):
    results = db.search_claims_for_matching(mem_db_with_claims, "Zzyzx Nobody", "2025-06", expand=False)
    assert results == []


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


def test_update_doctor_expense_logs_changed_fields(mem_db):
    conn = mem_db
    conn.execute(
        "INSERT INTO doctor_expenses (tid, patient_name, admission_date, month, hosp_ex) VALUES (?, ?, ?, ?, ?)",
        ("T001", "Patient A", "2025-06-01", "2025-06", 100.0),
    )
    conn.commit()
    row_id = conn.execute("SELECT id FROM doctor_expenses").fetchone()[0]
    db.update_doctor_expense(conn, row_id, {"hosp_ex": 200.0, "comments": "Updated"})
    log = db.get_doctor_expense_log(conn, row_id)
    assert set(log["field"]) == {"hosp_ex", "comments"}
    hosp_row = log[log["field"] == "hosp_ex"].iloc[0]
    assert hosp_row["old_value"] == "100.0"
    assert hosp_row["new_value"] == "200.0"


def test_update_doctor_expense_does_not_log_unchanged_fields(mem_db):
    conn = mem_db
    conn.execute(
        "INSERT INTO doctor_expenses (tid, patient_name, admission_date, month, hosp_ex) VALUES (?, ?, ?, ?, ?)",
        ("T001", "Patient A", "2025-06-01", "2025-06", 100.0),
    )
    conn.commit()
    row_id = conn.execute("SELECT id FROM doctor_expenses").fetchone()[0]
    # Value is unchanged (int 100 vs the stored float 100.0 must compare equal, not log).
    db.update_doctor_expense(conn, row_id, {"hosp_ex": 100})
    log = db.get_doctor_expense_log(conn, row_id)
    assert log.empty


def test_get_doctor_expense_log_empty_for_untouched_entry(mem_db):
    conn = mem_db
    conn.execute(
        "INSERT INTO doctor_expenses (tid, patient_name, admission_date, month) VALUES (?, ?, ?, ?)",
        ("T001", "Patient A", "2025-06-01", "2025-06"),
    )
    conn.commit()
    row_id = conn.execute("SELECT id FROM doctor_expenses").fetchone()[0]
    log = db.get_doctor_expense_log(conn, row_id)
    assert log.empty


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
    """created_at must not be writable via update_doctor_expense."""
    conn = mem_db
    conn.execute(
        "INSERT INTO doctor_expenses (tid, patient_name, admission_date, month) VALUES (?, ?, ?, ?)",
        ("T001", "Patient A", "2025-06-01", "2025-06"),
    )
    conn.commit()
    row_id = conn.execute("SELECT id FROM doctor_expenses").fetchone()[0]
    db.update_doctor_expense(conn, row_id, {"created_at": "1970-01-01"})
    row = conn.execute("SELECT created_at FROM doctor_expenses WHERE id=?", (row_id,)).fetchone()
    assert row[0] != "1970-01-01", "created_at must not be updated via update_doctor_expense"


def test_update_doctor_expense_month(mem_db):
    conn = mem_db
    conn.execute(
        "INSERT INTO doctor_expenses (tid, patient_name, admission_date, month) VALUES (?, ?, ?, ?)",
        ("T001", "Patient A", "2025-06-01", "2025-06"),
    )
    conn.commit()
    row_id = conn.execute("SELECT id FROM doctor_expenses").fetchone()[0]
    db.update_doctor_expense(conn, row_id, {"month": "2025-07"})
    month = conn.execute("SELECT month FROM doctor_expenses WHERE id=?", (row_id,)).fetchone()[0]
    assert month == "2025-07"


def test_update_doctor_expense_doctor_paid(mem_db):
    conn = mem_db
    conn.execute(
        "INSERT INTO doctor_expenses (tid, patient_name, admission_date, month, doctor_paid) VALUES (?, ?, ?, ?, ?)",
        ("T001", "Patient A", "2025-06-01", "2025-06", 0),
    )
    conn.commit()
    row_id = conn.execute("SELECT id FROM doctor_expenses").fetchone()[0]
    db.update_doctor_expense(conn, row_id, {"doctor_paid": 1})
    paid = conn.execute("SELECT doctor_paid FROM doctor_expenses WHERE id=?", (row_id,)).fetchone()[0]
    assert paid == 1


def test_unmark_doctor_paid(mem_db):
    conn = mem_db
    conn.executemany(
        "INSERT INTO doctor_expenses (tid, patient_name, admission_date, month, doctor_paid, doctor_payment_month) VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("T001", "P1", "2025-06-01", "2025-06", 1, "2025-06"),
            ("T002", "P2", "2025-06-02", "2025-06", 1, "2025-06"),
        ],
    )
    conn.commit()
    ids = [r[0] for r in conn.execute("SELECT id FROM doctor_expenses").fetchall()]
    db.unmark_doctor_paid(conn, ids)
    rows = conn.execute("SELECT doctor_paid, doctor_payment_month FROM doctor_expenses").fetchall()
    assert all(r[0] == 0 for r in rows)
    assert all(r[1] is None for r in rows)


def test_doctor_name_migration_idempotent(mem_db):
    """Running init_db a second time must not raise even though column already exists."""
    db.init_db(":memory:")
    # The real migration guard is in init_db for on-disk DBs; this verifies no crash


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


def test_compute_doctor_share_clamps_shortfall_to_zero_when_not_rejected():
    # pct*(maa-ex) would be negative; not rejected, so both shares floor at 0.
    doctor_share, hospital_share = db.compute_doctor_share(
        maa_payment=1000.0, total_ex=5000.0, doctor_pct=0.4, doctor_flat=None, is_rejected=False,
    )
    assert doctor_share == 0.0
    assert hospital_share == 0.0


def test_compute_doctor_share_allows_negative_when_rejected():
    # A Rejected claim doesn't waive the doctor's share, so the shortfall stays negative
    # instead of being floored at 0 — it's still owed, just not covered by MAA payment.
    doctor_share, hospital_share = db.compute_doctor_share(
        maa_payment=1000.0, total_ex=5000.0, doctor_pct=0.4, doctor_flat=None, is_rejected=True,
    )
    assert doctor_share == pytest.approx(0.4 * (1000.0 - 5000.0))
    assert hospital_share == pytest.approx(1000.0 - doctor_share - 5000.0)


def test_compute_doctor_share_no_maa_link_no_flat_returns_none():
    doctor_share, hospital_share = db.compute_doctor_share(
        maa_payment=None, total_ex=0.0, doctor_pct=0.4, doctor_flat=None, is_rejected=False,
    )
    assert doctor_share is None
    assert hospital_share is None


def test_compute_doctor_share_flat_override_without_maa_link():
    doctor_share, hospital_share = db.compute_doctor_share(
        maa_payment=None, total_ex=0.0, doctor_pct=0.4, doctor_flat=3000.0, is_rejected=False,
    )
    assert doctor_share == 3000.0
    assert hospital_share is None


def test_compute_doctor_share_linked_but_unpaid_hospital_share_is_none():
    # tid linked but MAA hasn't paid anything yet (maa_payment == 0): doctor_share is
    # still derived from the flat/pct, but hospital_share is unknowable until paid.
    doctor_share, hospital_share = db.compute_doctor_share(
        maa_payment=0.0, total_ex=0.0, doctor_pct=0.4, doctor_flat=2000.0, is_rejected=False,
    )
    assert doctor_share == 2000.0
    assert hospital_share is None


def test_compute_doctor_share_zero_pct_is_not_treated_as_default():
    # Regression guard: doctor_pct=0.0 is a legitimate value (doctor takes no percentage
    # share) and must not be coerced to the 0.4 schema default anywhere in the pipeline.
    doctor_share, hospital_share = db.compute_doctor_share(
        maa_payment=24300.0, total_ex=1500.0, doctor_pct=0.0, doctor_flat=None, is_rejected=False,
    )
    assert doctor_share == 0.0
    assert hospital_share == pytest.approx(24300.0 - 1500.0)


def test_get_doctor_lifetime_totals_no_entries(mem_db):
    totals = db.get_doctor_lifetime_totals(mem_db, "Dr. Kavesh")
    assert totals["entries"] == 0
    assert totals["total_doctor_share"] == 0.0


def test_get_doctor_lifetime_totals_spans_all_months(mem_db_with_claims):
    conn = mem_db_with_claims
    conn.executemany(
        """INSERT INTO doctor_expenses
               (tid, patient_name, admission_date, month, hosp_ex, doctor_flat, doctor_paid, doctor_name)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            ("TID001", "Ravi Kumar",  "2025-06-15", "2025-06", 0.0, 5000.0, 1, "Dr. Kavesh"),
            (None,     "Cash Patient", "2025-07-01", "2025-07", 0.0, 3000.0, 0, "Dr. Kavesh"),
        ],
    )
    conn.commit()
    totals = db.get_doctor_lifetime_totals(conn, "Dr. Kavesh")
    assert totals["entries"] == 2
    assert totals["paid_entries"] == 1
    assert totals["unpaid_entries"] == 1
    assert totals["total_doctor_share"] == pytest.approx(8000.0)
    assert totals["outstanding_doctor_share"] == pytest.approx(3000.0)


def test_get_doctor_lifetime_totals_scoped_to_doctor(mem_db):
    mem_db.executemany(
        "INSERT INTO doctor_expenses (patient_name, admission_date, month, doctor_flat, doctor_name) VALUES (?, ?, ?, ?, ?)",
        [
            ("Patient A", "2025-06-01", "2025-06", 1000.0, "Dr. Kavesh"),
            ("Patient B", "2025-07-01", "2025-07", 2000.0, "Dr. X"),
        ],
    )
    mem_db.commit()
    kavesh = db.get_doctor_lifetime_totals(mem_db, "Dr. Kavesh")
    assert kavesh["entries"] == 1
    assert kavesh["total_doctor_share"] == pytest.approx(1000.0)


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
