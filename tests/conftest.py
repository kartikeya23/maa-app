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
