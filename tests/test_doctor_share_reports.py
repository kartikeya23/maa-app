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
    # + 1 blank spacer + 1 outstanding summary header + 1 "Total Unpaid" subtotal row = 7
    assert ws.max_row == 7


def test_generate_doctor_copy_header_is_teal(sample_entries):
    wb = load_workbook(io.BytesIO(reports.generate_doctor_copy(sample_entries, "June 2025")))
    ws = wb.active
    # openpyxl returns ARGB; last 6 chars are the hex colour
    header_fill = ws.cell(1, 1).fill.fgColor.rgb[-6:]
    assert header_fill == "00695C", f"Expected teal 00695C, got {header_fill}"


def test_generate_doctor_internal_header_is_navy(sample_entries):
    wb = load_workbook(io.BytesIO(reports.generate_doctor_internal(sample_entries, "June 2025")))
    ws = wb.active
    header_fill = ws.cell(1, 1).fill.fgColor.rgb[-6:]
    assert header_fill == "1F3864", f"Expected navy 1F3864, got {header_fill}"
