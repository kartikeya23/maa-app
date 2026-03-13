#!/usr/bin/env python3
"""
Ingest MAA GenericSearchReport CSV files into the SQLite database (maa.db).

Usage:
    python ingest.py [--dry-run] [file1.csv file2.csv ...]

If no CSV files are specified, all GenericSearchReport*.csv files in the
current directory are processed in sorted order.

Dates are stored as ISO strings (YYYY-MM-DD), times as HH:MM (24h).
ID Number, Aadhaar Number, and Mobile No are stored as text to preserve
leading zeros and avoid integer precision loss.
"""

import argparse
import csv
import glob
import sys
from datetime import datetime
from pathlib import Path

import db

# CSVs live in the parent directory (one level above maa_app/)
CSV_DIR = Path(__file__).parent.parent
DEFAULT_CSV_PATTERN = str(CSV_DIR / "GenericSearchReport*.csv")

# ── Date / time parsers ───────────────────────────────────────────────────────

def parse_time_of_admission(s: str):
    """'09:06 PM' → '21:06' (HH:MM 24h string)"""
    s = s.strip()
    if not s:
        return None
    try:
        t = datetime.strptime(s, "%I:%M %p").time()
        return t.strftime("%H:%M")
    except ValueError:
        return s


def parse_date_dmy(s: str):
    """' 05,June     , 2025' → '2025-06-05' (ISO YYYY-MM-DD string)"""
    s = s.strip()
    if not s:
        return None
    try:
        normalized = " ".join(s.replace(",", " ").split())
        return datetime.strptime(normalized, "%d %B %Y").strftime("%Y-%m-%d")
    except ValueError:
        return s


def parse_claim_submission_dt(s: str):
    """'05/06/25' → '2025-06-05' (ISO YYYY-MM-DD string)"""
    s = s.strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%d/%m/%y").strftime("%Y-%m-%d")
    except ValueError:
        return s


def parse_modified_date(s: str):
    """'23,July     , 2025 12:00 AM' → '2025-07-23T00:00:00'"""
    s = s.strip()
    if not s:
        return None
    try:
        normalized = " ".join(s.replace(",", " ").split())
        return datetime.strptime(normalized, "%d %B %Y %I:%M %p").isoformat(timespec="seconds")
    except ValueError:
        return s


def parse_payment_date(s: str):
    """'23-JUL-25 12.00.00 AM' → '2025-07-23T00:00:00'"""
    s = s.strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%d-%b-%y %I.%M.%S %p").isoformat(timespec="seconds")
    except ValueError:
        return s


# ── Numeric coercions ─────────────────────────────────────────────────────────

def to_int(s: str):
    s = s.strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return s


def to_float(s: str):
    s = s.strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return s


def to_text(s: str):
    """Store as plain text string (for IDs, phone numbers)."""
    s = s.strip()
    return s if s else None


# ── CSV column → DB column mapping ───────────────────────────────────────────

# Maps CSV header → (db_column, transform_fn)
COLUMN_MAP = {
    "TID":                    ("tid",                   to_text),
    "Patient Name":           ("patient_name",          to_text),
    "Hospital Name":          ("hospital_name",         to_text),
    "Hospital Code":          ("hospital_code",         to_text),
    "Hospital Type":          ("hospital_type",         to_text),
    "Date of Admission":      ("date_of_admission",     parse_date_dmy),
    "Time of Admission":      ("time_of_admission",     parse_time_of_admission),
    "Date of Discharge":      ("date_of_discharge",     parse_date_dmy),
    "Time of Discharge":      ("time_of_discharge",     parse_time_of_admission),
    "Modified Date":          ("modified_date",         parse_modified_date),
    "Pkg Code":               ("pkg_code",              to_text),
    "Pkg Name":               ("pkg_name",              to_text),
    "Pkg Rate":               ("pkg_rate",              to_float),
    "Id Type":                ("id_type",               to_text),
    "Id Number":              ("id_number",             to_text),
    "District Name":          ("district_name",         to_text),
    "Aadhaar Number":         ("aadhaar_number",        to_text),
    "Aadhaar Name":           ("aadhaar_name",          to_text),
    "Policy Year":            ("policy_year",           to_text),
    "Mobile No":              ("mobile_no",             to_text),
    "Status":                 ("status",                to_text),
    "Payment Type":           ("payment_type",          to_text),
    "Query Raised":           ("query_raised",          to_int),
    "Claim Number":           ("claim_number",          to_text),
    "Approved Amount":        ("approved_amount",       to_float),
    "Paid Amount":            ("paid_amount",           to_float),
    "Gender":                 ("gender",                to_text),
    "Age":                    ("age",                   to_int),
    "Payment Date":           ("payment_date",          parse_payment_date),
    "Bank UTR Number":        ("bank_utr_number",       to_text),
    "TPA Name":               ("tpa_name",              to_text),
    "Claim Processor Name":   ("claim_processor_name",  to_text),
    "Claim Processor SSOID":  ("claim_processor_ssoid", to_text),
    "Pkg Speciality Name":    ("pkg_speciality_name",   to_text),
    "Package Remark":         ("package_remark",        to_text),
    "Claim Submission Dt":    ("claim_submission_dt",   parse_claim_submission_dt),
}


def parse_csv(csv_path: str) -> list[dict]:
    """Read a CSV file and return a list of db-ready row dicts."""
    rows = []
    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for csv_row in reader:
            row = {}
            for csv_col, (db_col, transform) in COLUMN_MAP.items():
                raw = csv_row.get(csv_col, "")
                row[db_col] = transform(raw)
            rows.append(row)
    return rows


def ingest(csv_files: list[str], dry_run: bool = False):
    conn = db.init_db()
    total_new = total_updated = total_unchanged = 0

    for csv_path in csv_files:
        rows = parse_csv(csv_path)

        if dry_run:
            print(f"  [DRY RUN] {Path(csv_path).name}: {len(rows)} rows parsed (DB not modified)")
            total_new += len(rows)
        else:
            new, updated, unchanged = db.upsert_claims(conn, rows)
            name = Path(csv_path).name
            print(f"  Ingested {name}: {len(rows)} rows — {new} new, {updated} updated, {unchanged} unchanged")
            total_new += new
            total_updated += updated
            total_unchanged += unchanged

    if dry_run:
        print(f"\nDry run complete — {total_new} rows parsed total. DB NOT modified.")
    else:
        total = db.get_total_record_count(conn)
        print(f"\nTotal: {total_new} new, {total_updated} updated, {total_unchanged} unchanged")
        print(f"Database now contains {total} records.")

    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("csv_files", nargs="*",
                        help="CSV files to ingest (default: GenericSearchReport*.csv)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would be added without modifying the database")
    args = parser.parse_args()

    csv_files = args.csv_files or sorted(glob.glob(DEFAULT_CSV_PATTERN))
    if not csv_files:
        print("No CSV files found. Pass file paths or run from the exports directory.")
        sys.exit(1)

    ingest(csv_files, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
