"""
One-time import of historical Doctor Share data from the Kavesh Year Combined CSV.

Usage:
    python import_doctor_data.py <path_to_csv> [--dry-run]

The CSV is expected to have columns:
    Patient Name, Discharge Date, Hospital Ex, Pharmacy Ex, Dialysis Ex,
    Total Ex, Revenue, Difference, MAA Payment, Hospital Share, Doctor Share,
    Paid, Comments

Doctor share parameterisation:
  - If the pre-computed doctor_share / (maa_payment - total_ex) is within 0.005
    of a known percentage (33%, 40%, 45%, 50%, 60%, 67%), store doctor_pct only.
  - Otherwise store the raw value as doctor_flat (covers flat-fee referral cases
    like ₹10,000 and ₹11,000 entries).
"""

import csv
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import log  # noqa: E402

logger = logging.getLogger("maa.import_doctor_data")

DB_PATH = Path(__file__).parent.parent / "maa.db"

_KNOWN_PCTS = [1 / 3, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 2 / 3]
_PCT_TOLERANCE = 0.005


def _parse_currency(value: str) -> float:
    if not value:
        return 0.0
    cleaned = value.replace("₹", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _parse_date(value: str) -> str | None:
    """DD/MM/YYYY → YYYY-MM-DD"""
    parts = (value or "").strip().split("/")
    if len(parts) == 3:
        return f"{parts[2]}-{parts[1]}-{parts[0]}"
    return None


def _infer_share_params(
    doctor_share: float, maa_payment: float, total_ex: float
) -> tuple[float, float | None]:
    """Return (doctor_pct, doctor_flat) from the pre-computed doctor_share."""
    net = maa_payment - total_ex
    if net > 0 and doctor_share > 0:
        ratio = doctor_share / net
        for pct in _KNOWN_PCTS:
            if abs(ratio - pct) < _PCT_TOLERANCE:
                return pct, None
    # Flat fee (referral bonus, cash patient, or unusual split)
    return 0.4, doctor_share if doctor_share > 0 else None


def _map_maa_status(paid_col: str, maa_payment: float) -> str | None:
    paid_col = paid_col.strip()
    if paid_col == "Rejected":
        return "Rejected"
    if maa_payment > 0:
        return "Claim Paid"
    if paid_col == "Not Recieved":
        return "Claim Approved"
    return None


def run(csv_path: str, dry_run: bool = False) -> None:
    conn = sqlite3.connect(DB_PATH)
    # Ensure schema exists
    conn.executescript(
        """
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
    )

    inserted = 0
    skipped = 0
    errors: list[str] = []

    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for lineno, row in enumerate(reader, start=2):  # 1-indexed, header = line 1
            name = row.get("Patient Name", "").strip()
            if not name:
                skipped += 1
                continue

            admission_date = _parse_date(row.get("Discharge Date", ""))
            if not admission_date:
                errors.append(f"  Line {lineno}: {name!r} — missing discharge date, skipped")
                skipped += 1
                continue

            month = admission_date[:7]  # YYYY-MM

            hosp_ex    = _parse_currency(row.get("Hospital Ex", ""))
            pharma_ex  = _parse_currency(row.get("Pharmacy Ex", ""))
            dialysis_ex = _parse_currency(row.get("Dialysis Ex", ""))
            total_ex    = _parse_currency(row.get("Total Ex", ""))
            maa_payment = _parse_currency(row.get("MAA Payment", ""))
            doctor_share = _parse_currency(row.get("Doctor Share", ""))
            paid_col   = row.get("Paid", "").strip()
            comments   = row.get("Comments", "").strip() or None

            maa_status   = _map_maa_status(paid_col, maa_payment)
            doctor_paid  = 1 if paid_col == "Paid" else 0

            doctor_pct, doctor_flat = _infer_share_params(doctor_share, maa_payment, total_ex)

            if not dry_run:
                try:
                    conn.execute(
                        """INSERT INTO doctor_expenses
                               (patient_name, admission_date, month,
                                hosp_ex, pharma_ex, dialysis_ex,
                                doctor_pct, doctor_flat,
                                comments, maa_status, doctor_paid)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            name, admission_date, month,
                            hosp_ex, pharma_ex, dialysis_ex,
                            doctor_pct, doctor_flat,
                            comments, maa_status, doctor_paid,
                        ),
                    )
                except sqlite3.IntegrityError as e:
                    errors.append(f"  Line {lineno}: {name!r} — DB error: {e}")
                    skipped += 1
                    continue

            tag = "[DRY-RUN] " if dry_run else ""
            flat_info = f"flat=₹{doctor_flat:,.2f}" if doctor_flat else f"pct={doctor_pct:.0%}"
            logger.info(
                f"  {tag}{month} | {name:<30} | MAA={maa_payment:>9,.2f}"
                f" | {flat_info:<18} | paid={doctor_paid} | {maa_status or 'non-MAA'}"
            )
            inserted += 1

    if not dry_run:
        conn.commit()
    conn.close()

    logger.info("")
    if errors:
        logger.warning("Warnings / errors:\n%s\n", "\n".join(errors))

    action = "Would insert" if dry_run else "Inserted"
    logger.info("%s %d rows, skipped %d blank rows.", action, inserted, skipped)


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args or args[0] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0)

    log.setup_logging(console=True, verbose="--verbose" in args)

    dry = "--dry-run" in args
    paths = [a for a in args if not a.startswith("--")]
    if not paths:
        logger.error("Error: provide a CSV path.  Use --dry-run to preview without writing.")
        sys.exit(1)

    run(paths[0], dry_run=dry)
