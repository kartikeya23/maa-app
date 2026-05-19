# MAA Payment Record Manager

A web application for tracking hospital admission claims under the MAA (Mother's Absolute Affection) health insurance scheme. Supports CSV ingestion, claim browsing with filters, and Excel report generation.

## Features

- **Dashboard** — Overview statistics, monthly approved vs paid bar chart, status breakdown
- **Ingest** — Upload `GenericSearchReport*.csv` files with dry-run validation and change detection
- **Admissions** — Filterable, paginated table with per-claim package details and Excel export
- **Reports** — Multiple report types (Admission Summary, Monthly, Financial Year, Raw Export) as downloadable `.xlsx` files
- **Doctor Share** — Per-doctor expense tracking with MAA claim linking, monthly filters, bulk mark-paid, and internal/doctor-copy Excel exports

## Architecture

```
app.py            Entry point: page config, DB connection, sidebar nav, routing
utils.py          Shared UI helpers (fmt_inr)
db.py             All database access (SQLite). Schema: claims + doctor_expenses
ingest.py         CSV parsing and ingestion (CLI and library)
reports.py        In-memory .xlsx generation via openpyxl
pages/
  dashboard.py    Dashboard page
  ingest.py       Ingest page
  admissions.py   Admissions page
  reports.py      Reports page
  doctor_share.py Doctor Share page + entry/delete dialogs
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Running

```bash
streamlit run app.py
```

## Data Ingestion

CSV files should be in the `GenericSearchReport*.csv` format exported from the claims portal.

```bash
# Ingest specific files
python ingest.py file1.csv file2.csv

# Dry-run (validate without writing)
python ingest.py --dry-run

# Auto-detect CSV files from parent directory
python ingest.py
```

## Tech Stack

- [Streamlit](https://streamlit.io) — Web UI
- [SQLite](https://www.sqlite.org) — Local database (`maa.db`)
- [pandas](https://pandas.pydata.org) — Data manipulation
- [openpyxl](https://openpyxl.readthedocs.io) — Excel report generation
- [Plotly](https://plotly.com/python/) — Charts
