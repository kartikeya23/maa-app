# MAA Payment Record Manager

A web application for tracking hospital admission claims under the MAA (Mother's Absolute Affection) health insurance scheme. Supports CSV ingestion, claim browsing with filters, and Excel report generation.

## Features

- **Dashboard** — Overview statistics, monthly approved vs paid bar chart, status breakdown
- **Ingest** — Upload `GenericSearchReport*.csv` files with dry-run validation and change detection
- **Admissions** — Filterable, paginated table with per-claim package details and Excel export
- **Reports** — Multiple report types (Admission Summary, Monthly, Financial Year, Raw Export) as downloadable `.xlsx` files

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
