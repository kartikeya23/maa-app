# MAA Payment Record Manager

A web application for tracking hospital admission claims under the MAA (Mother's Absolute Affection) health insurance scheme. Supports CSV ingestion, claim browsing with filters, Excel report generation, and per-doctor expense tracking.

## Features

- **Dashboard** — Overview statistics, monthly approved vs paid bar chart, status breakdown
- **Ingest** — Upload `GenericSearchReport*.csv` files with dry-run validation and change detection; or fetch directly from the MAA portal via browser session reuse
- **Admissions** — Filterable, paginated table (name search + date/policy/status/speciality) with per-claim package details and Excel export
- **Reports** — Multiple report types (Admission Summary, Monthly, Financial Year, Raw Export) as downloadable `.xlsx` files
- **Doctor Share** — Per-doctor expense tracking with MAA claim linking, auto-status detection, monthly/range filters, bulk mark-paid, and internal/doctor-copy Excel exports
- **Automatic backups** — Daily rotating snapshot of `maa.db` to `backups/` on app launch (newest 14 kept)

## Architecture

```
app.py                Entry point: page config, DB connection, sidebar nav, routing
utils.py              Shared helpers (fmt_inr currency formatter)
db.py                 All database access (SQLite). Schema: claims + doctor_expenses
ingest.py             CSV parsing and ingestion (CLI and library)
fetch.py              MAA portal scraper — browser session reuse + Playwright fallback
reports.py            In-memory .xlsx generation via openpyxl
doctors.toml          Local config: doctor names and default fee percentages (gitignored)
doctors.toml.example  Template for doctors.toml
backups/              Daily rotating DB snapshots (maa-YYYY-MM-DD.db, gitignored)
ui/
  dashboard.py        Dashboard page
  ingest.py           Ingest page
  admissions.py       Admissions page
  reports.py          Reports page
  doctor_share.py     Doctor Share page + entry/delete dialogs
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Copy and configure the doctor list:

```bash
cp doctors.toml.example doctors.toml
# Edit doctors.toml with your doctor names and default fee percentages
```

## Running

```bash
streamlit run app.py
```

## Backups

On the first launch of each day, the app snapshots `maa.db` to
`backups/maa-YYYY-MM-DD.db` using SQLite's online backup API and keeps the
newest 14 copies. The sidebar footer shows the last backup date.

**To restore:** quit the app, then

```bash
cp backups/maa-YYYY-MM-DD.db maa.db
```

and relaunch. Claims can always be re-fetched from the portal, but doctor
share entries exist only in this database — restore from the most recent
good backup.

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

## Configuration

### doctors.toml

Doctor names and their default fee percentages are stored in `doctors.toml` (gitignored, not committed). Copy `doctors.toml.example` and edit:

```toml
[doctors]
"Dr. Smith" = 0.40
"Dr. Jones" = 0.35
```

The percentage is applied to `(MAA payment − expenses)` when no flat override is set on an entry. Restart the app after editing.

## Tech Stack

- [Streamlit](https://streamlit.io) — Web UI
- [SQLite](https://www.sqlite.org) — Local database (`maa.db`)
- [pandas](https://pandas.pydata.org) — Data manipulation
- [openpyxl](https://openpyxl.readthedocs.io) — Excel report generation
- [Plotly](https://plotly.com/python/) — Charts
- [Playwright](https://playwright.dev/python/) — MAA portal scraping (optional)
