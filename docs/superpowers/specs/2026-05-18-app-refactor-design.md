# Design: app.py Refactor + README Update

**Date:** 2026-05-18  
**Scope:** Split 910-line `app.py` into a `pages/` package, extract shared utilities, update README.

---

## Problem

`app.py` has grown to 910 lines across 5 pages (Dashboard, Ingest, Admissions, Reports, Doctor Share) plus shared setup code. It is increasingly hard to navigate and will keep growing as pages are added.

---

## Approach

**Approach A — `pages/` Python package with custom navigation preserved.**

Each page becomes a module with a single `render(conn)` function. `app.py` stays the Streamlit entry point but shrinks to ~60 lines. Shared UI helpers move to `utils.py`.

---

## File Structure

```
maa_app/
├── app.py                  # ~60 lines: page config, DB connection, sidebar nav, routing
├── utils.py                # fmt_inr + any future shared UI helpers
├── db.py                   # unchanged
├── ingest.py               # unchanged
├── reports.py              # unchanged (minor: _month_label → month_label)
├── pages/
│   ├── __init__.py         # empty
│   ├── dashboard.py        # Dashboard page
│   ├── ingest.py           # Ingest page
│   ├── admissions.py       # Admissions page
│   ├── reports.py          # Reports page
│   └── doctor_share.py     # Doctor Share page + its two @st.dialog functions
```

---

## Interface Convention

Every page module exposes exactly one public function:

```python
def render(conn) -> None: ...
```

`app.py` routing:

```python
from pages import dashboard, ingest, admissions, reports as reports_page, doctor_share

PAGE_MAP = {
    "Dashboard":    dashboard,
    "Ingest":       ingest,
    "Admissions":   admissions,
    "Reports":      reports_page,
    "Doctor Share": doctor_share,
}
PAGE_MAP[page].render(conn)
```

The alias `reports_page` is only in `app.py`'s import line — it disambiguates `pages.reports` (UI) from `reports` (Excel generator). Inside `pages/reports.py`, `import reports` resolves to the top-level `reports.py` with no conflict.

---

## Shared Utilities (`utils.py`)

`fmt_inr` moves from `app.py` (line 62) to `utils.py`. All page modules import it from there.

```python
# utils.py
def fmt_inr(val: float) -> str:
    if val is None:
        return "₹0"
    return f"₹{val:,.0f}"
```

---

## Code Review Fix: `reports._month_label`

`_month_label` in `reports.py` is a private function currently called from `app.py` in two places (the Doctor Share sidebar month selector and export labels). After the refactor these calls will live in `pages/doctor_share.py`. Making this cross-module private call explicit: rename `_month_label` → `month_label` in `reports.py` and update all call sites.

---

## Page Module Breakdown

| Module | Approx lines | Content |
|---|---|---|
| `pages/dashboard.py` | ~55 | Stats metrics, bar chart, pie chart, recent admissions table |
| `pages/ingest.py` | ~70 | File uploader, auto-detect, ingest run, log output |
| `pages/admissions.py` | ~60 | Filter sidebar, paginated table, TID detail expander, Excel download |
| `pages/reports.py` | ~135 | Report type selector, sidebar filters, preview + download for all report types |
| `pages/doctor_share.py` | ~335 | `_link_and_infer_status`, `_entry_detail_dialog`, `_confirm_delete_dialog`, full page render |

---

## README Changes

1. **Features section** — add Doctor Share entry (currently missing entirely):
   > **Doctor Share** — Per-doctor expense tracking with MAA claim linking, bulk mark-paid, and internal/doctor-copy Excel exports

2. **Architecture section** — new section describing each module's role, including `pages/` and `utils.py`

3. No changes to Setup, Running, Data Ingestion, or Tech Stack sections.

---

## What Does NOT Change

- Navigation model: custom `st.session_state["_page"]` sidebar buttons, unchanged
- `db.py`, `ingest.py` — no modifications
- `reports.py` — only the `_month_label` rename
- Test files — no modifications needed (tests target `db.py` and `reports.py` directly)
- No new features, no behavior changes — pure structural refactor

---

## Success Criteria

- `app.py` is ≤ 70 lines
- All 5 pages render and function identically
- `streamlit run app.py` starts without error
- Existing tests pass unchanged
