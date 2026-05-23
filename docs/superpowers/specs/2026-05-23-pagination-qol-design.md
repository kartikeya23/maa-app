# Pagination & QOL UX Improvements — Design Spec

**Date:** 2026-05-23  
**Scope:** `ui/admissions.py`, `ui/doctor_share.py`, `README.md`, `CLAUDE.md`

---

## 1. Admissions — Pagination (Prev/Next)

**Problem:** The Admissions page uses `st.number_input("Page")` — a bare number input with no Prev/Next buttons, no session-state persistence, and no page-reset when filters change.

**Solution:**

- Store the current page in `st.session_state["adm_page_num"]` (default 1), matching Doctor Share's pattern.
- Render `← Prev` / centered `Page X of Y · rows A–B of N` / `Next →` below the dataframe, shown only when `total_pages > 1`.
- Detect filter changes by hashing the active `filters` dict (including name search) with `json.dumps(..., sort_keys=True)`. Store the hash in `st.session_state["adm_filters_hash"]`. If the hash differs at render time, reset `adm_page_num` to 1 before slicing.

**Page size:** Keep existing 50 rows (no selector needed per YAGNI).

---

## 2. Admissions — Name Search

**Problem:** The Admissions sidebar has no way to find a patient by name. Users must scroll through pages.

**Solution:**

- Add a `st.text_input("Search name")` at the top of the Admissions sidebar (before date filters).
- After `df = db.query_admissions(conn, filters)`, apply client-side filtering:  
  `df = df[df["patient_name"].str.contains(name, case=False, na=False)]`
- The name is included when computing the filter hash (so changing it also resets the page).
- No DB layer change needed.

---

## 3. Doctor Share — Month-Switch Page Reset Bug

**Problem:** When the user selects a different month in the sidebar, `ds_page_num` is not reset. If the previous page number exceeds the new month's total pages, an empty table is shown.

**Solution:**

- Store the previous months list as `st.session_state["ds_months_prev"]`.
- At the start of the listing block (before slicing), compare `selected_months` to `ds_months_prev`. If different, set `ds_page_num = 1` and update `ds_months_prev`.
- This is a 4-line addition at the top of the listing block in `doctor_share.py`.

---

## 4. README.md Updates

**Problems:**
- Architecture diagram references `pages/` (directory does not exist; actual path is `ui/`).
- `fetch.py` is not mentioned anywhere.
- Only 4 pages listed in Features; there are 5.

**Changes:**
- Rename `pages/` → `ui/` in the architecture tree and the directory listing.
- Add `fetch.py` entry: "Portal data fetcher (Playwright/session-reuse)".
- Update Features to include the portal fetch capability under Ingest.

---

## 5. CLAUDE.md Updates

**Problems:**
- Architecture section says "4 pages" but there are 5.
- `utils.py` and `fetch.py` are not mentioned.

**Changes:**
- Update bullet count / description for app.py to say 5 pages.
- Add `utils.py` — Shared helpers (`fmt_inr` currency formatter).
- Add `fetch.py` — MAA portal scraper; browser session reuse + Playwright fallback.

---

## Files Changed

| File | Change |
|------|--------|
| `ui/admissions.py` | Prev/Next pagination + name search + filter-hash page reset |
| `ui/doctor_share.py` | Month-switch page reset (4-line fix) |
| `README.md` | Fix `pages/` → `ui/`, add `fetch.py`, update feature list |
| `CLAUDE.md` | Fix page count, add `utils.py` and `fetch.py` |

## Out of Scope

- Configurable page size (YAGNI)
- DB query changes (name search is client-side)
- Any other pages (Reports, Dashboard, Ingest have no pagination to fix)
