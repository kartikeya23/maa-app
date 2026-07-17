# Batch MAA Status Refresh — Design

**Date:** 2026-07-17
**Status:** Approved

## Problem

In the Doctor Share month view, the only way to update MAA payment statuses
after ingesting fresh portal data is to open each entry and click
"🔄 Auto-detect Status" one by one.

## Scope decisions

- **Local re-infer only.** The batch refresh runs `infer_maa_status` against
  the already-ingested `claims` table. It does not fetch from the portal
  (that remains the Ingest page's job).
- **Target set:** all entries in the selected month(s) that have a TID link
  and whose `maa_status` is not `Claim Paid`. View filters (status, doctor
  paid, name search) are ignored; the sweep covers the full month set.
- Multiple TIDs for the same patient name + admission date are separate
  admissions, not resubmissions — no cross-TID matching or relink
  suggestions are in scope.

## Design

### 1. DB helper (`db.py`)

**`batch_refresh_maa_status(conn, entries) -> list[dict]`**

- Input: list of eligible entries (`id`, `tid`, current `maa_status`,
  `patient_name`).
- For each entry, run `infer_maa_status(conn, tid)`:
  - Inferred status differs from current → write via existing
    `update_doctor_expense` (keeps `doctor_expenses_log` history).
  - Inferred equals current → no write.
  - Inferred is `None` (TID has no rows in `claims`) → leave status
    untouched.
- Returns per-entry results:
  `{id, patient_name, tid, old_status, new_status, changed}`.

### 2. UI (`ui/doctor_share.py`)

- New "🔄 Refresh MAA Statuses" button in the counts row next to the
  existing refresh icon.
- On click:
  1. Collect eligible entries from `full_df` (unfiltered month set):
     `tid` present and `maa_status != 'Claim Paid'`.
  2. Run `batch_refresh_maa_status`.
  3. Stash results in `st.session_state["ds_batch_results"]`, `st.rerun()`.
- Results panel rendered from session state (dismissible via ✕):
  - Summary line: "✅ 3 updated · 11 unchanged".
  - Table of changes: Patient, Old status → New status.

### 3. Edge cases

- No eligible entries → toast "No unpaid linked entries to refresh."; no
  results panel.
- `infer_maa_status` returns `None` → status untouched; the entry shows as
  unchanged in the results.

### 4. Testing

- `batch_refresh_maa_status`: status change written and logged; unchanged
  rows not rewritten; `None` inference leaves status untouched.
- Single test run at the end of implementation.
