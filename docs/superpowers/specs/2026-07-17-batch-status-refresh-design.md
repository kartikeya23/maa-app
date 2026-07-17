# Batch MAA Status Refresh — Design

**Date:** 2026-07-17
**Status:** Approved

## Problem

In the Doctor Share month view, the only way to update MAA payment statuses
after ingesting fresh portal data is to open each entry and click
"🔄 Auto-detect Status" one by one.

A second, related problem: when a claim is resubmitted/updated on the MAA
portal, it reappears under a **new TID**. The old TID's rows stay in `claims`
frozen at their last status, so a doctor-share entry linked to the old TID
never sees the payment. Real example: Usha (adm. 2026-03-12) — linked-era TID
`T12032646192267` stuck at "Pre Auth Approved" while resubmission
`T19042647167144` is "Claim Paid".

## Scope decisions

- **Local re-infer only.** The batch refresh runs `infer_maa_status` against
  the already-ingested `claims` table. It does not fetch from the portal
  (that remains the Ingest page's job).
- **Target set:** all entries in the selected month(s) that have a TID link
  and whose `maa_status` is not `Claim Paid`. View filters (status, doctor
  paid, name search) are ignored; the sweep covers the full month set.
- **Stale TIDs:** detect and *suggest* relinks; never silently re-point an
  entry to a different TID.

## Design

### 1. DB helpers (`db.py`)

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

**`find_successor_tids(conn, tid) -> list[dict]`**

- Given a linked TID, look up its portal `patient_name` +
  `date_of_admission` in `claims`, then find *other* TIDs with the same
  name + admission date.
- Exclude TIDs already linked to any `doctor_expenses` row.
- Each candidate returns with its own inferred status. Only candidates whose
  status ranks ahead of the entry's are returned
  (rank: Claim Paid > Claim Approved > Claim Raised / Query Raised >
  Rejected > blank/None — a blank or missing entry status ranks lowest, so
  any inferable successor beats it).
- Matching uses the **claims-side** name (portal spelling), not the
  doctor_expenses name, so hospital-bill spelling variations don't matter.

### 2. UI (`ui/doctor_share.py`)

- New "🔄 Refresh MAA Statuses" button in the counts row next to the
  existing refresh icon.
- On click:
  1. Collect eligible entries from `full_df` (unfiltered month set):
     `tid` present and `maa_status != 'Claim Paid'`.
  2. Run `batch_refresh_maa_status`.
  3. For entries still non-paid after re-inference (including `None`
     inference), run `find_successor_tids`.
  4. Stash results + suggestions in `st.session_state["ds_batch_results"]`,
     `st.rerun()`.
- Results panel rendered from session state (dismissible via ✕):
  - Summary line: "✅ 3 updated · 11 unchanged".
  - Table of changes: Patient, Old status → New status.
  - "Stale links" section: one row per suggestion —
    "Usha · linked T12032… (Pre Auth) · newer claim T19042… (Claim Paid)"
    with a **Relink** button that calls the existing
    `_link_and_infer_status(conn, row_id, new_tid)` and removes that
    suggestion from session state.

### 3. Edge cases

- No eligible entries → toast "No unpaid linked entries to refresh."; no
  results panel.
- `infer_maa_status` returns `None` → status untouched, successor search
  still runs (the vanished-TID case is exactly when a successor is likely).
- A successor candidate linked to another entry between render and click →
  `_link_and_infer_status` proceeds as the existing manual flow would;
  exclusion is best-effort at suggestion time.
- Suggestions never auto-apply.

### 4. Testing

- `find_successor_tids`: Usha-style fixture (old TID pre-auth, new TID paid,
  same portal name + admission date) → suggested; negatives: different
  admission date, candidate already linked to another doctor_expenses row,
  candidate status not ahead of current.
- `batch_refresh_maa_status`: status change written and logged; unchanged
  rows not rewritten; `None` inference leaves status untouched.
- Single test run at the end of implementation.
