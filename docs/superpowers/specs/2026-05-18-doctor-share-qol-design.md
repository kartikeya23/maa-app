# Doctor Share QoL Improvements — Design Spec

**Date:** 2026-05-18  
**Status:** Approved

---

## Scope

Six incremental improvements to the Doctor Share page and its entry edit dialog. No schema changes beyond adding `month` to the `update_doctor_expense` allowlist.

---

## 1. Edit Dialog — Expose patient_name and admission_date

**What:** Add `patient_name` and `admission_date` as editable fields at the top of the "✏️ Edit Entry" tab, above the expense inputs.

**Why:** Both fields are writable in `update_doctor_expense` but not exposed in the UI, making correction of typos or wrong dates impossible without direct DB access.

**How:** Two inputs in a two-column row:
- `st.text_input("Patient Name", ...)` — pre-filled from `r["patient_name"]`
- `st.date_input("Admission Date", ...)` — pre-filled, stored as ISO string on save

Both are included in the `db.update_doctor_expense` call on "💾 Save Changes".

---

## 2. Edit Dialog — Advanced Expander

**What:** A collapsed `st.expander("⚙️ Advanced")` at the bottom of the Edit tab containing:
1. **Filing month** — `st.text_input("Filing Month (YYYY-MM)", value=r["month"])` — moves the entry to a different month bucket on save
2. **Doctor Paid toggle** — `st.checkbox("Paid to Doctor", value=bool(r["doctor_paid"]))` — sets `doctor_paid` directly
3. **Payment month** — moves the existing `doctor_payment_month` text input here (only relevant when marking paid); still shown and saved regardless

The existing "Payment Month (YYYY-MM)" field is removed from the regular section and placed here. The `Save Changes` button covers all fields including advanced ones.

**DB change:** Add `"month"` and `"doctor_paid"` to the `allowed` set in `update_doctor_expense`.

---

## 3. Export Color Schemes

**What:** Visually differentiate the two download buttons and their Excel reports.

| Export | Header fill | Accent |
|---|---|---|
| Internal (`generate_doctor_internal`) | Dark navy `#1F3864` (unchanged) | Blue tone |
| Doctor Copy (`generate_doctor_copy`) | Deep teal `#00695C` | Teal tone |

**How:**
- Add a second header fill constant in `reports.py`: `DOCTOR_COPY_HEADER_FILL = StylePatternFill(fill_type="solid", fgColor="00695C")`
- Pass it into `_write_sheet` via an optional `header_fill` parameter (defaults to existing `HEADER_FILL`).
- In `app.py`, style the two download buttons differently using `st.markdown` + custom CSS or `type=` parameter differences to reinforce the visual distinction.

---

## 4. Refresh Button

**What:** A `🔄 Refresh` button placed inline with the entry-count line at the top of the table section.

**How:** `st.button("🔄", help="Refresh entries", key="ds_refresh")` — if clicked, calls `st.rerun()`. Placed in the same row as the count markdown using `st.columns`.

---

## 5. Bulk Actions — Unmark Paid + Change Month

**What:** Extend the existing bulk-action bar (shown when rows are selected) with two new actions:

**Unmark Paid:**
- Button `✖ Unmark Paid` — sets `doctor_paid = 0` and clears `doctor_payment_month` for all selected IDs.
- New DB function: `unmark_doctor_paid(conn, ids)` — mirrors `mark_doctor_paid`.

**Bulk Change Month:**
- A text input `"Move to month (YYYY-MM)"` and a `📅 Change Month` button.
- On click, calls `update_doctor_expense(conn, id, {"month": target_month})` for each selected ID.
- Validates `YYYY-MM` format before executing; shows error if invalid.
- After success, calls `st.rerun()`.

**Layout:** The bulk action bar becomes a 4-column layout:
```
[Pay month input + ✔ Mark Paid] | [✖ Unmark Paid] | [Target month input + 📅 Change Month] | [🗑 Delete]
```

---

## 6. QoL Polish

**Payment month in table row:**  
When `doctor_paid == 1` and `doctor_payment_month` is set, show the payment month as small grey text beneath the 🟢 dot in the last column of each table row.

**Last updated in dialog caption:**  
Add `updated_at` to the dialog caption line: `Month: {r['month']} · Adm: {r['admission_date']} · Updated: {r['updated_at'][:10]}{tid_badge}`

---

## Files Changed

| File | Change |
|---|---|
| `db.py` | Add `month`, `doctor_paid` to `update_doctor_expense` allowlist; add `unmark_doctor_paid` function |
| `reports.py` | Add `DOCTOR_COPY_HEADER_FILL`; add `header_fill` param to `_write_sheet`; pass teal fill to `generate_doctor_copy` |
| `app.py` | Edit dialog: patient_name/admission_date inputs, Advanced expander; refresh button; extended bulk action bar; table row payment month display |
