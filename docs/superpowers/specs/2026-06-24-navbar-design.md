# Sidebar Navbar Redesign — Design Spec
**Date:** 2026-06-24

## Goal

Replace the current Streamlit button-based sidebar navigation with a visually polished left-accent design that adds icons, section grouping, and a clear active-state indicator.

## Approved Design

**Direction:** Left-accent (Option A)
**Accent colour:** Indigo (`#6366f1` border / `#4338ca` text / `#eef2ff` background tint)

### Visual anatomy

```
┌──────────────────────────┐
│ 🏥 MAA Records           │  ← bold title, bottom border
├──────────────────────────┤
│ MAIN               ↑ section label (10px, uppercase, muted)
│                          │
│ ▌📊 Dashboard            │  ← active: indigo left-border + tint bg + bold indigo text
│   📥 Ingest              │  ← inactive: transparent border + muted text
│   🗂️ Admissions          │
│   📈 Reports             │
│ ──────────────────       │  ← thin separator line
│ CLINICAL                 │
│   🩺 Doctor Share        │
│                          │
│  [page-specific content] │  ← Doctor Share filters etc. unchanged
└──────────────────────────┘
```

### Active-state rules
- Active item: `border-left: 3px solid #6366f1`, `background: #eef2ff`, text `font-weight: 600; color: #4338ca`
- Inactive item: `border-left: 3px solid transparent`, text `color: #6b7280`
- Hover state: light `#f5f3ff` background tint (no border change)

### Icon map
| Page | Icon |
|---|---|
| Dashboard | 📊 |
| Ingest | 📥 |
| Admissions | 🗂️ |
| Reports | 📈 |
| Doctor Share | 🩺 |

## Implementation Approach

Streamlit sidebar buttons cannot be replaced with arbitrary HTML that calls Python — clicking custom HTML cannot directly invoke `st.session_state`. Instead:

1. **Keep the existing `st.button` nav calls** in `app.py`. Update each button label to include its icon emoji (e.g. `"📊 Dashboard"`). The buttons continue to handle all routing via `_nav()`.
2. **Inject a `<style>` block** via `st.markdown(unsafe_allow_html=True)` at the top of `app.py` that overrides sidebar button appearance:
   - Strip default button borders and backgrounds.
   - Use `box-shadow: inset 3px 0 0 #6366f1` on primary buttons to fake the left accent border (true `border-left` doesn't work cleanly on Streamlit buttons but inset box-shadow does).
   - Apply `background: #eef2ff; color: #4338ca; font-weight: 600` to active (primary) buttons.
   - Apply muted `color: #6b7280` to inactive (secondary) buttons.
   - Left-align button text, add hover tint.
3. **Section labels** ("MAIN", "CLINICAL") stay as `st.caption()` calls — no change needed there.
4. **Active state** continues to be driven by the existing `type="primary" if active else "secondary"` logic — no change to the routing model.
5. **Page-specific sidebar content** (Doctor Share filters etc.) renders below the nav buttons unchanged.

### What changes
- `app.py`: add icon prefixes to button labels; inject a `<style>` block above the sidebar block.
- No changes to any `ui/` modules.

### What does NOT change
- Routing logic (`_nav`, `_PAGE_MAP`, query params)
- Page-specific sidebar content in `ui/doctor_share.py` and others
- Any page content area

## Out of Scope
- Dark mode support
- Collapsible sidebar
- Notification badges on nav items
