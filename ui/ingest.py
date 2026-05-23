# pages/ingest.py
import calendar
import glob
from datetime import date, timedelta
from pathlib import Path

import streamlit as st

import db
import fetch as fetch_module
import ingest as ingest_module


def _month_options(n: int = 18) -> list[str]:
    """Return last n months as MAY_2026-style labels, newest first."""
    options = []
    d = date.today().replace(day=1)
    for _ in range(n):
        label = f"{calendar.month_abbr[d.month].upper()}_{d.year}"
        options.append(label)
        # Go to previous month
        d = (d - timedelta(days=1)).replace(day=1)
    return options


def _cached_months(months: list[str]) -> list[str]:
    return [m for m in months if (fetch_module.CACHE_DIR / f"{m}.csv").exists()]


def render(conn) -> None:
    st.title("Ingest")

    tab_upload, tab_fetch = st.tabs(["Upload CSV", "Fetch from Portal"])

    # ── Tab 1: Upload CSV ─────────────────────────────────────────────────────
    with tab_upload:
        uploaded = st.file_uploader(
            "Upload GenericSearchReport CSV files",
            type=["csv"],
            accept_multiple_files=True,
        )
        auto_detect = st.button("Auto-detect CSVs in current directory")
        dry_run = st.checkbox("Dry Run (parse only, don't write to DB)")

        csv_paths: list[str] = []

        if uploaded:
            tmp_dir = Path("/tmp/maa_uploads")
            tmp_dir.mkdir(exist_ok=True)
            for f in uploaded:
                dest = tmp_dir / f.name
                dest.write_bytes(f.read())
                csv_paths.append(str(dest))

        if auto_detect:
            csv_dir = Path(__file__).parent.parent.parent
            cwd_csvs = sorted(glob.glob(str(csv_dir / "GenericSearchReport*.csv")))
            csv_paths = cwd_csvs
            if cwd_csvs:
                st.info(f"Found {len(cwd_csvs)} CSV file(s) in `{csv_dir}`.")
            else:
                st.warning(f"No GenericSearchReport*.csv files found in `{csv_dir}`.")

        if csv_paths and st.button("Run Ingest", type="primary"):
            st.subheader("Ingest Log")
            total_new = total_updated = total_unchanged = 0

            for csv_path in csv_paths:
                try:
                    rows = ingest_module.parse_csv(csv_path)
                    new, updated, unchanged = db.upsert_claims(conn, rows, dry_run=dry_run)
                    total_new += new
                    total_updated += updated
                    total_unchanged += unchanged
                    if dry_run:
                        st.write(
                            f"🔍 `{Path(csv_path).name}` — "
                            f"**{new} new** (est.) | {updated} updated (est.) | {unchanged} unchanged"
                        )
                    else:
                        st.write(
                            f"✅ `{Path(csv_path).name}` — "
                            f"**{new} new** | {updated} updated | {unchanged} unchanged"
                        )
                except Exception as e:
                    st.error(f"❌ `{Path(csv_path).name}`: {e}")

            st.divider()
            if dry_run:
                st.info(
                    f"Dry run: **{total_new} new** (est.), {total_updated} updated (est.), "
                    f"{total_unchanged} unchanged. DB not modified."
                )
            else:
                total = db.get_total_record_count(conn)
                st.success(
                    f"Done — **{total_new} new**, {total_updated} updated, "
                    f"{total_unchanged} unchanged. "
                    f"Database total: **{total:,} records**."
                )
                st.cache_data.clear()

    # ── Tab 2: Fetch from Portal ──────────────────────────────────────────────
    with tab_fetch:
        col1, col2 = st.columns(2)
        with col1:
            ssoid = st.text_input("SSOID", key="fetch_ssoid")
        with col2:
            password = st.text_input("Password", type="password", key="fetch_password")

        browser = st.selectbox(
            "Login browser",
            ["chromium", "firefox", "webkit"],
            key="fetch_browser",
        )

        month_opts = _month_options()
        selected_months = st.multiselect(
            "Months to fetch",
            options=month_opts,
            default=month_opts[:1],
            key="fetch_months",
        )

        cached = _cached_months(selected_months) if selected_months else []
        all_cached = bool(selected_months) and len(cached) == len(selected_months)

        if cached:
            st.caption(f"Cached: {', '.join(cached)}")

        # Action buttons
        btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 1])
        do_dry_run = btn_col1.button("Dry Run", disabled=not selected_months)
        do_fetch   = btn_col2.button("Fetch & Ingest", type="primary", disabled=not selected_months)
        do_cached  = btn_col3.button(
            "Apply Cached",
            disabled=not all_cached,
            help="Apply cached CSVs to the database without fetching from the portal",
        )

        def _run_fetch(months, dry_run, fresh):

            with st.spinner("Fetching…"):
                try:
                    results = fetch_module.fetch_and_ingest(
                        months=months,
                        ssoid=ssoid,
                        password=password,
                        browser=browser,
                        dry_run=dry_run,
                        fresh=fresh,
                    )
                    st.session_state["fetch_results"] = results
                    st.session_state["fetch_dry_run"] = dry_run
                    if not dry_run:
                        st.cache_data.clear()
                except Exception as e:
                    st.error(f"Fetch failed: {e}")

        if do_dry_run:
            _run_fetch(selected_months, dry_run=True, fresh=True)

        if do_fetch:
            _run_fetch(selected_months, dry_run=False, fresh=False)

        if do_cached:
            _run_fetch(selected_months, dry_run=False, fresh=False)

        # Display results table
        if "fetch_results" in st.session_state:
            results = st.session_state["fetch_results"]
            is_dry = st.session_state.get("fetch_dry_run", False)
            st.divider()
            st.subheader("Results" + (" (dry run)" if is_dry else ""))

            rows = [
                {"Month": m, "New": n, "Updated": u, "Unchanged": c}
                for m, (n, u, c) in results.items()
            ]
            st.dataframe(rows, use_container_width=True, hide_index=True)

            total_new = sum(r["New"] for r in rows)
            total_updated = sum(r["Updated"] for r in rows)
            if is_dry:
                st.info(f"Dry run — **{total_new} new** (est.), {total_updated} updated (est.). DB not modified.")
            else:
                total = db.get_total_record_count(conn)
                st.success(
                    f"Done — **{total_new} new**, {total_updated} updated. "
                    f"Database total: **{total:,} records**."
                )
