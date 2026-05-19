# pages/ingest.py
import glob
from pathlib import Path

import streamlit as st

import db
import ingest as ingest_module


def render(conn) -> None:
    st.title("Ingest CSV Files")

    uploaded = st.file_uploader(
        "Upload GenericSearchReport CSV files",
        type=["csv"],
        accept_multiple_files=True,
    )
    auto_detect = st.button("Auto-detect CSVs in current directory")
    dry_run     = st.checkbox("Dry Run (parse only, don't write to DB)")

    csv_paths: list[str] = []

    if uploaded:
        tmp_dir = Path("/tmp/maa_uploads")
        tmp_dir.mkdir(exist_ok=True)
        for f in uploaded:
            dest = tmp_dir / f.name
            dest.write_bytes(f.read())
            csv_paths.append(str(dest))

    if auto_detect:
        # pages/ingest.py lives two levels below the project root's parent
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
                total_new       += new
                total_updated   += updated
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
