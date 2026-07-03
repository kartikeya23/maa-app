# Daily Rotating DB Backups — Design Spec
**Date:** 2026-07-03

## Goal

Protect the hand-entered Doctor Share data in `maa.db` from corruption or accidental deletion. Claims can be re-fetched from the MAA portal; `doctor_expenses` rows cannot. Today there are zero backups.

## Approved Design

**Direction:** In-app daily snapshot on launch (local folder, rotating).

### Behaviour

1. **Trigger:** When the app starts, take a snapshot of `maa.db` — once per day. Runs inside `get_conn()` in `app.py` (already `@st.cache_resource`, so once per server process); the date-named target file makes it effectively daily.
2. **Target:** `backups/maa-YYYY-MM-DD.db`, where `backups/` sits next to `maa.db`. If today's file already exists, do nothing (reruns and restarts are free).
3. **Method:** SQLite online backup API — `sqlite3.Connection.backup()` into a destination connection. Safe while the source DB is open, unlike a raw file copy.
4. **Rotation:** After a successful backup, keep the newest **14** `maa-*.db` files in `backups/`, delete the rest (~2 weeks × ~5 MB ≈ 70 MB).
5. **Failure handling:** A backup failure must never block app startup. Catch exceptions, surface as a sidebar warning, continue.
6. **Visibility:** Sidebar footer caption, e.g. `Last backup: 2026-07-03`, driven by the newest file in `backups/` (or a warning if the last attempt failed).

### API

New function in `db.py`:

```python
def backup_db(conn, backup_dir=BACKUP_DIR, keep=14) -> tuple[Path | None, str | None]:
    """Snapshot the DB to backup_dir/maa-YYYY-MM-DD.db (no-op if today's
    exists), prune to the newest `keep` files. Returns (path, error):
    path is the backup file (existing or newly created), error is a
    human-readable message if the attempt failed. Never raises."""
```

`BACKUP_DIR = DB_PATH.parent / "backups"`. The function creates the directory if missing.

Caller contract (`app.py`): `get_conn()` stores the `(path, error)` result; the sidebar footer shows `Last backup: YYYY-MM-DD` from the path's filename, or a warning caption with the error message if the attempt failed.

### What changes

- `db.py`: add `BACKUP_DIR` constant and `backup_db()`.
- `app.py`: call `backup_db(conn)` inside `get_conn()` after `init_db()`; add sidebar footer caption with last-backup date.
- `.gitignore`: add `backups/` (already covered by `*.db`, added for clarity).
- `README.md`: new "Backups" section — where backups live, retention, and the manual restore procedure (quit app → copy `backups/maa-YYYY-MM-DD.db` over `maa.db` → relaunch).

### Testing

Unit tests (pytest, in-memory/tmp-path DBs):

- Creates `backups/maa-<today>.db`; the copy opens and contains the source rows.
- Second call same day is a no-op (file mtime/content unchanged, returns `None`).
- With >14 backup files present, prunes to the newest 14; never deletes non-matching filenames.
- Unwritable/bogus backup dir does not raise; app-facing error state is set.

## Out of Scope

- Off-machine sync (Time Machine / iCloud can be layered on by the user)
- Restore UI (restore is manual, documented in README)
- Backup before every write
- Compression
