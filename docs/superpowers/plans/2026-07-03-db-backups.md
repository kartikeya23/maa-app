# Daily Rotating DB Backups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Snapshot `maa.db` to a rotating local `backups/` folder once per day on app launch, so hand-entered doctor share data survives corruption or accidental deletion.

**Architecture:** A single new function `db.backup_db(conn, backup_dir, keep)` uses SQLite's online backup API to copy the live DB to a date-named file and prune old copies. `app.py` calls it once per server process via `@st.cache_resource` and shows the result as a sidebar footer caption. No new dependencies.

**Tech Stack:** Python 3 stdlib (`sqlite3`, `pathlib`, `datetime`), Streamlit, pytest.

**Spec:** `docs/superpowers/specs/2026-07-03-db-backups-design.md`

## Global Constraints

- Backup target filename: `maa-YYYY-MM-DD.db` (local date), inside `backups/` next to `maa.db`.
- Default retention: newest **14** `maa-*.db` files; pruning must never touch files not matching `maa-*.db`.
- `backup_db()` **never raises** — it returns `(path, error)` where exactly one side is meaningful.
- A backup failure must never block or crash app startup.
- Run the full test suite once at the end of each task, not after every file edit.

---

### Task 1: `db.backup_db()` with tests

**Files:**
- Modify: `db.py` (add `BACKUP_DIR` constant near `DB_PATH` at line ~15; add `backup_db()` at the end of the file)
- Create: `tests/test_backup.py`

**Interfaces:**
- Consumes: `db.init_db(path)` (existing), `mem_db` / `mem_db_with_claims` fixtures from `tests/conftest.py`.
- Produces: `db.BACKUP_DIR: Path` and `db.backup_db(conn, backup_dir=BACKUP_DIR, keep=14) -> tuple[Path | None, str | None]`. Task 2 imports and calls this exact signature.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_backup.py`:

```python
import sqlite3
from datetime import datetime

import db


def _today_name() -> str:
    return f"maa-{datetime.now().strftime('%Y-%m-%d')}.db"


def test_backup_creates_valid_copy(mem_db_with_claims, tmp_path):
    backup_dir = tmp_path / "backups"
    path, err = db.backup_db(mem_db_with_claims, backup_dir)

    assert err is None
    assert path == backup_dir / _today_name()
    assert path.exists()

    copy = sqlite3.connect(path)
    try:
        n = copy.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
    finally:
        copy.close()
    assert n == 2


def test_backup_same_day_is_noop(mem_db_with_claims, tmp_path):
    backup_dir = tmp_path / "backups"
    first_path, _ = db.backup_db(mem_db_with_claims, backup_dir)

    # Change the source DB, then back up again the same day.
    mem_db_with_claims.execute("DELETE FROM claims")
    mem_db_with_claims.commit()
    second_path, err = db.backup_db(mem_db_with_claims, backup_dir)

    assert err is None
    assert second_path == first_path

    # The backup still holds the pre-deletion data — proving no-op.
    copy = sqlite3.connect(first_path)
    try:
        n = copy.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
    finally:
        copy.close()
    assert n == 2


def test_backup_prunes_to_keep_newest(mem_db_with_claims, tmp_path):
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    # 16 old dated backups + 2 files that must never be touched.
    old_names = [f"maa-2026-06-{d:02d}.db" for d in range(1, 17)]
    for name in old_names:
        (backup_dir / name).write_bytes(b"old")
    (backup_dir / "notes.txt").write_text("keep me")
    (backup_dir / "other.db").write_bytes(b"keep me")

    path, err = db.backup_db(mem_db_with_claims, backup_dir, keep=14)

    assert err is None
    remaining = sorted(p.name for p in backup_dir.glob("maa-*.db"))
    # Newest 14 of the 17 dated files (16 old + today's) survive.
    expected = sorted(old_names)[3:] + [_today_name()]
    assert remaining == sorted(expected)
    assert (backup_dir / "notes.txt").exists()
    assert (backup_dir / "other.db").exists()


def test_backup_bad_dir_returns_error(mem_db, tmp_path):
    blocked = tmp_path / "blocked"
    blocked.write_text("I am a file, not a directory")

    path, err = db.backup_db(mem_db, blocked)

    assert path is None
    assert err  # non-empty message


def test_backup_failure_cleans_partial_file(tmp_path):
    class BoomConn:
        def backup(self, dest):
            raise RuntimeError("boom")

    backup_dir = tmp_path / "backups"
    path, err = db.backup_db(BoomConn(), backup_dir)

    assert path is None
    assert "boom" in err
    # The partially-created target must not linger — it would make
    # tomorrow's run treat a corrupt file as a valid backup.
    assert not (backup_dir / _today_name()).exists()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source .venv/bin/activate && python -m pytest tests/test_backup.py -v`
Expected: 5 failures/errors with `AttributeError: module 'db' has no attribute 'backup_db'`.

- [ ] **Step 3: Implement `backup_db` in `db.py`**

Below `DB_PATH = Path(__file__).parent / "maa.db"` (line 15), add:

```python
BACKUP_DIR = Path(__file__).parent / "backups"
```

At the end of `db.py`, add:

```python
# ── Backups ───────────────────────────────────────────────────────────────────

def backup_db(
    conn: sqlite3.Connection,
    backup_dir: Path = BACKUP_DIR,
    keep: int = 14,
) -> tuple[Path | None, str | None]:
    """Snapshot the live DB to backup_dir/maa-YYYY-MM-DD.db.

    No-op if today's backup already exists. After a successful backup,
    keep only the newest `keep` maa-*.db files. Returns (path, error);
    never raises — a failed backup must not block app startup.
    """
    target = None
    try:
        backup_dir = Path(backup_dir)
        backup_dir.mkdir(parents=True, exist_ok=True)
        target = backup_dir / f"maa-{datetime.now().strftime('%Y-%m-%d')}.db"
        if target.exists():
            return target, None

        dest = sqlite3.connect(target)
        try:
            with dest:
                conn.backup(dest)
        finally:
            dest.close()

        # Dated names sort lexically == chronologically.
        for old in sorted(backup_dir.glob("maa-*.db"))[:-keep]:
            old.unlink()
        return target, None
    except Exception as e:
        # Remove a partial target so tomorrow's run doesn't mistake a
        # corrupt file for a valid backup.
        if target is not None:
            target.unlink(missing_ok=True)
        return None, str(e)
```

Note: `datetime` is already imported at the top of `db.py`.

- [ ] **Step 4: Run the full test suite**

Run: `source .venv/bin/activate && python -m pytest -q`
Expected: 57 passed (52 existing + 5 new).

- [ ] **Step 5: Commit**

```bash
git add db.py tests/test_backup.py
git commit -m "feat: add db.backup_db() — daily rotating SQLite backups"
```

---

### Task 2: Wire backups into app startup + sidebar caption

**Files:**
- Modify: `app.py` (the `get_conn()` block at lines 21-26, and the end of the file after `_PAGE_MAP[...].render(conn)` at line 72)

**Interfaces:**
- Consumes: `db.backup_db(conn) -> tuple[Path | None, str | None]` from Task 1.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add a cached backup runner in `app.py`**

Replace lines 21-26 of `app.py`:

```python
@st.cache_resource
def get_conn():
    return db.init_db()


conn = get_conn()
```

with:

```python
@st.cache_resource
def get_conn():
    return db.init_db()


@st.cache_resource
def run_daily_backup() -> tuple:
    """Once per server process; date-named file makes it daily."""
    return db.backup_db(get_conn())


conn = get_conn()
backup_path, backup_error = run_daily_backup()
```

- [ ] **Step 2: Add the sidebar footer caption**

At the end of `app.py`, after `_PAGE_MAP[st.session_state["_page"]].render(conn)`, add:

```python
with st.sidebar:
    if backup_error:
        st.caption(f"⚠️ Backup failed: {backup_error}")
    elif backup_path:
        st.caption(f"Last backup: {backup_path.stem.removeprefix('maa-')}")
```

(Rendering this after the page's `render()` places it below page-specific sidebar content — a true footer.)

- [ ] **Step 3: Smoke-test the app**

Run:

```bash
source .venv/bin/activate
python - <<'EOF'
import ast, sys
ast.parse(open("app.py").read())
print("app.py parses OK")
EOF
streamlit run app.py --server.headless true &
sleep 5
curl -sf http://localhost:8501 > /dev/null && echo "app responds OK"
kill %1
```

Expected: `app.py parses OK`, then `app responds OK`.
Then verify: `ls backups/` shows `maa-<today>.db`, and its size is close to `maa.db` (~4.8 MB).

- [ ] **Step 4: Run the full test suite**

Run: `source .venv/bin/activate && python -m pytest -q`
Expected: 57 passed.

- [ ] **Step 5: Commit**

```bash
git add app.py
git commit -m "feat: run daily DB backup on app launch, show status in sidebar"
```

---

### Task 3: Docs — .gitignore, README, CLAUDE.md

**Files:**
- Modify: `.gitignore` (Database files section, lines ~14-19)
- Modify: `README.md` (Features list ~line 7, Architecture tree ~line 16, new Backups section after "## Running")
- Modify: `CLAUDE.md` (Architecture bullet for `db.py`)

**Interfaces:**
- Consumes: behaviour established in Tasks 1-2 (paths, retention, restore procedure).
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add `backups/` to `.gitignore`**

In the `# Database files` section of `.gitignore` (after `*.sqlite`), add:

```
backups/
```

(Already covered by `*.db`, added for clarity per spec.)

- [ ] **Step 2: Update `README.md`**

1. In the Features list, add:

```markdown
- **Automatic backups** — Daily rotating snapshot of `maa.db` to `backups/` on app launch (newest 14 kept)
```

2. In the Architecture tree, after the `doctors.toml.example` line, add:

```
backups/              Daily rotating DB snapshots (maa-YYYY-MM-DD.db, gitignored)
```

3. After the "## Running" section, add:

````markdown
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
````

- [ ] **Step 3: Update `CLAUDE.md`**

In the Architecture section, extend the `db.py` bullet. Change:

```markdown
- **`db.py`** — All database access. Schema: `claims` table (PK: `tid, pkg_code, claim_number`) + `claims_hash` for upsert change detection via MD5. Financial year starts April 1 (`fy_of()`).
```

to:

```markdown
- **`db.py`** — All database access. Schema: `claims` table (PK: `tid, pkg_code, claim_number`) + `claims_hash` for upsert change detection via MD5. Financial year starts April 1 (`fy_of()`). `backup_db()` writes daily rotating snapshots to `backups/` (newest 14 kept), called from `app.py` on launch.
```

- [ ] **Step 4: Run the full test suite**

Run: `source .venv/bin/activate && python -m pytest -q`
Expected: 57 passed.

- [ ] **Step 5: Commit**

```bash
git add .gitignore README.md CLAUDE.md
git commit -m "docs: document daily DB backups in README and CLAUDE.md, gitignore backups/"
```
