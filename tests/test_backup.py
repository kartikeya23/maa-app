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


def test_prune_failure_keeps_new_backup(mem_db_with_claims, tmp_path, monkeypatch):
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    for d in range(1, 16):
        (backup_dir / f"maa-2026-06-{d:02d}.db").write_bytes(b"old")

    from pathlib import Path
    real_unlink = Path.unlink

    def failing_unlink(self, missing_ok=False):
        if self.name.startswith("maa-2026-06-"):
            raise PermissionError("locked")
        return real_unlink(self, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", failing_unlink)
    path, err = db.backup_db(mem_db_with_claims, backup_dir)

    assert path is not None and path.exists()  # valid backup survives
    assert err and "prun" in err.lower()


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
