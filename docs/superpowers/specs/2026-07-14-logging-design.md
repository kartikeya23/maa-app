# Logging for MAA App — Design

Date: 2026-07-14
Status: Approved

## Goal

Add comprehensive logging to the MAA Payment Record Management System to support:
after-the-fact diagnosis, an audit trail of data changes, visibility while running,
and debug-level output during development. Today there is no `logging` usage at all —
only `print()` in the CLI tools — and output vanishes entirely when the app is
launched via `MAA App.app`.

## Decisions (from brainstorming)

- Stdlib `logging` only; no new dependencies.
- Audit trail = summary lines in the log file (counts + claim keys added/updated).
  No audit DB table.
- CLI tools route **all** output through the logging module (prints replaced), with
  a console handler that keeps terminal output human-friendly.

## Design

### 1. Core setup — new `log.py` module

`setup_logging(console: bool = False, verbose: bool = False)`, idempotent (returns
early if the `maa` root logger already has handlers, so Streamlit reruns are safe).

- `TimedRotatingFileHandler` → `logs/maa.log`, rotate at midnight, `backupCount=14`
  (mirrors backup retention). `logs/` created on demand; added to `.gitignore`.
- File format: `%(asctime)s %(levelname)s %(name)s: %(message)s`
  e.g. `2026-07-14 09:32:01 INFO ingest: Ingested GenericSearchReport-Jun.csv: 412 rows — 3 new, 7 updated, 402 unchanged`
- Console handler only when `console=True` (CLI tools); message-only format so the
  terminal looks like today's `print()` output.
- INFO by default; DEBUG when `verbose=True` or env `MAA_DEBUG=1`.
- Modules use `logger = logging.getLogger("maa.<module>")` (or `__name__`-based
  child loggers under the `maa` root).

### 2. CLI tools — `ingest.py`, `fetch.py`, `scripts/import_doctor_data.py`

- Every `print()` → `logger.info()`; error paths → `logger.error()`; session-expiry
  and timeout paths in `fetch.py` → `logger.exception()` where a traceback helps.
- Each CLI gains a `--verbose` flag.
- The month-summary table in `fetch.py` is emitted as one multi-line log call so it
  renders correctly in both console and file.

### 3. Streamlit app — `app.py`, `db.py`

- `app.py` calls `setup_logging()` at import (console off). Logs: app start, backup
  result (path or error), and page render exceptions via try/except around
  `_PAGE_MAP[...].render(conn)` — `logger.exception` then re-raise so Streamlit's
  error UI still appears.
- `db.py`: `backup_db()` logs backup/prune outcomes; `upsert_claims()` logs summary
  counts plus the claim keys added/updated at INFO (the audit trail); `init_db()`
  logs schema migrations when the `OperationalError` fallback path runs.

### 4. Out of scope (YAGNI)

No log viewer UI, no JSON/structured logs, no audit table, no logging in pure
`query_*` functions.

## Testing

- Unit tests for `setup_logging`: idempotency (no duplicate handlers), rotation
  config, verbose/env-var level switching.
- Smoke test: `ingest --dry-run` writes expected lines to the log file.
- Existing tests must pass; if any capture stdout from ingest and the output format
  is preserved, fix the test, not the code.
