#!/usr/bin/env python3
"""
Fetch MAA portal data and ingest into the database.

Usage:
    python fetch.py [--dry-run] [--fresh] [--browser chromium|firefox|webkit] MONTH [MONTH ...]

MONTH format: MAY_2026, APR_2026, MAR_2026, etc.

Session acquisition:
  1. Tries existing browser session via browser-cookie3 (no login needed).
  2. Falls back to a headed Playwright browser — fill the CAPTCHA, then
     the script takes over automatically after login completes.

Cache:
  Fetched CSVs are written to cache/{MONTH}.csv.
  Subsequent runs reuse the cache unless --fresh is passed.
  --dry-run fetches and caches without writing to the database.
"""

import argparse
import getpass
import re
import ssl
import sys
import warnings
from pathlib import Path
from xml.etree import ElementTree

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

import db
import ingest


class _LegacySSLAdapter(HTTPAdapter):
    """Allow legacy TLS renegotiation required by some older government servers."""
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.options |= getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0x4)
        kwargs["ssl_context"] = ctx
        super().init_poolmanager(*args, **kwargs)


def _new_session() -> requests.Session:
    session = requests.Session()
    session.mount("https://", _LegacySSLAdapter())
    return session

PORTAL_URL = "https://chiranjeeviapp.rajasthan.gov.in/webapp/page/core/genericSearch.jsf"
HOSPITAL_CODE = "Jaip880"
HOSPITAL_NAME = "Jaipur1"
CACHE_DIR = Path(__file__).parent / "cache"

CASE_STATUSES = [
    "claimapprovalpending",
    "claimApprovalPendingAtAnalyser",
    "claimPackageApproved",
    "claimPackageApprovedandPaid",
    "claimPackageQuery",
    "claimPackageRejected",
    "claimQueryByAnalyser",
    "claimRejectedByAnalyser",
    "fundEnhancementApprovalPending",
    "fundEnhancementPackageQuery",
    "fundEnhancementPackageRejected",
    "preAuthApprovalPending",
    "preAuthApproved",
    "preAuthPackageQuery",
    "preAuthPackageRejected",
    "queryclaimreplybyhospital",
    "queryFundreplybyhospital",
    "queryreplytodecesion",
    "revertBackToAnalyserByClaim",
]

COLUMN_INDICES = list(range(36))


# ── Session helpers ───────────────────────────────────────────────────────────

def _test_session(session: requests.Session) -> bool:
    """Return True if the session can reach the portal page (not redirected to SSO)."""
    try:
        r = session.get(PORTAL_URL, timeout=15, allow_redirects=True)
        return "genericSearch" in r.url and r.status_code == 200
    except Exception:
        return False


def _try_browser_cookies(browser_name: str) -> requests.Session | None:
    """Attempt to load an existing session cookie from an installed browser.

    browser_cookie3's domain_name filter can miss httponly/secure cookies on
    some platforms, so we load all cookies and let the session test determine
    whether a valid auth session exists.
    """
    try:
        import browser_cookie3
        loaders = {
            "chrome": browser_cookie3.chrome,
            "chromium": browser_cookie3.chromium,
            "firefox": browser_cookie3.firefox,
            "safari": browser_cookie3.safari,
        }
        # Map browser_name choice to corresponding browser_cookie3 keys
        name_map = {
            "chromium": ["chrome", "chromium"],
            "firefox": ["firefox"],
            "webkit": ["safari"],
        }
        primary = name_map.get(browser_name, [])
        ordered_names = primary + [name for name in loaders if name not in primary]

        for name in ordered_names:
            if name not in loaders:
                continue
            loader = loaders[name]
            try:
                cj = loader()  # no domain filter — avoids missed httponly cookies
                session = _new_session()
                session.cookies.update(cj)
                if _test_session(session):
                    print(f"  Reusing existing {name} session (no login needed).")
                    return session
            except Exception:
                continue
    except ImportError:
        pass
    return None


def _playwright_login(ssoid: str, password: str, browser_name: str) -> requests.Session:
    """Open a headed browser, pre-fill credentials, wait for user to complete CAPTCHA."""
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        raise RuntimeError(
            "Playwright is not installed. Run: pip install playwright && playwright install chromium"
        )

    print("Opening browser for login — fill the CAPTCHA and complete login.")
    print("The script will take over automatically once you are logged in.")

    with sync_playwright() as pw:
        launcher = getattr(pw, browser_name)
        browser = launcher.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        page.goto(PORTAL_URL)

        # Pre-fill credentials when the SSO login form appears
        try:
            page.wait_for_selector("input[name='ssoid'], input#ssoid", timeout=10_000)
            page.fill("input[name='ssoid'], input#ssoid", ssoid)
            # Try common password field names
            for sel in ["input[name='password']", "input[type='password']", "input#password"]:
                try:
                    page.fill(sel, password)
                    break
                except Exception:
                    pass
        except PWTimeout:
            pass  # Login form may look different; user fills manually

        # Wait until redirected back to the portal
        try:
            page.wait_for_url(
                re.compile(r"chiranjeeviapp\.rajasthan\.gov\.in"),
                timeout=300_000,  # 5 minutes for human to complete CAPTCHA
            )
        except PWTimeout:
            browser.close()
            raise TimeoutError(
                "Login timed out (5 min). Re-run the command and complete the CAPTCHA promptly."
            )

        cookies = context.cookies()
        browser.close()

    session = _new_session()
    for c in cookies:
        session.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))
    return session


def acquire_session(ssoid: str, password: str, browser: str = "chromium") -> requests.Session:
    """Return an authenticated requests.Session for the MAA portal."""
    session = _try_browser_cookies(browser)
    if session:
        return session
    print("No valid browser session found — launching browser for login.")
    return _playwright_login(ssoid, password, browser)


# ── Form / ViewState helpers ──────────────────────────────────────────────────

def _get_viewstate(html_or_xml: str) -> str:
    """Extract javax.faces.ViewState from full HTML or JSF AJAX XML response."""
    # AJAX partial response: <update id="j_id1:javax.faces.ViewState:0"><![CDATA[VALUE]]></update>
    if html_or_xml.lstrip().startswith("<?xml") or "<partial-response" in html_or_xml:
        try:
            root = ElementTree.fromstring(html_or_xml)
            for update in root.iter("update"):
                if "ViewState" in (update.get("id") or ""):
                    return update.text.strip()
        except ElementTree.ParseError:
            pass

    # Full page HTML: <input name="javax.faces.ViewState" value="..." />
    soup = BeautifulSoup(html_or_xml, "html.parser")
    el = soup.find("input", {"name": "javax.faces.ViewState"})
    if el:
        return el.get("value", "")

    raise ValueError("Could not find javax.faces.ViewState in response.")


def _build_post_body(page_html: str, overrides: dict) -> list[tuple]:
    """
    Discover all form fields from the live page HTML, then apply overrides.
    Multi-value fields (lists in overrides) are expanded to repeated tuples.
    This approach is resilient to JSF hash changes in hidden field names.
    """
    soup = BeautifulSoup(page_html, "html.parser")
    form = soup.find("form", id="genericSearchForm")
    if not form:
        raise ValueError("Could not find genericSearchForm in page HTML.")

    fields = []
    for el in form.find_all(["input", "select", "textarea"]):
        name = el.get("name")
        if not name:
            continue
        if el.name == "select":
            selected = el.find("option", selected=True)
            fields.append((name, selected["value"] if selected else ""))
        else:
            fields.append((name, el.get("value", "")))

    override_keys = set(overrides.keys())
    base = [(k, v) for k, v in fields if k not in override_keys]

    result = list(base)
    for k, v in overrides.items():
        if isinstance(v, list):
            result.extend((k, item) for item in v)
        else:
            result.append((k, v))
    return result


# ── Per-month fetch ───────────────────────────────────────────────────────────

def fetch_month(session: requests.Session, month: str) -> bytes:
    """
    Fetch one month's CSV from the portal.
    month: "MAY_2026", "APR_2026", etc.
    Returns raw CSV bytes.
    """
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": PORTAL_URL,
        "Origin": "https://chiranjeeviapp.rajasthan.gov.in",
    }

    # Step 1: GET the page — extract form fields + initial ViewState
    r = session.get(PORTAL_URL, headers=headers, timeout=30)
    if r.status_code != 200 or "genericSearch" not in r.url:
        raise RuntimeError(f"Session expired or redirected for month {month}. Re-run to re-authenticate.")
    page_html = r.text

    # Step 2: Build search POST body
    month_field = next(
        (k for k in [
            "genericSearchForm:seletedMonth_input",
            "genericSearchForm:selectedMonth_input",
        ] if k in page_html),
        "genericSearchForm:seletedMonth_input",
    )

    search_overrides = {
        "javax.faces.partial.ajax": "true",
        "javax.faces.source": "genericSearchForm:searchButton",
        "javax.faces.partial.execute": "@all",
        "javax.faces.partial.render": "genericSearchForm",
        month_field: month,
        "genericSearchForm:hospitalCode": HOSPITAL_CODE,
        "genericSearchForm:paymentType_input": "99",
        "genericSearchForm:caseStatus": CASE_STATUSES,
        "genericSearchForm:seletedColumnsForDownload": [str(i) for i in COLUMN_INDICES],
        "genericSearchForm_SUBMIT": "1",
        "javax.faces.ViewState": _get_viewstate(page_html),
    }
    search_body = _build_post_body(page_html, search_overrides)

    r2 = session.post(
        PORTAL_URL,
        data=search_body,
        headers={**headers, "Faces-Request": "partial/ajax", "X-Requested-With": "XMLHttpRequest"},
        timeout=60,
    )
    r2.raise_for_status()

    # Extract updated ViewState from the AJAX response (required for download)
    search_viewstate = _get_viewstate(r2.text)

    # Step 3: Build download POST body (no AJAX headers, add download button)
    download_overrides = {
        month_field: month,
        "genericSearchForm:hospitalCode": HOSPITAL_CODE,
        "genericSearchForm:paymentType_input": "99",
        "genericSearchForm:caseStatus": CASE_STATUSES,
        "genericSearchForm:seletedColumnsForDownload": [str(i) for i in COLUMN_INDICES],
        "genericSearchForm:genericSearchTable:downloadbtn": "genericSearchForm:genericSearchTable:downloadbtn",
        "genericSearchForm_SUBMIT": "1",
        "javax.faces.ViewState": search_viewstate,
    }
    download_body = _build_post_body(page_html, download_overrides)

    r3 = session.post(PORTAL_URL, data=download_body, headers=headers, timeout=120)
    r3.raise_for_status()

    if not r3.content:
        raise RuntimeError(f"Empty response for month {month} — no data returned.")

    # Server sends CSV despite Excel MIME type
    return r3.content


# ── Orchestration ─────────────────────────────────────────────────────────────

def fetch_and_ingest(
    months: list[str],
    ssoid: str = "",
    password: str = "",
    browser: str = "chromium",
    dry_run: bool = False,
    fresh: bool = False,
) -> dict[str, tuple[int, int, int]]:
    """
    Fetch and optionally ingest data for the given months.
    Returns {month: (new, updated, unchanged)}.
    Credentials are optional when all months are available in cache.
    """
    CACHE_DIR.mkdir(exist_ok=True)

    # Determine which months need fetching
    needs_fetch = []
    for m in months:
        cache_file = CACHE_DIR / f"{m}.csv"
        if fresh or not cache_file.exists():
            needs_fetch.append(m)

    session = None
    if needs_fetch:
        session = acquire_session(ssoid, password, browser)

    conn = db.init_db()
    results = {}

    for month in months:
        cache_file = CACHE_DIR / f"{month}.csv"

        # Load from cache or fetch fresh
        if not fresh and cache_file.exists():
            print(f"  [{month}] Using cache: {cache_file}")
            csv_bytes = cache_file.read_bytes()
        else:
            print(f"  [{month}] Fetching from portal...", flush=True)
            try:
                csv_bytes = fetch_month(session, month)
            except RuntimeError as e:
                if "expired" in str(e).lower():
                    print(f"  [{month}] Session expired mid-fetch. Re-run to re-authenticate.")
                    raise
                raise
            cache_file.write_bytes(csv_bytes)
            print(f"  [{month}] Cached to {cache_file}")

        # Decode (server may return UTF-8 or UTF-8-BOM)
        csv_str = csv_bytes.decode("utf-8-sig")
        rows = ingest.parse_csv_content(csv_str)

        if not rows:
            warnings.warn(f"Month {month} returned 0 data rows.")
            results[month] = (0, 0, 0)
            continue

        new, updated, unchanged = db.upsert_claims(conn, rows, dry_run=dry_run)
        results[month] = (new, updated, unchanged)

        tag = "[DRY RUN] " if dry_run else ""
        print(f"  [{month}] {tag}{new} new, {updated} updated, {unchanged} unchanged")

    if conn:
        conn.close()

    return results


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("months", nargs="+", help="Month(s) to fetch, e.g. MAY_2026 APR_2026")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and cache CSVs without writing to the database")
    parser.add_argument("--fresh", action="store_true",
                        help="Ignore cache and re-fetch from the portal")
    parser.add_argument("--browser", default="chromium",
                        choices=["chromium", "firefox", "webkit"],
                        help="Browser to use for Playwright login (default: chromium)")
    parser.add_argument("--ssoid", default="",
                        help="SSOID username (prompted if omitted and login is needed)")
    args = parser.parse_args()

    # Determine whether any month needs fetching (not covered by cache)
    needs_fetch = args.fresh or any(
        not (CACHE_DIR / f"{m}.csv").exists() for m in args.months
    )

    ssoid = args.ssoid
    password = ""
    if needs_fetch:
        # Try existing browser session first — avoids credential prompt entirely
        if not _try_browser_cookies(args.browser):
            if not ssoid:
                ssoid = input("SSOID: ").strip()
            password = getpass.getpass("Password: ")

    try:
        results = fetch_and_ingest(
            months=args.months,
            ssoid=ssoid,
            password=password,
            browser=args.browser,
            dry_run=args.dry_run,
            fresh=args.fresh,
        )
    except TimeoutError as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)

    print()
    print(f"{'Month':<12} {'New':>6} {'Updated':>8} {'Unchanged':>10}")
    print("-" * 40)
    for month, (new, updated, unchanged) in results.items():
        print(f"{month:<12} {new:>6} {updated:>8} {unchanged:>10}")

    if args.dry_run:
        print("\nDry run — database was NOT modified.")


if __name__ == "__main__":
    main()
