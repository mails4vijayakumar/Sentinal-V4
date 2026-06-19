"""
Test all external MCP integrations configured in .env.mcp.

Run from repo root:
    python scripts/test_mcp_connections.py

Each integration runs a lightweight, read-only API call. The script prints
a colored summary (PASS / FAIL / SKIP) with latency and a short diagnostic
on failure. Exit code is 0 if every non-skipped check passes, else 1.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Importing shared.http_client triggers system trust-store injection so
# Confluence (and any other corp-proxied endpoint) verifies correctly.
import shared.http_client  # noqa: F401  -- side-effect import
import httpx
from dotenv import load_dotenv

ENV_FILE = ROOT / ".env.mcp"
ENV_LOCAL = ROOT / ".env"

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
DIM = "\033[2m"
RESET = "\033[0m"

PLACEHOLDER_MARKERS = ("YOUR_", "CHANGE_ME", "<", "placeholder")
TIMEOUT = 15.0


def is_placeholder(v: str | None) -> bool:
    if not v:
        return True
    return any(m in v for m in PLACEHOLDER_MARKERS)


def fmt_ms(seconds: float) -> str:
    return f"{int(seconds * 1000)} ms"


class Result:
    def __init__(self, name: str, status: str, detail: str, latency: float = 0.0):
        self.name = name
        self.status = status  # PASS | FAIL | SKIP
        self.detail = detail
        self.latency = latency

    def render(self) -> str:
        color = {"PASS": GREEN, "FAIL": RED, "SKIP": YELLOW}[self.status]
        lat = f" ({fmt_ms(self.latency)})" if self.status == "PASS" else ""
        return f"  {color}[{self.status}]{RESET} {self.name:<14}{lat}  {DIM}{self.detail}{RESET}"


def run_check(name: str, fn: Callable[[], tuple[str, str]]) -> Result:
    t0 = time.perf_counter()
    try:
        status, detail = fn()
        return Result(name, status, detail, time.perf_counter() - t0)
    except httpx.HTTPStatusError as e:
        return Result(name, "FAIL", f"HTTP {e.response.status_code}: {e.response.text[:120]}")
    except httpx.RequestError as e:
        return Result(name, "FAIL", f"{type(e).__name__}: {e}")
    except Exception as e:  # last-resort catch so one bad check can't kill the rest
        return Result(name, "FAIL", f"{type(e).__name__}: {e}")


def check_servicenow() -> tuple[str, str]:
    base = os.environ.get("SERVICENOW_INSTANCE")
    user = os.environ.get("SERVICENOW_USERNAME")
    pwd = os.environ.get("SERVICENOW_PASSWORD")
    if is_placeholder(base) or is_placeholder(user) or is_placeholder(pwd):
        return "SKIP", "credentials not configured"
    r = httpx.get(
        f"{base}/api/now/table/incident",
        params={"sysparm_limit": 1, "sysparm_fields": "number"},
        auth=(user, pwd),
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    body = r.json()
    count = len(body.get("result", []))
    return "PASS", f"reachable, {count} record returned"


def check_dynatrace() -> tuple[str, str]:
    url = os.environ.get("DYNATRACE_URL")
    token = os.environ.get("DYNATRACE_API_TOKEN")
    if is_placeholder(url) or is_placeholder(token):
        return "SKIP", "credentials not configured"
    r = httpx.get(
        f"{url.rstrip('/')}/entities",
        params={"entitySelector": 'type("HOST")', "pageSize": 1},
        headers={"Authorization": f"Api-Token {token}"},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    body = r.json()
    total = body.get("totalCount", 0)
    return "PASS", f"reachable, totalCount={total}"


def check_splunk() -> tuple[str, str]:
    host = os.environ.get("SPLUNK_HOST")
    user = os.environ.get("SPLUNK_USERNAME")
    pwd = os.environ.get("SPLUNK_PASSWORD")
    port = os.environ.get("SPLUNK_PORT", "8089")
    if is_placeholder(host) or is_placeholder(user) or is_placeholder(pwd):
        return "SKIP", "credentials not configured"
    base = host if ":" in host.split("//", 1)[-1] else f"{host}:{port}"
    r = httpx.get(
        f"{base}/services/server/info",
        params={"output_mode": "json"},
        auth=(user, pwd),
        timeout=TIMEOUT,
        verify=False,  # Splunk self-signed certs are common in dev
    )
    r.raise_for_status()
    body = r.json()
    version = body.get("entry", [{}])[0].get("content", {}).get("version", "?")
    return "PASS", f"reachable, splunkd {version}"


def check_confluence() -> tuple[str, str]:
    base = os.environ.get("CONFLUENCE_URL")
    user = os.environ.get("CONFLUENCE_USERNAME")
    token = os.environ.get("CONFLUENCE_API_TOKEN")
    if is_placeholder(base) or is_placeholder(user) or is_placeholder(token):
        return "SKIP", "credentials not configured"
    r = httpx.get(
        f"{base.rstrip('/')}/rest/api/space",
        params={"limit": 1},
        auth=(user, token),  # Atlassian Cloud requires Basic auth, not Bearer
        headers={"Accept": "application/json"},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    body = r.json()
    size = body.get("size", 0)
    return "PASS", f"reachable, {size} space returned (sample)"


def check_gitlab() -> tuple[str, str]:
    url = os.environ.get("GITLAB_URL")
    token = os.environ.get("GITLAB_TOKEN")
    if is_placeholder(url) or is_placeholder(token):
        return "SKIP", "credentials not configured"
    r = httpx.get(
        f"{url.rstrip('/')}/api/v4/user",
        headers={"PRIVATE-TOKEN": token},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    body = r.json()
    return "PASS", f"authenticated as {body.get('username', '?')}"


def check_pagerduty() -> tuple[str, str]:
    token = os.environ.get("PAGERDUTY_API_TOKEN")
    from_email = os.environ.get("PD_FROM_EMAIL")
    if is_placeholder(token):
        return "SKIP", "credentials not configured"
    headers = {
        "Authorization": f"Token token={token}",
        "Accept": "application/vnd.pagerduty+json;version=2",
    }
    if from_email and not is_placeholder(from_email):
        headers["From"] = from_email
    r = httpx.get(
        "https://api.pagerduty.com/users",
        params={"limit": 1},
        headers=headers,
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    body = r.json()
    total = body.get("total")
    return "PASS", f"reachable, total users={total}"


def check_postgres() -> tuple[str, str]:
    """
    Verify the Sentinel-V4 Postgres setup:
      1. Login + reach
      2. pgvector extension created
      3. Write privilege on the public schema (CREATE TABLE ... DROP)
    All three must pass before agents can use the DB. Any one missing → FAIL
    with a one-line hint at which SQL grant is needed.
    """
    url = os.environ.get("DATABASE_URL_SYNC") or os.environ.get("DATABASE_URL", "")
    if not url:
        return "SKIP", "DATABASE_URL not configured"
    if is_placeholder(url) or "CHANGE_ME" in url:
        return "SKIP", "password placeholder not set"
    # psycopg2 doesn't understand the +asyncpg driver tag in the URL
    sync_url = url.replace("+asyncpg", "")

    try:
        import psycopg2
    except ImportError:
        return "FAIL", "psycopg2 not installed (pip install psycopg2-binary)"

    try:
        conn = psycopg2.connect(sync_url, connect_timeout=5)
    except psycopg2.OperationalError as e:
        return "FAIL", f"connect: {str(e).strip().splitlines()[0]}"

    issues: list[str] = []
    try:
        cur = conn.cursor()
        cur.execute("SELECT current_database()")
        db = cur.fetchone()[0]

        cur.execute("SELECT extname FROM pg_extension WHERE extname='vector'")
        if not cur.fetchone():
            issues.append("pgvector not created (CREATE EXTENSION vector)")

        try:
            cur.execute("CREATE TEMP TABLE _sentinel_conntest(id int)")
            cur.execute("DROP TABLE _sentinel_conntest")
            # Also probe permanent-schema write since TEMP works for anyone
            cur.execute("CREATE TABLE _sentinel_perm_test(id int)")
            cur.execute("DROP TABLE _sentinel_perm_test")
            conn.commit()
        except psycopg2.errors.InsufficientPrivilege:
            conn.rollback()
            issues.append("no CREATE on public schema (GRANT ALL ON SCHEMA public)")
        except Exception as e:
            conn.rollback()
            issues.append(f"write probe: {type(e).__name__}")

        cur.close()
    finally:
        conn.close()

    if issues:
        return "FAIL", f"db={db}; " + "; ".join(issues)
    return "PASS", f"reachable, db={db}, pgvector ok, writable"


CHECKS: list[tuple[str, Callable[[], tuple[str, str]]]] = [
    ("Postgres", check_postgres),
    ("ServiceNow", check_servicenow),
    ("Dynatrace", check_dynatrace),
    ("Splunk", check_splunk),
    ("Confluence", check_confluence),
    ("GitLab", check_gitlab),
    ("PagerDuty", check_pagerduty),
]


def main() -> int:
    if not ENV_FILE.exists():
        print(f"{RED}ERROR{RESET}  {ENV_FILE} not found")
        return 1
    # .env.mcp wins on conflict (loaded first, override=False on later loads)
    load_dotenv(ENV_FILE, override=False)
    if ENV_LOCAL.exists():
        load_dotenv(ENV_LOCAL, override=False)

    print(f"{CYAN}MCP connection tests{RESET}  {DIM}({ENV_FILE.name}){RESET}\n")

    results = [run_check(name, fn) for name, fn in CHECKS]
    for r in results:
        print(r.render())

    passed = sum(1 for r in results if r.status == "PASS")
    failed = sum(1 for r in results if r.status == "FAIL")
    skipped = sum(1 for r in results if r.status == "SKIP")
    print(f"\n{CYAN}Summary{RESET}  {GREEN}{passed} pass{RESET}  {RED}{failed} fail{RESET}  {YELLOW}{skipped} skip{RESET}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
