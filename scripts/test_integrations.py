#!/usr/bin/env python3
"""Connectivity smoke test for Sentinel's six external integrations.

Reads credentials from ../.env and performs one read-only call per vendor
(plus Dynatrace scope probes). Writes nothing; safe to run at any time.

Exit code: 0 if all configured integrations pass, 1 otherwise.
Placeholder/empty values are reported as SKIP (not counted as failure).
"""

from __future__ import annotations

import asyncio
import base64
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable

import httpx

try:
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

try:
    import asyncpg
except ImportError:
    asyncpg = None  # PostgreSQL probe will report SKIP with install hint


# --------------------------------------------------------------------------- #
# .env loader (no third-party dep)
# --------------------------------------------------------------------------- #

def load_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def is_placeholder(value: str) -> bool:
    if not value:
        return True
    markers = ("YOUR_", "CHANGE_ME", "YOUR_DOMAIN", "REPLACE_", "<")
    return any(m in value for m in markers)


def basic(user: str, pw: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode()


# --------------------------------------------------------------------------- #
# Test result type
# --------------------------------------------------------------------------- #

@dataclass
class Result:
    name: str
    status: str        # PASS | FAIL | SKIP
    code: int          # HTTP status, 0 if no response
    ms: float          # latency
    detail: str        # short message


async def probe(
    client: httpx.AsyncClient,
    name: str,
    method: str,
    url: str,
    headers: dict[str, str],
    parse_ok: Callable[[httpx.Response], str] | None = None,
    expect: int = 200,
) -> Result:
    t0 = time.monotonic()
    try:
        r = await client.request(method, url, headers=headers, timeout=20.0)
    except Exception as e:
        ms = (time.monotonic() - t0) * 1000
        return Result(name, "FAIL", 0, ms, f"{type(e).__name__}: {str(e)[:160]}")

    ms = (time.monotonic() - t0) * 1000
    if r.status_code == expect:
        try:
            detail = parse_ok(r) if parse_ok else "ok"
        except Exception as e:
            detail = f"parse error: {e}"
        return Result(name, "PASS", r.status_code, ms, detail)
    return Result(name, "FAIL", r.status_code, ms,
                  r.text[:160].replace("\n", " ").strip())


# --------------------------------------------------------------------------- #
# Per-vendor probes
# --------------------------------------------------------------------------- #

async def test_dynatrace(client: httpx.AsyncClient, env: dict) -> list[Result]:
    """Classic API token scopes used by Agents 1, 7 (Davis): entities/problems/events.

    Logs is no longer in this probe — it's handled by test_dynatrace_grail_logs
    against the Platform/Grail endpoint, gated by DT_LOGS_ENABLED.
    """
    token = env.get("DT_API_TOKEN", "")
    base = env.get("DT_URL", "")
    if is_placeholder(token) or is_placeholder(base):
        return [Result("dynatrace", "SKIP", 0, 0, "DT_API_TOKEN or DT_URL missing/placeholder")]

    headers = {"Authorization": f"Api-Token {token}"}
    base = base.rstrip("/")

    scopes = [
        ("dynatrace:entities.read", f"{base}/entities?entitySelector=type(HOST)&pageSize=1",
         lambda r: f'{r.json().get("totalCount", "?")} hosts'),
        ("dynatrace:problems.read", f"{base}/problems?pageSize=1",
         lambda r: f'{r.json().get("totalCount", "?")} problems'),
        ("dynatrace:events.read",   f"{base}/events?pageSize=1",
         lambda r: f'{r.json().get("totalCount", "?")} events'),
    ]
    return await asyncio.gather(*[probe(client, n, "GET", u, headers, p) for n, u, p in scopes])


async def test_dynatrace_grail_logs(client: httpx.AsyncClient, env: dict) -> Result:
    """Grail/DQL probe used by Agent 7 Flow B. Soft-skipped on trial tenants.

    When DT_LOGS_ENABLED=true, runs the OAuth2 client-credentials grant against
    DT_OAUTH_TOKEN_URL, then executes a trivial 'fetch logs | limit 1' DQL query
    against DT_PLATFORM_BASE_URL. A pass means Agent 7 Flow B's log adapter will
    work in production. A fail typically means the OAuth client is missing the
    'storage:logs:read' scope.
    """
    if env.get("DT_LOGS_ENABLED", "false").strip().lower() != "true":
        return Result(
            "dynatrace:logs (grail)", "SKIP", 0, 0,
            "DT_LOGS_ENABLED=false (set true when prod OAuth client is provisioned)",
        )

    token_url = env.get("DT_OAUTH_TOKEN_URL", "")
    client_id = env.get("DT_OAUTH_CLIENT_ID", "")
    client_secret = env.get("DT_OAUTH_CLIENT_SECRET", "")
    platform_base = env.get("DT_PLATFORM_BASE_URL", "")
    scope = env.get("DT_OAUTH_SCOPE", "storage:logs:read")

    if any(is_placeholder(v) for v in (token_url, client_id, client_secret, platform_base)):
        return Result(
            "dynatrace:logs (grail)", "FAIL", 0, 0,
            "DT_LOGS_ENABLED=true but DT_OAUTH_* / DT_PLATFORM_BASE_URL missing",
        )

    t0 = time.monotonic()
    try:
        tr = await client.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": scope,
            },
            timeout=20.0,
        )
        if tr.status_code != 200:
            ms = (time.monotonic() - t0) * 1000
            return Result(
                "dynatrace:logs (grail)", "FAIL", tr.status_code, ms,
                f"OAuth failed: {tr.text[:160]}".replace("\n", " "),
            )
        access_token = tr.json().get("access_token", "")

        qr = await client.post(
            f"{platform_base.rstrip('/')}/platform/storage/query/v1/query:execute",
            json={"query": "fetch logs | limit 1"},
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=20.0,
        )
        ms = (time.monotonic() - t0) * 1000
        if qr.status_code in (200, 202):
            state = qr.json().get("state", "?")
            return Result(
                "dynatrace:logs (grail)", "PASS", qr.status_code, ms,
                f"DQL executable (state={state})",
            )
        return Result(
            "dynatrace:logs (grail)", "FAIL", qr.status_code, ms,
            qr.text[:160].replace("\n", " "),
        )
    except Exception as e:
        ms = (time.monotonic() - t0) * 1000
        return Result(
            "dynatrace:logs (grail)", "FAIL", 0, ms,
            f"{type(e).__name__}: {str(e)[:160]}",
        )


async def test_splunk(client: httpx.AsyncClient, env: dict) -> Result:
    if env.get("SPLUNK_ENABLED", "false").strip().lower() != "true":
        return Result(
            "splunk", "SKIP", 0, 0,
            "SPLUNK_ENABLED=false (trial tier — REST API not exposed; "
            "Agent 2 falls back to DT hypothesis)",
        )
    base = env.get("SPLUNK_BASE_URL", "")
    token = env.get("SPLUNK_TOKEN", "")
    if is_placeholder(base) or is_placeholder(token):
        return Result("splunk", "FAIL", 0, 0,
                      "SPLUNK_ENABLED=true but SPLUNK_BASE_URL or SPLUNK_TOKEN missing")
    return await probe(
        client, "splunk", "GET",
        f"{base.rstrip('/')}/services/server/info?output_mode=json",
        {"Authorization": f"Bearer {token}"},
        parse_ok=lambda r: f'splunk {r.json().get("generator", {}).get("version", "?")}',
    )


async def test_servicenow(client: httpx.AsyncClient, env: dict) -> Result:
    base = env.get("SNOW_BASE_URL", "")
    user = env.get("SNOW_USERNAME", "")
    pw = env.get("SNOW_PASSWORD", "")
    if is_placeholder(base) or is_placeholder(user) or is_placeholder(pw):
        return Result("servicenow", "SKIP", 0, 0, "SNOW_BASE_URL/USERNAME/PASSWORD missing")
    return await probe(
        client, "servicenow", "GET",
        f"{base.rstrip('/')}/api/now/table/sys_user?sysparm_limit=1&sysparm_fields=sys_id",
        {"Authorization": basic(user, pw), "Accept": "application/json"},
        parse_ok=lambda r: f'returned {len(r.json().get("result", []))} user row(s)',
    )


async def test_pagerduty(client: httpx.AsyncClient, env: dict) -> Result:
    key = env.get("PD_API_KEY", "")
    if is_placeholder(key):
        return Result("pagerduty", "SKIP", 0, 0, "PD_API_KEY missing")
    return await probe(
        client, "pagerduty", "GET",
        "https://api.pagerduty.com/users?limit=1",
        {"Authorization": f"Token token={key}",
         "Accept": "application/vnd.pagerduty+json;version=2"},
        parse_ok=lambda r: f'account has {r.json().get("total") or "?"} user(s) page',
    )


async def test_confluence(client: httpx.AsyncClient, env: dict) -> Result:
    base = env.get("CONFLUENCE_BASE_URL", "")
    user = env.get("CONFLUENCE_USERNAME", "")
    token = env.get("CONFLUENCE_TOKEN", "")
    if is_placeholder(base) or is_placeholder(user) or is_placeholder(token):
        return Result("confluence", "SKIP", 0, 0, "CONFLUENCE_BASE_URL/USERNAME/TOKEN missing")
    return await probe(
        client, "confluence", "GET",
        f"{base.rstrip('/')}/rest/api/space?limit=1",
        {"Authorization": basic(user, token), "Accept": "application/json"},
        parse_ok=lambda r: f'reachable, {r.json().get("size", "?")} space(s) in page',
    )


async def test_postgres(env: dict) -> Result:
    """Verify the shared `sentinel` PostgreSQL instance is reachable and
    pgvector is installed. Connects via DATABASE_URL, runs `SELECT version()`
    and `SELECT extname FROM pg_extension WHERE extname='vector'`.

    Per CLAUDE.md section 7.3 / 8, this single instance hosts routing-db
    tables, pgvector RAG tables, Agent 6 feedback, and Agent 1 chat history,
    so any agent will fail without it.
    """
    if asyncpg is None:
        return Result("postgres", "SKIP", 0, 0,
                      "asyncpg not installed (pip install asyncpg)")
    dsn = env.get("DATABASE_URL", "")
    if is_placeholder(dsn):
        return Result("postgres", "SKIP", 0, 0, "DATABASE_URL missing")

    # SQLAlchemy DSNs prefix the driver: postgresql+asyncpg://... -> postgresql://...
    if dsn.startswith("postgresql+asyncpg://"):
        dsn = "postgresql://" + dsn[len("postgresql+asyncpg://"):]
    elif dsn.startswith("postgresql+psycopg2://"):
        dsn = "postgresql://" + dsn[len("postgresql+psycopg2://"):]

    t0 = time.monotonic()
    conn = None
    try:
        conn = await asyncpg.connect(dsn, timeout=10.0)
        version = await conn.fetchval("SELECT version()")
        pgvector = await conn.fetchval(
            "SELECT extname FROM pg_extension WHERE extname = 'vector'"
        )
        ms = (time.monotonic() - t0) * 1000
        short_ver = (version or "").split(",")[0]  # 'PostgreSQL 16.4 on x86_64...' -> 'PostgreSQL 16.4 on x86_64'
        pgv = "pgvector installed" if pgvector else "pgvector NOT installed"
        return Result("postgres", "PASS", 0, ms, f"{short_ver} | {pgv}")
    except Exception as e:
        ms = (time.monotonic() - t0) * 1000
        return Result("postgres", "FAIL", 0, ms,
                      f"{type(e).__name__}: {str(e)[:160]}")
    finally:
        if conn is not None:
            await conn.close()


async def test_outlook(client: httpx.AsyncClient, env: dict) -> Result:
    """Microsoft Graph API probe for Agent 5's Outlook email channel.

    OAuth2 client_credentials against Azure AD -> GET /v1.0/users/{from_user}.
    Verifies app registration auth, scope grant, and mailbox existence without
    sending an email. Soft-skipped on trial/dev until OUTLOOK_ENABLED=true.
    """
    if env.get("OUTLOOK_ENABLED", "false").strip().lower() != "true":
        return Result("outlook (graph)", "SKIP", 0, 0,
                      "OUTLOOK_ENABLED=false (set true once Azure AD app is provisioned)")
    tenant = env.get("OUTLOOK_TENANT_ID", "")
    cid = env.get("OUTLOOK_CLIENT_ID", "")
    cs = env.get("OUTLOOK_CLIENT_SECRET", "")
    user = env.get("OUTLOOK_FROM_USER", "")
    if any(is_placeholder(v) for v in (tenant, cid, cs, user)):
        return Result("outlook (graph)", "FAIL", 0, 0,
                      "OUTLOOK_ENABLED=true but OUTLOOK_TENANT_ID/CLIENT_ID/CLIENT_SECRET/FROM_USER missing")

    t0 = time.monotonic()
    try:
        tr = await client.post(
            f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
            data={
                "grant_type": "client_credentials",
                "client_id": cid,
                "client_secret": cs,
                "scope": "https://graph.microsoft.com/.default",
            },
            timeout=20.0,
        )
        if tr.status_code != 200:
            ms = (time.monotonic() - t0) * 1000
            return Result("outlook (graph)", "FAIL", tr.status_code, ms,
                          f"OAuth failed: {tr.text[:160]}".replace("\n", " "))
        token = tr.json().get("access_token", "")
        r = await client.get(
            f"https://graph.microsoft.com/v1.0/users/{user}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=20.0,
        )
        ms = (time.monotonic() - t0) * 1000
        if r.status_code == 200:
            upn = r.json().get("userPrincipalName", "?")
            return Result("outlook (graph)", "PASS", r.status_code, ms,
                          f"mailbox {upn} reachable")
        return Result("outlook (graph)", "FAIL", r.status_code, ms,
                      r.text[:160].replace("\n", " "))
    except Exception as e:
        ms = (time.monotonic() - t0) * 1000
        return Result("outlook (graph)", "FAIL", 0, ms,
                      f"{type(e).__name__}: {str(e)[:160]}")


async def test_teams(client: httpx.AsyncClient, env: dict) -> Result:
    """Teams incoming-webhook probe for Agent 5's Teams channel.

    Default is soft-skip (no side effects). Set TEAMS_TEST_SEND=true to fire a
    one-line connectivity message — useful pre-deploy but noisy in CI loops.
    """
    url = env.get("TEAMS_WEBHOOK_URL", "")
    if is_placeholder(url):
        return Result("teams", "SKIP", 0, 0, "TEAMS_WEBHOOK_URL missing")
    if env.get("TEAMS_TEST_SEND", "false").strip().lower() != "true":
        return Result("teams", "SKIP", 0, 0,
                      "TEAMS_TEST_SEND=false (set true to send a 'Sentinel connectivity test' message)")

    payload = {"text": f"Sentinel connectivity test - {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}"}
    t0 = time.monotonic()
    try:
        r = await client.post(url, json=payload, timeout=15.0)
        ms = (time.monotonic() - t0) * 1000
        # Legacy O365 connector: body == "1", status 200.
        # Power Automate Workflows: status 200 with empty/JSON body.
        # Both are PASS.
        if r.status_code == 200:
            return Result("teams", "PASS", r.status_code, ms,
                          f'message delivered (body[:30]={r.text[:30]!r})')
        return Result("teams", "FAIL", r.status_code, ms,
                      r.text[:160].replace("\n", " "))
    except Exception as e:
        ms = (time.monotonic() - t0) * 1000
        return Result("teams", "FAIL", 0, ms,
                      f"{type(e).__name__}: {str(e)[:160]}")


async def test_gitlab(client: httpx.AsyncClient, env: dict) -> Result:
    base = env.get("GITLAB_BASE_URL", "")
    token = env.get("GITLAB_API_TOKEN", "")
    if is_placeholder(base) or is_placeholder(token):
        return Result("gitlab", "SKIP", 0, 0, "GITLAB_BASE_URL or GITLAB_API_TOKEN is placeholder")
    return await probe(
        client, "gitlab", "GET",
        f"{base.rstrip('/')}/api/v4/version",
        {"PRIVATE-TOKEN": token},
        parse_ok=lambda r: f'gitlab {r.json().get("version", "?")}',
    )


# --------------------------------------------------------------------------- #
# Driver
# --------------------------------------------------------------------------- #

async def run_all(env: dict) -> list[Result]:
    async with httpx.AsyncClient(verify=True, follow_redirects=False) as client:
        dt_results, dt_logs, pg, others = await asyncio.gather(
            test_dynatrace(client, env),
            test_dynatrace_grail_logs(client, env),
            test_postgres(env),
            asyncio.gather(
                test_splunk(client, env),
                test_servicenow(client, env),
                test_pagerduty(client, env),
                test_confluence(client, env),
                test_outlook(client, env),
                test_teams(client, env),
                test_gitlab(client, env),
            ),
        )
        return [pg] + list(dt_results) + [dt_logs] + list(others)


def render(results: list[Result]) -> None:
    name_w = max(len(r.name) for r in results)
    bar = "-" * (name_w + 38)
    print(bar)
    print(f"{'INTEGRATION'.ljust(name_w)}  STATUS  HTTP   MS   DETAIL")
    print(bar)
    for r in results:
        ms = f"{r.ms:>4.0f}" if r.ms else "  - "
        code = f"{r.code:>3}" if r.code else " - "
        print(f"{r.name.ljust(name_w)}  {r.status:<6}  {code}  {ms}   {r.detail}")
    print(bar)

    passes = sum(1 for r in results if r.status == "PASS")
    fails = sum(1 for r in results if r.status == "FAIL")
    skips = sum(1 for r in results if r.status == "SKIP")
    print(f"Summary: {passes} PASS / {fails} FAIL / {skips} SKIP\n")


def main() -> int:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.is_file():
        print(f"ERROR: .env not found at {env_path}", file=sys.stderr)
        return 2

    env = load_env(env_path)
    results = asyncio.run(run_all(env))
    render(results)
    return 0 if all(r.status != "FAIL" for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
