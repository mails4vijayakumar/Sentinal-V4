"""Backfill the previous N months of synthesised KB by calling Agent 8's admin endpoint.

This script is the first-deploy workflow: once Agent 8 is up and reachable,
run it to produce articles for the trailing N months instead of waiting for
the monthly cron to walk forward one month at a time.

Each month is dispatched as its own ``POST /jobs/synthesize`` window; the
endpoint is synchronous and runs the full extract → … → publish → retire
pipeline before returning. Long runs are expected — the client uses a 10-min
timeout per window. Set ``SYNTH_ADMIN_TOKEN`` in the environment first.

Usage::

    # Backfill the last 3 calendar months (default)
    python scripts/backfill_synthesized_kb.py --months 3

    # Backfill against a non-localhost deployment
    AGENT8_BASE_URL=https://agent8.internal \\
      SYNTH_ADMIN_TOKEN=$(vault kv get -field=token secret/agent8) \\
      python scripts/backfill_synthesized_kb.py --months 6

    # Custom window (mutually exclusive with --months)
    python scripts/backfill_synthesized_kb.py \\
      --window-start 2026-04-01 --window-end 2026-04-30
"""
from __future__ import annotations

import argparse
import asyncio
import calendar
import os
import sys
from datetime import date
from typing import Iterable

import httpx


DEFAULT_BASE_URL = os.environ.get("AGENT8_BASE_URL", "http://localhost:8008")
DEFAULT_TIMEOUT_SECONDS = 600.0  # 10 minutes per window — synthesis is slow


def previous_month_windows(n: int, today: date | None = None) -> list[tuple[date, date]]:
    """Return the last ``n`` calendar-month windows (oldest first).

    Each window is ``(first_of_month, last_of_month)``. Windows never include
    the current month — Agent 8 only synthesises closed months.
    """
    if n <= 0:
        raise ValueError("--months must be a positive integer")
    today = today or date.today()
    out: list[tuple[date, date]] = []
    y, m = today.year, today.month
    for _ in range(n):
        m -= 1
        if m == 0:
            m = 12
            y -= 1
        last_day = calendar.monthrange(y, m)[1]
        out.append((date(y, m, 1), date(y, m, last_day)))
    out.reverse()  # oldest first so re-runs deduplicate against earlier output
    return out


def _format_windows(windows: Iterable[tuple[date, date]]) -> str:
    return ", ".join(f"{a.isoformat()}..{b.isoformat()}" for a, b in windows)


async def _post_window(
    client: httpx.AsyncClient,
    base_url: str,
    token: str,
    start: date,
    end: date,
) -> dict:
    r = await client.post(
        f"{base_url.rstrip('/')}/jobs/synthesize",
        headers={
            "X-Synth-Admin-Token": token,
            "Content-Type": "application/json",
        },
        json={"window_start": start.isoformat(), "window_end": end.isoformat()},
    )
    r.raise_for_status()
    return r.json()


async def run(args: argparse.Namespace) -> int:
    token = os.environ.get("SYNTH_ADMIN_TOKEN")
    if not token:
        print("error: SYNTH_ADMIN_TOKEN environment variable is required", file=sys.stderr)
        return 2

    if args.window_start and args.window_end:
        windows = [(args.window_start, args.window_end)]
    elif args.window_start or args.window_end:
        print("error: --window-start and --window-end must be used together", file=sys.stderr)
        return 2
    else:
        windows = previous_month_windows(args.months)

    print(f"Backfilling {len(windows)} window(s) → {args.base_url}")
    print(f"  windows: {_format_windows(windows)}")

    failures = 0
    async with httpx.AsyncClient(timeout=httpx.Timeout(args.timeout)) as client:
        for start, end in windows:
            print(f"\n▶ {start.isoformat()} → {end.isoformat()}")
            try:
                body = await _post_window(client, args.base_url, token, start, end)
            except httpx.HTTPStatusError as exc:
                failures += 1
                print(f"  HTTP {exc.response.status_code}: {exc.response.text[:200]}", file=sys.stderr)
                continue
            except httpx.HTTPError as exc:
                failures += 1
                print(f"  network error: {exc!r}", file=sys.stderr)
                continue

            print(f"  status={body.get('status')!r}")
            counts = body.get("counts")
            if counts:
                print(f"  counts={counts}")

    if failures:
        print(f"\n{failures} window(s) failed", file=sys.stderr)
        return 1
    print("\nAll windows completed.")
    return 0


def _parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"not an ISO date (YYYY-MM-DD): {value!r}") from exc


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    p.add_argument(
        "--months",
        type=int,
        default=3,
        help="number of trailing calendar months to backfill (default: 3)",
    )
    p.add_argument(
        "--window-start",
        type=_parse_iso_date,
        default=None,
        help="custom window start (YYYY-MM-DD); requires --window-end and overrides --months",
    )
    p.add_argument(
        "--window-end",
        type=_parse_iso_date,
        default=None,
        help="custom window end (YYYY-MM-DD); requires --window-start and overrides --months",
    )
    p.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"Agent 8 base URL (default: {DEFAULT_BASE_URL}, also AGENT8_BASE_URL env)",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"per-window HTTP timeout in seconds (default: {DEFAULT_TIMEOUT_SECONDS})",
    )
    return p


def main() -> None:
    args = build_parser().parse_args()
    sys.exit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
