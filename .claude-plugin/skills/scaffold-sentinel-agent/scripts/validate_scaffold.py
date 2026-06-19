#!/usr/bin/env python3
"""
validate_scaffold.py — preflight checks before scaffolding a new Sentinel agent.

Usage:
    python validate_scaffold.py --n 8 --name fidelis --repo-root C:/AI/Sentinal-V4

Exits 0 on success, 1 on any failure. Prints a human-readable report to stdout.

Checks:
  1. N is an integer >= 2 and not already used by an agent directory
  2. Name matches /^[a-z][a-z0-9-]+$/ (snake/kebab, lowercase, starts alpha)
  3. agents/Agent-N-<name>/ does not already exist
  4. Port 8000+N is not bound to any existing service in docker-compose.yml
     (best-effort grep; not a live socket check)
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


NAME_RE = re.compile(r"^[a-z][a-z0-9-]+$")


def check_n(n: int, repo_root: Path) -> tuple[bool, str]:
    if n < 2:
        return False, f"N={n} invalid: must be >= 2 (port 8000 is reserved for routing-db)"
    if n > 99:
        return False, f"N={n} invalid: arbitrary cap at 99 to avoid 5-digit ports"
    agents_dir = repo_root / "agents"
    if not agents_dir.is_dir():
        return False, f"{agents_dir} not found — wrong --repo-root?"
    existing = [p.name for p in agents_dir.iterdir() if p.is_dir() and p.name.startswith(f"Agent-{n}-")]
    if existing:
        return False, f"Agent number {n} already taken by {existing[0]}"
    return True, f"N={n} is free"


def check_name(name: str) -> tuple[bool, str]:
    if not NAME_RE.match(name):
        return False, f"name {name!r} invalid: must match /^[a-z][a-z0-9-]+$/"
    if name in {"shared", "tests", "scripts", "docker", "webapp"}:
        return False, f"name {name!r} clashes with a top-level directory"
    return True, f"name {name!r} ok"


def check_dir_absent(n: int, name: str, repo_root: Path) -> tuple[bool, str]:
    target = repo_root / "agents" / f"Agent-{n}-{name}"
    if target.exists():
        return False, f"{target} already exists — refusing to overwrite"
    return True, f"{target.relative_to(repo_root)} does not exist (good)"


def check_port_free(n: int, repo_root: Path) -> tuple[bool, str]:
    port = 8000 + n
    compose = repo_root / "docker" / "docker-compose.yml"
    if not compose.is_file():
        return True, f"docker-compose.yml not found — skipping port check"
    body = compose.read_text(encoding="utf-8")
    # crude but adequate: look for `"{port}:{port}"` or `- {port}:` patterns
    needle1 = f'"{port}:{port}"'
    needle2 = f"{port}:{port}"
    if needle1 in body or needle2 in body:
        return False, f"port {port} already bound in docker-compose.yml"
    return True, f"port {port} appears unused in docker-compose.yml"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, required=True, help="Agent number, e.g. 8")
    parser.add_argument("--name", required=True, help="snake/kebab agent name, e.g. fidelis")
    parser.add_argument("--repo-root", required=True, help="absolute path to Sentinel repo root")
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    checks = [
        ("N is free", check_n(args.n, repo_root)),
        ("name format", check_name(args.name)),
        ("target dir absent", check_dir_absent(args.n, args.name, repo_root)),
        ("port free", check_port_free(args.n, repo_root)),
    ]

    print("Scaffold preflight:")
    all_ok = True
    for label, (ok, msg) in checks:
        marker = "  ok " if ok else " FAIL"
        print(f"  [{marker}] {label:20s} — {msg}")
        all_ok = all_ok and ok

    if all_ok:
        print("\nAll checks passed — safe to scaffold.")
        return 0
    print("\nOne or more checks failed — refusing to scaffold.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
