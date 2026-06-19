#!/usr/bin/env python3
"""
scripts/test_snow_webhook.py
=============================
Fire a synthetic ServiceNow outbound webhook (secondary / Flow B) at Agent 1.
Useful for local testing without a real SNOW instance.

Usage:
    python scripts/test_snow_webhook.py [--priority 4] [--number INC0099999] [--url http://localhost:8001]
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import sys
import time
import uuid

import httpx


def make_signature(body: bytes, secret: str) -> str:
    sig = hmac.new(secret.encode(), body, hashlib.sha256).digest()
    return base64.b64encode(sig).decode()


def main():
    parser = argparse.ArgumentParser(description="Fire a test SNOW webhook at Agent 1")
    parser.add_argument("--url",      default=os.getenv("AGENT1_URL", "http://localhost:8001"))
    parser.add_argument("--priority", default="4", choices=["1","2","3","4","5"])
    parser.add_argument("--number",   default=f"INC{int(time.time()) % 10_000_000:07d}")
    parser.add_argument("--ci",       default="san-ctrl-02")
    parser.add_argument("--title",    default="[Test] Storage volume nearing capacity threshold")
    parser.add_argument("--secret",   default=os.getenv("SNOW_WEBHOOK_SECRET", ""))
    parser.add_argument("--dry-run",  action="store_true")
    args = parser.parse_args()

    payload = {
        "number":            args.number,
        "priority":          args.priority,
        "short_description": args.title,
        "caller_id":         "test.user@hospital.org",
        "cmdb_ci":           args.ci,
        "state":             "1",
        "category":          "Hardware",
        "subcategory":       "Storage",
        "u_service_tier":    "non-critical",
    }

    body    = json.dumps(payload).encode()
    sig     = make_signature(body, args.secret) if args.secret else "NOSIG"
    headers = {
        "Content-Type":     "application/json",
        "X-SNOW-Signature": sig,
    }

    print(f"POST {args.url}/api/webhook/servicenow")
    print(f"  INC: {args.number}  Priority: P{args.priority}  CI: {args.ci}")
    print(f"  Signature: {sig[:24]}…")

    if args.dry_run:
        print("\n[dry-run] Not sending request.")
        return

    with httpx.Client(timeout=10) as client:
        resp = client.post(f"{args.url}/api/webhook/servicenow", content=body, headers=headers)

    print(f"\nHTTP {resp.status_code}")
    try:
        data = resp.json()
        print(json.dumps(data, indent=2))
    except Exception:
        print(resp.text)

    if resp.status_code == 202 and data.get("accepted"):
        run_id = data.get("run_id")
        print(f"\n✓ Pipeline started: run_id={run_id}")
        print(f"  SSE: {args.url}/sse/run/{run_id}")
    else:
        print("\n✗ Webhook was not accepted")
        sys.exit(1)


if __name__ == "__main__":
    main()
