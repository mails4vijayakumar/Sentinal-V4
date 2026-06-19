#!/usr/bin/env python3
"""
scripts/redis_server.py
========================
Local Redis dev launcher — starts a temporary Redis server on port 6379.
Useful when Docker isn't available but you want to test agents locally.

Usage:
    python scripts/redis_server.py
"""
import os, signal, subprocess, sys, time, logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

PORT = int(os.getenv("REDIS_PORT", "6379"))

def main():
    try:
        proc = subprocess.Popen(
            ["redis-server", "--port", str(PORT), "--loglevel", "notice",
             "--save", "", "--appendonly", "no"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        )
    except FileNotFoundError:
        log.error("redis-server not found. Install with: apt install redis-server  OR  brew install redis")
        sys.exit(1)

    log.info("Redis started on port %d (pid=%d)", PORT, proc.pid)

    def _stop(signum, _):
        log.info("Stopping Redis…")
        proc.terminate()
        proc.wait(timeout=5)
        sys.exit(0)

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    while True:
        line = proc.stdout.readline()
        if not line: break
        print(line.decode().strip())

if __name__ == "__main__":
    main()
