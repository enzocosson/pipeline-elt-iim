#!/usr/bin/env python3
"""Run the full pipeline + API and print refresh timings.

Steps:
 - If FastAPI is not reachable on localhost:8000, start it with uvicorn in background
 - Run the flow that ingests Gold -> MongoDB
 - Query the API for metadata for each collection and print human-readable deltas

Usage:
    python3 run_all.py

The script assumes you run it inside the project's virtualenv (or have requirements installed),
and that `.env` (with `MONGODB_URI` / `API_URL`) exists in repo root if needed.
"""
import os
import sys
import time
import subprocess
import argparse
from datetime import datetime, timedelta, timezone

import httpx
from dotenv import load_dotenv


def human_seconds(seconds: float) -> str:
    if seconds is None:
        return "N/A"
    try:
        secs = int(round(seconds))
    except Exception:
        return str(seconds)
    return str(timedelta(seconds=secs))


def api_up(api_url: str) -> bool:
    try:
        with httpx.Client(timeout=2) as client:
            r = client.get(f"{api_url}/health")
            return r.status_code == 200
    except Exception:
        return False


def start_uvicorn(python_exe: str, api_module: str = "app.api:app", host: str = "0.0.0.0", port: int = 8000):
    log = open("uvicorn.run_all.log", "a")
    cmd = [python_exe, "-m", "uvicorn", api_module, "--host", host, "--port", str(port)]
    print(f"Starting uvicorn: {' '.join(cmd)} (logs -> uvicorn.run_all.log)")
    proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=os.environ)
    return proc


def wait_for_api(api_url: str, timeout: int = 30):
    print(f"Waiting for API {api_url} to become ready (timeout {timeout}s)...")
    start = time.time()
    while time.time() - start < timeout:
        if api_up(api_url):
            print("API is up")
            return True
        time.sleep(1)
    print("Timed out waiting for API")
    return False


def run_flow(python_exe: str):
    cmd = [python_exe, "-m", "flows.gold_to_mongo"]
    print(f"Running flow: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def fetch_and_print_metadata(api_url: str):
    print("Fetching collections from API...")
    with httpx.Client(timeout=10) as client:
        r = client.get(f"{api_url}/collections")
        r.raise_for_status()
        cols = r.json()

        print(f"Found collections: {cols}")

        for c in cols:
            # skip internal
            if c == "ingest_metadata":
                continue
            try:
                mr = client.get(f"{api_url}/metadata/{c}")
                if mr.status_code != 200:
                    print(f"- {c}: metadata not found (status {mr.status_code})")
                    continue
                meta = mr.json()
                src = meta.get("source_last_modified")
                ing = meta.get("ingest_time")
                ds = meta.get("delta_source_to_ingest_seconds")
                di = meta.get("delta_ingest_to_now_seconds")
                print(f"\nCollection: {c}")
                print(f"  source_last_modified: {src}")
                print(f"  ingest_time:          {ing}")
                print(f"  source->ingest:       {human_seconds(ds)} ({ds} s)")
                print(f"  ingest->now:          {human_seconds(di)} ({di} s)")
            except Exception as e:
                print(f"- error fetching metadata for {c}: {e}")


def main():
    load_dotenv()
    api_url = os.getenv("API_URL", "http://localhost:8000")

    python_exe = sys.executable

    server_proc = None
    parser = argparse.ArgumentParser(description="Run API + pipeline and show timings")
    parser.add_argument("--keep-server", action="store_true", help="Don't stop uvicorn started by this script")
    parser.add_argument("--timeout", type=int, default=30, help="Timeout waiting for API (seconds)")
    args = parser.parse_args()

    api_started_by_script = False
    api_start_time = None

    if api_up(api_url):
        print(f"API already running at {api_url}, will reuse it.")
        # consider the 'launch' time as now when reusing a running API
        api_start_time = datetime.now(timezone.utc)
    else:
        server_proc = start_uvicorn(python_exe)
        api_started_by_script = True
        ok = wait_for_api(api_url, timeout=args.timeout)
        if not ok:
            print("API did not start. Exiting.")
            if server_proc:
                server_proc.terminate()
            sys.exit(1)
        api_start_time = datetime.now(timezone.utc)

    flow_start = datetime.now(timezone.utc)
    try:
        run_flow(python_exe)
    except subprocess.CalledProcessError as e:
        print(f"Flow failed: {e}")
        if api_started_by_script and server_proc:
            server_proc.terminate()
        sys.exit(1)
    flow_end = datetime.now(timezone.utc)

    # compute delta between API launch and flow end
    if api_start_time:
        delta = flow_end - api_start_time
        print(f"\nTiming: API launch -> flow end = {delta} ({delta.total_seconds():.3f} s)")

    # give API a short moment to reflect new metadata
    time.sleep(1)
    fetch_and_print_metadata(api_url)

    # stop uvicorn if we started it and user did not request to keep it
    if api_started_by_script and server_proc:
        if args.keep_server:
            print("Left uvicorn running (requested by --keep-server).\n")
        else:
            print("Stopping uvicorn started by this script...\n")
            server_proc.terminate()

    print("All done.\n")


if __name__ == "__main__":
    main()
