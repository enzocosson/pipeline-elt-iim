#!/usr/bin/env python3
"""Configure Metabase: create admin (if needed) and add MongoDB datasource.

Reads env vars:
 - METABASE_URL (default http://localhost:3000)
 - METABASE_ADMIN_EMAIL, METABASE_ADMIN_PASSWORD (used or created)
 - MONGODB_URI, MONGODB_DB

Usage: set -a; [ -f .env ] && source .env; set +a; python3 scripts/setup_metabase.py
"""
import os
import time
import httpx
from urllib.parse import urljoin


METABASE_URL = os.getenv("METABASE_URL", "http://localhost:3000")
ADMIN_EMAIL = os.getenv("METABASE_ADMIN_EMAIL", "admin@local")
ADMIN_PASSWORD = os.getenv("METABASE_ADMIN_PASSWORD", "change_me")
MONGODB_URI = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI")
MONGODB_DB = os.getenv("MONGODB_DB") or os.getenv("MONGODB_DB") or os.getenv("MONGODB_DB") or os.getenv("MONGODB_DB")


def wait_for_metabase(timeout=120):
    print(f"Waiting for Metabase at {METABASE_URL}...")
    start = time.time()
    health = urljoin(METABASE_URL, "/api/health")
    while time.time() - start < timeout:
        try:
            r = httpx.get(health, timeout=5)
            if r.status_code == 200:
                print("Metabase is up")
                return True
        except Exception:
            pass
        time.sleep(2)
    print("Timed out waiting for Metabase")
    return False


def setup_admin_and_db():
    setup_url = urljoin(METABASE_URL, "/api/setup")
    session_url = urljoin(METABASE_URL, "/api/session")
    db_url = urljoin(METABASE_URL, "/api/database")

    print(f"Attempting Metabase actions against {METABASE_URL}")
    print(f"Using admin email: {ADMIN_EMAIL} (password hidden)")

    # Try to login first
    with httpx.Client() as client:
        try:
            r = client.post(session_url, json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD}, timeout=10)
            if r.status_code == 200:
                print("Logged into Metabase as existing admin")
                token = r.json().get("id")
                session = client
                session.headers.update({"X-Metabase-Session": token})
                return session
        except Exception:
            pass

        # If login failed, try setup
        payload = {
            "prefs": {},
            "database": None,
            "user": {
                "first_name": "Admin",
                "last_name": "User",
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD
            }
        }

        try:
            r = client.post(setup_url, json=payload, timeout=20)
            if r.status_code in (200, 201):
                print("Metabase initial setup completed (admin created)")
                # login to get session
                r2 = client.post(session_url, json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD}, timeout=10)
                if r2.status_code == 200:
                    token = r2.json().get("id")
                    client.headers.update({"X-Metabase-Session": token})
                    return client
                else:
                    print("Setup succeeded but login failed")
                    return None
            else:
                print(f"Setup API returned status {r.status_code}, response: {r.text}")
                # If setup API refuses because token missing or similar, inform user and poll for manual admin creation
                try:
                    body = r.json()
                except Exception:
                    body = {}
                if r.status_code == 400 and ("Token does not match" in r.text or body.get("errors")):
                    print("It looks like Metabase requires interactive setup (setup token) or is already configured.")
                    print("Please open Metabase UI at", METABASE_URL, "and finish the initial setup (create admin user).")
                    # Provide more specific guidance based on validation errors
                    errs = body.get("errors") or {}
                    if isinstance(errs, dict):
                        if errs.get("user") and errs["user"].get("password"):
                            print(" - Password rejected: choose a stronger, less common password in the UI setup.")
                        if errs.get("prefs") and errs["prefs"].get("site_name"):
                            print(" - Site name missing: provide a non-empty site name in the UI setup.")
                        if errs.get("token"):
                            print(" - Setup token mismatch: the setup must be completed interactively in the UI.")

                    print("After completing the UI setup, set METABASE_ADMIN_EMAIL and METABASE_ADMIN_PASSWORD in your .env and re-run this script.")
                    # Abort instead of polling to avoid rate-limiting / infinite loops
                    return None
        except Exception as e:
            print(f"Error calling setup API: {e}")

        # If setup failed because Metabase already configured, try to login again and proceed
        try:
            r = client.post(session_url, json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD}, timeout=10)
            if r.status_code == 200:
                token = r.json().get("id")
                client.headers.update({"X-Metabase-Session": token})
                return client
        except Exception:
            pass

    return None


def add_mongo_datasource(session: httpx.Client):
    db_url = urljoin(METABASE_URL, "/api/database")
    # Attempt to add using connection string in details — Metabase supports different shapes across versions.
    payload = {
        "name": "MongoDB Atlas",
        "engine": "mongo",
        "details": {
            "url": MONGODB_URI,
        }
    }
    try:
        r = session.post(db_url, json=payload, timeout=20)
        if r.status_code in (200, 201):
            print("MongoDB datasource created in Metabase")
            return True
        else:
            print(f"Failed to create datasource: {r.status_code} {r.text}")
            return False
    except Exception as e:
        print(f"Error creating datasource: {e}")
        return False


def main():
    if not MONGODB_URI:
        print("MONGODB_URI not set — cannot configure Metabase datasource")
        return 1

    if not wait_for_metabase(timeout=120):
        return 2

    session = setup_admin_and_db()
    if not session:
        print("Could not obtain Metabase admin session — aborting")
        return 3

    ok = add_mongo_datasource(session)
    return 0 if ok else 4


if __name__ == "__main__":
    exit(main())
