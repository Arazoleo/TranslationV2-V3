#!/usr/bin/env python3
"""
Fetches all sheets from the Zazos API along with their default view details
(fields and record count) and saves the result to output/sheets_default_views.json.

Usage:
    python fetch_sheets.py [--token <bearer_token>] [--output <path>]

The token can also be set via the ZAZOS_TOKEN environment variable.
"""

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

BASE_URL = "https://aws-production-api.zazos.com/v1"
DEFAULT_OUTPUT = "output/sheets_default_views.json"
MAX_WORKERS = 4
MAX_RETRIES = 5
RETRY_BACKOFF = 2.0  # seconds, doubled on each retry


def build_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }


def request_with_retry(session: requests.Session, url: str) -> dict:
    delay = RETRY_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        resp = session.get(url)
        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", delay))
            print(f"    [rate limit] waiting {retry_after:.1f}s before retry {attempt}/{MAX_RETRIES}...")
            time.sleep(retry_after)
            delay *= 2
            continue
        if resp.status_code in (500, 502, 503, 504):
            print(f"    [server error {resp.status_code}] waiting {delay:.1f}s before retry {attempt}/{MAX_RETRIES}...")
            time.sleep(delay)
            delay *= 2
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"Failed after {MAX_RETRIES} retries: {url}")


def fetch_sheets(session: requests.Session) -> list:
    return request_with_retry(session, f"{BASE_URL}/sheets")


def fetch_view(session: requests.Session, view_id: str) -> dict:
    return request_with_retry(session, f"{BASE_URL}/builder/views/{view_id}")


def extract_view_data(view: dict) -> dict:
    return {
        "records_count": len(view.get("records", [])),
        "fields": [
            {
                "id": f["id"],
                "name": f["name"],
                "type": f.get("type"),
                "system_name": f.get("system_name"),
            }
            for f in view.get("fields", [])
        ],
    }


def build_sheet_entry(sheet: dict) -> dict:
    default_view = next(
        (v for v in sheet.get("views", []) if v.get("page_data_default")), None
    )
    return {
        "sheet_id": sheet["id"],
        "sheet_name": sheet["name"],
        "system_name": sheet.get("system_name"),
        "default_view_id": default_view["id"] if default_view else None,
    }


def main():
    parser = argparse.ArgumentParser(description="Fetch Zazos sheets and view details.")
    parser.add_argument("--token", help="Bearer token (or set ZAZOS_TOKEN env var)")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output JSON file path")
    args = parser.parse_args()

    token = args.token or os.environ.get("ZAZOS_TOKEN")
    if not token:
        sys.exit("Error: provide --token or set the ZAZOS_TOKEN environment variable.")

    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    with requests.Session() as session:
        session.headers.update(build_headers(token))

        print("Fetching sheets...")
        sheets = fetch_sheets(session)
        print(f"  Found {len(sheets)} sheets.")

        entries = [build_sheet_entry(s) for s in sheets]
        view_ids = [(i, e["default_view_id"]) for i, e in enumerate(entries) if e["default_view_id"]]

        print(f"Fetching view details for {len(view_ids)} views (workers={MAX_WORKERS}, retries={MAX_RETRIES})...")

        def fetch_one(item):
            idx, view_id = item
            view = fetch_view(session, view_id)
            return idx, extract_view_data(view)

        failed = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_one, item): item for item in view_ids}
            for i, future in enumerate(as_completed(futures), 1):
                item = futures[future]
                try:
                    idx, view_data = future.result()
                    entries[idx].update(view_data)
                    print(f"  [{i}/{len(view_ids)}] {entries[idx]['sheet_name']}: "
                          f"{view_data['records_count']} records, {len(view_data['fields'])} fields")
                except Exception as e:
                    idx = item[0]
                    print(f"  [{i}/{len(view_ids)}] ERROR on '{entries[idx]['sheet_name']}': {e}")
                    failed.append(entries[idx]["sheet_name"])

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(entries, f, indent=2, ensure_ascii=False)

    print(f"\nSaved to {args.output}")
    if failed:
        print(f"Failed sheets ({len(failed)}): {', '.join(failed)}")


if __name__ == "__main__":
    main()
