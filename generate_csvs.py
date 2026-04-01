#!/usr/bin/env python3
"""
Fetches records from Zazos and generates one CSV per schema.json table,
mapping Zazos fields to schema columns.

Usage:
    python generate_csvs.py [--token <bearer_token>] [--output-dir <path>]

The token can also be set via the ZAZOS_TOKEN environment variable.
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

BASE_URL = "https://aws-production-api.zazos.com/v1"
DEFAULT_OUTPUT_DIR = "output"
MAX_RETRIES = 5
RETRY_BACKOFF = 2.0

# PostgreSQL trunca identificadores a 63 bytes; a coluna real no Supabase é sem o "u" final.
PG_DESEJA_CENTRO_CUSTO_COL = "deseja_alocar_o_reembolso_em_um_centro_de_custo_diferente_do_se"


def default_timestamptz_csv():
    """ISO-8601 para import CSV no Supabase (evita literal 'now()')."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# Mapping: schema_table -> view_id + column definitions
#
# Each column entry:
#   field_id  : Zazos field UUID (key in record["data"])
#   extract   : one of "string" | "number" | "date" | "select" | "link" |
#               "lookup" | "attachment" | "formula" | "markdown" | None
#   default   : value to use when field is absent / None to leave blank
# ---------------------------------------------------------------------------

SCHEMA_MAPPING = {
    "expenses": {
        "view_id": "5271fc5a-90ab-49a6-b53a-9243b2c27895",
        "columns": {
            "id":                        {"field_id": None,                                     "extract": None,         "default": "__record_id__"},
            "expense_description":       {"field_id": "b7e46458-f222-41cf-8f35-3a7dd8021f15",  "extract": "string"},
            "tipo_de_lancamento":        {"field_id": None,                                     "extract": None,         "default": "Reembolso"},
            "expense_category":          {"field_id": "36a15b39-5353-4c39-92b0-721397f50308",  "extract": "select"},
            "expense_value_v2":          {"field_id": "5d8f25c4-4d77-4295-8d2a-c3993b2f533d",  "extract": "number"},
            "expense_date":              {"field_id": "741200dd-b8cf-4a38-8703-2019fa15e2c6",  "extract": "date"},
            "expense_person":            {"field_id": "0a68be4b-388a-4e04-bca9-33724f25f341",  "extract": "link"},
            "expense_receipt":           {"field_id": "5e69dea6-e821-4ef2-a43b-19e9e1f0c08b",  "extract": "attachment"},
            "budget_id":                 {"field_id": "47a156ec-57a0-4689-809d-72cc00efe9cc",  "extract": "link"},
            "expense_status":            {"field_id": "ba13c390-ac65-4a7f-901c-5697a30aa828",  "extract": "select"},
            "expense_refundable":        {"field_id": "5447ef4e-acc5-4900-9fb5-f348141bd884",  "extract": "select"},
            "valor_reembolso_parcial":   {"field_id": None,                                     "extract": None},
            "expense_notes":             {"field_id": "bdea3530-8b5b-4120-befe-6d894613701e",  "extract": "string"},
            "comentarios_lideranca":     {"field_id": "f69b7bb0-362b-45ef-8b6a-0d265d4515c2",  "extract": "string"},
            PG_DESEJA_CENTRO_CUSTO_COL:
                                         {"field_id": "0f5a0598-c1df-49d6-bcc5-68fdc860ee79",  "extract": "select"},
            "centro_de_custo_realocacao":{"field_id": "c1fa223f-46c6-4130-8f64-8e31185aca70",  "extract": "select"},
            "cost_center":               {"field_id": "63510626-a08d-46f2-aa3b-a5779fb9d1f6",  "extract": "lookup"},
            "created_at":                {"field_id": None,                                     "extract": None,         "default": "__timestamptz__"},
            "updated_at":                {"field_id": None,                                     "extract": None,         "default": "__timestamptz__"},
        },
    },
    "budgets": {
        "view_id": "e8bc65ca-982f-4aa6-904b-4477654f44f7",
        "columns": {
            "id":                        {"field_id": None,                                     "extract": None,         "default": "__record_id__"},
            "name":                      {"field_id": "f91981bf-c6ae-475f-b801-2f6b34d7dfca",  "extract": "string"},
            "responsible_profile_id":    {"field_id": "f2dc4667-e260-41c2-95d2-dd885e5fac28",  "extract": "link"},
            "aligned_with_profile_id":   {"field_id": "50d0c63a-ac48-4eaa-ae04-a662e60d7a8c",  "extract": "link"},
            "cost_center":               {"field_id": "9c5401c9-2a19-4781-8c7c-6e79357d578d",  "extract": "lookup"},
            "budget_amount":             {"field_id": "858c0878-531c-4910-9cb6-02909afb66e0",  "extract": "number"},
            "consumed_amount":           {"field_id": "d760e09a-2288-4078-b9b1-10edcb752825",  "extract": "number"},
            "balance_amount":            {"field_id": "0aa27de0-81df-40eb-a768-9934fbd5ef46",  "extract": "number"},
            "created_at":                {"field_id": None,                                     "extract": None,         "default": "now()"},
            "updated_at":                {"field_id": None,                                     "extract": None,         "default": "now()"},
        },
    },
}


# ---------------------------------------------------------------------------
# Value extractors
# ---------------------------------------------------------------------------

def extract_value(raw, strategy):
    if raw is None:
        return ""
    try:
        if strategy == "string":
            if isinstance(raw, dict):
                return raw.get("value", "")
            return str(raw)
        if strategy == "number":
            if isinstance(raw, dict):
                return raw.get("value", "")
            return raw
        if strategy == "date":
            if isinstance(raw, dict):
                return raw.get("value", "")
            return raw
        if strategy == "formula":
            if isinstance(raw, dict):
                return raw.get("value", "")
            return raw
        if strategy == "markdown":
            return str(raw) if raw else ""
        if strategy == "select":
            if isinstance(raw, list) and raw:
                return raw[0].get("select_option_display_name", "")
            return ""
        if strategy == "link":
            if isinstance(raw, list) and raw:
                return raw[0].get("foreign_record_id", "")
            return ""
        if strategy == "lookup":
            if isinstance(raw, list) and raw:
                inner = raw[0].get("foreign_record_display_name")
                if isinstance(inner, list) and inner:
                    return inner[0].get("foreign_record_display_name", "")
                return inner or ""
            return ""
        if strategy == "attachment":
            if isinstance(raw, list) and raw:
                return raw[0].get("filename", "")
            return ""
    except Exception:
        return ""
    return ""


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def request_with_retry(session, url):
    delay = RETRY_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        resp = session.get(url)
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", delay))
            print(f"    [rate limit] waiting {wait:.1f}s (attempt {attempt}/{MAX_RETRIES})...")
            time.sleep(wait)
            delay *= 2
            continue
        if resp.status_code in (500, 502, 503, 504):
            print(f"    [server error {resp.status_code}] waiting {delay:.1f}s (attempt {attempt}/{MAX_RETRIES})...")
            time.sleep(delay)
            delay *= 2
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"Failed after {MAX_RETRIES} retries: {url}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate CSVs from Zazos data mapped to schema.json.")
    parser.add_argument("--token", help="Bearer token (or set ZAZOS_TOKEN env var)")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    token = args.token or os.environ.get("ZAZOS_TOKEN")
    if not token:
        sys.exit("Error: provide --token or set the ZAZOS_TOKEN environment variable.")

    os.makedirs(args.output_dir, exist_ok=True)

    with requests.Session() as session:
        session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/json"})

        for table_name, mapping in SCHEMA_MAPPING.items():
            view_id = mapping["view_id"]
            columns = mapping["columns"]

            print(f"\nFetching view for table '{table_name}' (view {view_id})...")
            view = request_with_retry(session, f"{BASE_URL}/builder/views/{view_id}")
            records = view.get("records", [])
            print(f"  {len(records)} records found.")

            out_path = os.path.join(args.output_dir, f"{table_name}.csv")
            with open(out_path, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=list(columns.keys()))
                writer.writeheader()

                for record in records:
                    record_id = record.get("id", "")
                    data = record.get("data", {})
                    row = {}

                    for col_name, col_def in columns.items():
                        default = col_def.get("default", "")
                        if default == "__record_id__":
                            row[col_name] = record_id
                            continue
                        if default == "__timestamptz__":
                            row[col_name] = default_timestamptz_csv()
                            continue
                        if default and col_def["field_id"] is None:
                            row[col_name] = default
                            continue

                        field_id = col_def["field_id"]
                        strategy = col_def["extract"]
                        raw = data.get(field_id) if field_id else None
                        row[col_name] = extract_value(raw, strategy)

                    writer.writerow(row)

            print(f"  Saved to {out_path}")

    print("\nDone.")


if __name__ == "__main__":
    main()
