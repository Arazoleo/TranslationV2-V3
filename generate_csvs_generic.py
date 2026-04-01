#!/usr/bin/env python3
"""
Gerador genérico de CSVs a partir de um mapping.json.

Lê o mapping.json (gerado e revisado a partir do suggest_mapping.py),
busca os dados no Zazos e gera os CSVs prontos para importar no Supabase.

Usage:
    python generate_csvs_generic.py --mapping output/<cliente>/<modulo>/mapping.json
    ZAZOS_TOKEN=... python generate_csvs_generic.py --mapping ...
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

BASE_URL    = "https://aws-production-api.zazos.com/v1"
MAX_RETRIES = 5
RETRY_BACKOFF = 2.0


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def request_with_retry(session, url):
    delay = RETRY_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        resp = session.get(url)
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", delay))
            print(f"    [rate limit] aguardando {wait:.1f}s (tentativa {attempt}/{MAX_RETRIES})...")
            time.sleep(wait)
            delay *= 2
            continue
        if resp.status_code in (500, 502, 503, 504):
            print(f"    [erro {resp.status_code}] aguardando {delay:.1f}s (tentativa {attempt}/{MAX_RETRIES})...")
            time.sleep(delay)
            delay *= 2
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"Falhou após {MAX_RETRIES} tentativas: {url}")


# ---------------------------------------------------------------------------
# Extractors
# ---------------------------------------------------------------------------

def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def extract_value(raw, strategy):
    if raw is None:
        return ""
    try:
        if strategy in ("string", "number", "date", "formula"):
            return raw.get("value", "") if isinstance(raw, dict) else (str(raw) if raw else "")
        if strategy == "markdown":
            return str(raw) if raw else ""
        if strategy == "select":
            return raw[0].get("select_option_display_name", "") if isinstance(raw, list) and raw else ""
        if strategy == "link":
            return raw[0].get("foreign_record_id", "") if isinstance(raw, list) and raw else ""
        if strategy == "link_array":
            if isinstance(raw, list) and raw:
                ids = [r.get("foreign_record_id") for r in raw if r.get("foreign_record_id")]
                return json.dumps(ids) if ids else ""
            return ""
        if strategy == "attachment":
            return raw[0].get("filename", "") if isinstance(raw, list) and raw else ""
        if strategy == "bool_select":
            if isinstance(raw, list) and raw:
                v = raw[0].get("select_option_display_name", "").lower()
                return "true" if v in ("sim", "yes", "true", "1") else "false"
            return "false"
    except Exception:
        return ""
    return ""


def build_row(record, columns_def, ts, uuid_map, seq_counter):
    record_id = record.get("id", "")
    data = record.get("data", {})
    row = {}
    for col, col_def in columns_def.items():
        # Ignorar chaves de metadados
        if col.startswith("_"):
            continue

        default = col_def.get("default", "")
        field_id = col_def.get("field_id")
        extract = col_def.get("extract")
        do_remap = col_def.get("uuid_remap", False)

        if default == "__record_id__":
            val = record_id
            if do_remap and uuid_map:
                val = uuid_map.get(val, val)
        elif default == "__seq__":
            val = str(seq_counter[0])
            seq_counter[0] += 1
        elif default == "__timestamptz__":
            val = ts
        elif field_id is None:
            val = default
        else:
            raw = data.get(field_id)
            val = extract_value(raw, extract)
            if do_remap and uuid_map and val:
                val = uuid_map.get(val, val)

        row[col] = val
    return row


# ---------------------------------------------------------------------------
# UUID remap
# ---------------------------------------------------------------------------

def build_uuid_map(supabase_profiles_path, zazos_profiles_path):
    with open(supabase_profiles_path) as f:
        supabase_by_email = {r['email']: r['id'] for r in csv.DictReader(f) if r.get('email')}
    with open(zazos_profiles_path) as f:
        zazos_profiles = list(csv.DictReader(f))
    uuid_map = {}
    for r in zazos_profiles:
        email = r.get('email', '')
        uuid_map[r['id']] = supabase_by_email[email] if (email and email in supabase_by_email) else r['id']
    return uuid_map


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mapping", required=True, help="Caminho para o mapping.json")
    parser.add_argument("--token", help="Zazos Bearer token (ou ZAZOS_TOKEN env)")
    args = parser.parse_args()

    token = args.token or os.environ.get("ZAZOS_TOKEN")
    if not token:
        sys.exit("Erro: informe --token ou defina ZAZOS_TOKEN.")

    with open(args.mapping) as f:
        mapping = json.load(f)

    output_dir = mapping["output_dir"]
    os.makedirs(output_dir, exist_ok=True)

    # UUID remap
    uuid_map = None
    remap_cfg = mapping.get("uuid_remap", {})
    if remap_cfg.get("enabled"):
        print("Carregando uuid_map...")
        uuid_map = build_uuid_map(
            remap_cfg["supabase_profiles"],
            remap_cfg["zazos_profiles"]
        )
        print(f"  {len(uuid_map)} entradas no uuid_map.")

    with requests.Session() as session:
        session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/json"})

        for table in mapping["tables"]:
            table_name = table["name"]
            view_id    = table["view_id"]
            columns_def = {k: v for k, v in table["columns"].items() if not k.startswith("_")}
            dedup_by   = table.get("dedup_by", [])

            if view_id == "PREENCHER":
                print(f"\n[SKIP] {table_name} — view_id não preenchido.")
                continue

            print(f"\nBuscando '{table_name}' (view {view_id})...")
            view = request_with_retry(session, f"{BASE_URL}/builder/views/{view_id}")
            records = view.get("records", [])
            print(f"  {len(records)} registros encontrados.")

            ts = now_iso()
            rows = []
            seq_counter = [1]  # mutable counter para __seq__
            for record in records:
                row = build_row(record, columns_def, ts, uuid_map, seq_counter)
                rows.append(row)

            # Deduplicação
            if dedup_by:
                seen = set()
                deduped = []
                for r in rows:
                    key = tuple(r.get(c, "") for c in dedup_by)
                    if key not in seen:
                        seen.add(key)
                        deduped.append(r)
                removed = len(rows) - len(deduped)
                if removed:
                    print(f"  Deduplicados por {dedup_by}: {removed} removidos → {len(deduped)} rows")
                rows = deduped

            fieldnames = [k for k in columns_def.keys() if not k.startswith("_")]
            out = os.path.join(output_dir, f"{table_name}.csv")
            with open(out, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            print(f"  {len(rows)} rows → {out}")

    print("\nConcluído.")


if __name__ == "__main__":
    main()
