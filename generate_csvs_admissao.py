#!/usr/bin/env python3
"""
Gera CSVs para o módulo admissao a partir do Zazos.
Tabelas: profiles, admissao.
Sem equivalente (tipo_admissao, etapa_admissao, template_contrato,
template_email, pergunta_admissao, resposta_admissao, configs): ignoradas.

Usage:
    python generate_csvs_admissao.py [--token <token>] [--output-dir <path>]
    ZAZOS_TOKEN=... python generate_csvs_admissao.py
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
DEFAULT_OUTPUT_DIR = "output/lemon/admissao"
MAX_RETRIES = 5
RETRY_BACKOFF = 2.0

VIEW_ONBOARDING = "e1325bed-fa47-4f86-9e34-6694b70821cb"
VIEW_PEOPLE     = "05e81fcf-620c-46e4-af66-e07d82d2ba85"

ADMISSAO_COLUMNS = {
    "id":                   {"field_id": None,                                    "extract": None,        "default": "__record_id__"},
    "pessoa_id":            {"field_id": "e6ec26b6-3b3f-40c1-9662-78e6bf5a0fa7",  "extract": "link"},
    "tipo_admissao_id":     {"field_id": None,                                    "extract": None},
    "tipo_contrato":        {"field_id": "cbdffb7a-278c-4497-8d97-7349e22c96b5",  "extract": "select"},
    "n1_id":                {"field_id": None,                                    "extract": None},
    "n2_id":                {"field_id": None,                                    "extract": None},
    "n3_id":                {"field_id": None,                                    "extract": None},
    "responsavel_rh_id":    {"field_id": "abb9cfca-254b-42e7-8ccf-0b71763fc2b5",  "extract": "link"},
    "cargo_id":             {"field_id": None,                                    "extract": None},
    "beneficios_ids":       {"field_id": "107503bc-ca35-4acc-b573-fe2be515604f",  "extract": "link_array"},
    "status_dados_pessoais":{"field_id": "a32c32ee-8306-4fcc-8218-13ee71749967",  "extract": "select"},
    "etapa_atual_id":       {"field_id": None,                                    "extract": None},
    "created_at":           {"field_id": None,                                    "extract": None,        "default": "__timestamptz__"},
    "updated_at":           {"field_id": None,                                    "extract": None,        "default": "__timestamptz__"},
    "observacao":           {"field_id": "5dc46abd-0288-46f1-992a-6c96a24c5a22",  "extract": "markdown"},
    "gestor_id":            {"field_id": "1c97a864-e2e1-45d4-ba38-1c5cfa7cc2c2",  "extract": "link"},
    "primeiro_emprego_clt": {"field_id": None,                                    "extract": None,        "default": "false"},
}

PROFILES_COLUMNS = {
    "id":           {"field_id": None,                                    "extract": None,   "default": "__record_id__"},
    "nome_completo":{"field_id": "7d7200f3-a3a8-4809-95dd-b87263e3392e",  "extract": "string"},
    "created_at":   {"field_id": None,                                    "extract": None,   "default": "__timestamptz__"},
    "updated_at":   {"field_id": None,                                    "extract": None,   "default": "__timestamptz__"},
}


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
            # Returns a JSON array of foreign_record_ids
            if isinstance(raw, list) and raw:
                ids = [r.get("foreign_record_id") for r in raw if r.get("foreign_record_id")]
                return json.dumps(ids) if ids else ""
            return ""
        if strategy == "attachment":
            return raw[0].get("filename", "") if isinstance(raw, list) and raw else ""
    except Exception:
        return ""
    return ""


def build_row(record, columns):
    record_id = record.get("id", "")
    data = record.get("data", {})
    ts = now_iso()
    row = {}
    for col, col_def in columns.items():
        default = col_def.get("default", "")
        if default == "__record_id__":
            row[col] = record_id
        elif default == "__timestamptz__":
            row[col] = ts
        elif col_def["field_id"] is None:
            row[col] = default
        else:
            raw = data.get(col_def["field_id"])
            row[col] = extract_value(raw, col_def["extract"])
    return row


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
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--token")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    # Reuse uuid_map from pessoas module
    parser.add_argument("--supabase-profiles",
                        default="output/lemon/pessoas/profiles_rows (1).csv",
                        help="CSV exportado do Supabase com profiles existentes")
    parser.add_argument("--zazos-profiles",
                        default="output/lemon/pessoas/profiles.csv",
                        help="CSV gerado do Zazos com todos os profiles")
    args = parser.parse_args()

    token = args.token or os.environ.get("ZAZOS_TOKEN")
    if not token:
        sys.exit("Erro: informe --token ou defina ZAZOS_TOKEN.")

    os.makedirs(args.output_dir, exist_ok=True)

    # Build uuid_map
    with open(args.supabase_profiles) as f:
        supabase_by_email = {r['email']: r['id'] for r in csv.DictReader(f) if r['email']}
    with open(args.zazos_profiles) as f:
        zazos_profiles = list(csv.DictReader(f))
    uuid_map = {}
    for r in zazos_profiles:
        email = r.get('email', '')
        uuid_map[r['id']] = supabase_by_email[email] if (email and email in supabase_by_email) else r['id']

    with requests.Session() as session:
        session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/json"})

        # --- profiles ---
        print("\nBuscando 'profiles' (view people)...")
        view = request_with_retry(session, f"{BASE_URL}/builder/views/{VIEW_PEOPLE}")
        people_records = view.get("records", [])
        print(f"  {len(people_records)} registros encontrados.")

        p_rows = []
        seen_ids = set()
        for record in people_records:
            row = build_row(record, PROFILES_COLUMNS)
            row['id'] = uuid_map.get(row['id'], row['id'])
            if row['id'] not in seen_ids:
                seen_ids.add(row['id'])
                p_rows.append(row)

        out = os.path.join(args.output_dir, "profiles.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(PROFILES_COLUMNS.keys()))
            writer.writeheader()
            writer.writerows(p_rows)
        print(f"  {len(p_rows)} rows (deduplicado) → {out}")

        # --- admissao ---
        print("\nBuscando 'admissao' (view onboarding)...")
        view = request_with_retry(session, f"{BASE_URL}/builder/views/{VIEW_ONBOARDING}")
        records = view.get("records", [])
        print(f"  {len(records)} registros encontrados.")

        a_rows = []
        for record in records:
            row = build_row(record, ADMISSAO_COLUMNS)
            # Remap FKs
            for fk_col in ("pessoa_id", "responsavel_rh_id", "gestor_id"):
                if row.get(fk_col):
                    row[fk_col] = uuid_map.get(row[fk_col], row[fk_col])
            a_rows.append(row)

        out = os.path.join(args.output_dir, "admissao.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(ADMISSAO_COLUMNS.keys()))
            writer.writeheader()
            writer.writerows(a_rows)
        print(f"  {len(a_rows)} rows → {out}")

    print("\nConcluído.")


if __name__ == "__main__":
    main()
