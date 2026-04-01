#!/usr/bin/env python3
"""
Gera CSVs para o módulo pagamentos a partir do Zazos.
Tabelas: pagamentos_p_j, profiles, competencias.
Tabelas sem equivalente (pagamentos_clt, configs) são ignoradas.

competencias é derivada dos registros de pagamentos (mês/ano únicos de payment_fiscal_date).

Usage:
    python generate_csvs_pagamentos.py [--token <token>] [--output-dir <path>]
    ZAZOS_TOKEN=... python generate_csvs_pagamentos.py
"""

import argparse
import csv
import os
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

import requests

BASE_URL = "https://aws-production-api.zazos.com/v1"
DEFAULT_OUTPUT_DIR = "output/lemon/pagamentos"
MAX_RETRIES = 5
RETRY_BACKOFF = 2.0

VIEW_PAYMENTS = "0e0c0c77-4573-44ec-8f3d-2f8f21aca2cb"
VIEW_PEOPLE   = "05e81fcf-620c-46e4-af66-e07d82d2ba85"

PAYMENTS_COLUMNS = {
    "id":                   {"field_id": None,                                    "extract": None,       "default": "__record_id__"},
    "email":                {"field_id": None,                                    "extract": None},
    "cnpj":                 {"field_id": "2299fc80-1258-4fea-9a02-784adfcc43ca",  "extract": "formula"},
    "pessoa_id":            {"field_id": "6adfd229-78e2-484e-82b4-b6bab1904501",  "extract": "link"},
    "data_admissao":        {"field_id": "9bcd0bc6-cce3-44f6-a90f-0d2b6b6261c9",  "extract": "date"},
    "valor_mensal":         {"field_id": "5c957b1f-802f-4be3-9afd-c3e5f212e3c7",  "extract": "number"},
    "ferias":               {"field_id": None,                                    "extract": None,       "default": "0"},
    "plano_saude":          {"field_id": "7f41daa3-a976-4f09-9643-c2181ba7201d",  "extract": "number"},
    "dissidio":             {"field_id": None,                                    "extract": None,       "default": "0"},
    "total":                {"field_id": "2c61494c-e41d-49de-b397-dfbba729dcd2",  "extract": "formula"},
    "created_at":           {"field_id": None,                                    "extract": None,       "default": "__timestamptz__"},
    "updated_at":           {"field_id": None,                                    "extract": None,       "default": "__timestamptz__"},
    "data_competencia":     {"field_id": "f5aed61d-f8d4-415f-b05c-42159d6b21aa",  "extract": "date"},
    "data_vencimento":      {"field_id": "2f09128e-463d-48a4-a8f5-f8b55b0a369d",  "extract": "date"},
    "odonto_dependentes":   {"field_id": "53cbf405-a167-4912-a8fe-2931085f788b",  "extract": "number"},
    "nota_fiscal_url":      {"field_id": "0733cfa0-afff-487f-bc20-88dbcfa973d4",  "extract": "attachment"},
    "status_nota_fiscal":   {"field_id": "6661deb5-2f1a-4ad4-8258-a0a810e7074c",  "extract": "select"},
    "nota_fiscal_data_envio":{"field_id": "2ced386f-1292-4515-a500-34fb6cad133c", "extract": "date"},
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
            return raw.get("value", "") if isinstance(raw, dict) else (str(raw) if raw is not None else "")
        if strategy == "markdown":
            return str(raw) if raw else ""
        if strategy == "select":
            return raw[0].get("select_option_display_name", "") if isinstance(raw, list) and raw else ""
        if strategy == "link":
            return raw[0].get("foreign_record_id", "") if isinstance(raw, list) and raw else ""
        if strategy == "attachment":
            return raw[0].get("filename", "") if isinstance(raw, list) and raw else ""
    except Exception:
        return ""
    return ""


def build_row(record, columns):
    record_id = record.get("id", "")
    data = record.get("data", {})
    row = {}
    ts = now_iso()
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
# Competencias derivation
# ---------------------------------------------------------------------------

def derive_competencias(payment_records):
    """
    Extrai mês/ano únicos de payment_fiscal_date e gera uma linha por competência.
    data_pagamento = payment_date do primeiro registro encontrado para aquela competência.
    """
    FISCAL_DATE_FIELD  = "f5aed61d-f8d4-415f-b05c-42159d6b21aa"
    PAYMENT_DATE_FIELD = "5933ab40-14d4-4902-84fc-18fdc93bb0d9"

    seen = {}  # (year, month) -> {"data_pagamento": ...}
    for record in payment_records:
        data = record.get("data", {})
        raw_fiscal = data.get(FISCAL_DATE_FIELD)
        raw_payment = data.get(PAYMENT_DATE_FIELD)

        fiscal_val = raw_fiscal.get("value", "") if isinstance(raw_fiscal, dict) else ""
        payment_val = raw_payment.get("value", "") if isinstance(raw_payment, dict) else ""

        if not fiscal_val:
            continue
        try:
            dt = datetime.strptime(fiscal_val, "%Y-%m-%d")
            key = (dt.year, dt.month)
            if key not in seen:
                seen[key] = payment_val
        except ValueError:
            continue

    rows = []
    for (year, month), data_pagamento in sorted(seen.items()):
        rows.append({
            "id":                   str(uuid.uuid4()),
            "mes":                  month,
            "ano":                  year,
            "data_pagamento":       data_pagamento,
            "status":               "rascunho",
            "pdf_consolidado_url":  "",
            "total_documentos":     "0",
            "documentos_vinculados":"0",
            "documentos_pendentes": "0",
            "created_by":           "",
            "created_at":           now_iso(),
            "updated_at":           now_iso(),
        })
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--token")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    token = args.token or os.environ.get("ZAZOS_TOKEN")
    if not token:
        sys.exit("Erro: informe --token ou defina ZAZOS_TOKEN.")

    os.makedirs(args.output_dir, exist_ok=True)

    with requests.Session() as session:
        session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/json"})

        # --- pagamentos_p_j ---
        print("\nBuscando 'pagamentos_p_j' (view payments)...")
        view = request_with_retry(session, f"{BASE_URL}/builder/views/{VIEW_PAYMENTS}")
        payment_records = view.get("records", [])
        print(f"  {len(payment_records)} registros encontrados.")

        out = os.path.join(args.output_dir, "pagamentos_p_j.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(PAYMENTS_COLUMNS.keys()))
            writer.writeheader()
            for record in payment_records:
                writer.writerow(build_row(record, PAYMENTS_COLUMNS))
        print(f"  Salvo em {out}")

        # --- competencias (derivada) ---
        print("\nDerivando 'competencias' de payment_fiscal_date...")
        competencias = derive_competencias(payment_records)
        print(f"  {len(competencias)} competências únicas encontradas.")

        competencia_cols = ["id","mes","ano","data_pagamento","status","pdf_consolidado_url",
                            "total_documentos","documentos_vinculados","documentos_pendentes",
                            "created_by","created_at","updated_at"]
        out = os.path.join(args.output_dir, "competencias.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=competencia_cols)
            writer.writeheader()
            writer.writerows(competencias)
        print(f"  Salvo em {out}")

        # --- profiles ---
        print("\nBuscando 'profiles' (view people)...")
        view = request_with_retry(session, f"{BASE_URL}/builder/views/{VIEW_PEOPLE}")
        people_records = view.get("records", [])
        print(f"  {len(people_records)} registros encontrados.")

        out = os.path.join(args.output_dir, "profiles.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(PROFILES_COLUMNS.keys()))
            writer.writeheader()
            for record in people_records:
                writer.writerow(build_row(record, PROFILES_COLUMNS))
        print(f"  Salvo em {out}")

    print("\nConcluído.")


if __name__ == "__main__":
    main()
