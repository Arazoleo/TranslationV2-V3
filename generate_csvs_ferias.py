#!/usr/bin/env python3
"""
Gera CSVs para o módulo ferias_e_ausencias (z_recessos) a partir do Zazos.

Tabelas geradas:
  - pessoa              (de VIEW_PEOPLE)
  - ausencia_p_j        (de VIEW_AUSENCIAS, tipo PJ/Recesso)
  - solicitacao_ferias  (de VIEW_AUSENCIAS, tipo CLT/Férias)
  - periodo_aquisitivo_p_j (de VIEW_PERIODOS, tipo PJ/Recesso)
  - periodo_aquisitivo     (de VIEW_PERIODOS, tipo CLT/Férias)

Tabelas sem equivalente Zazos (atestados, contrato, configs,
feriados_nacionais): ignoradas.

Routing: baseado no campo "Tipos de ausência" (Ausências) e
"Tipo de Ausência" (Períodos). Ajuste FERIAS_TYPES e RECESSO_TYPES
conforme os valores reais do seu workspace.

Usage:
    python generate_csvs_ferias.py [--token <token>] [--output-dir <path>]
    ZAZOS_TOKEN=... python generate_csvs_ferias.py
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone

import requests

BASE_URL = "https://aws-production-api.zazos.com/v1"
DEFAULT_OUTPUT_DIR = "output/lemon/ferias_e_ausencias"
MAX_RETRIES = 5
RETRY_BACKOFF = 2.0

VIEW_PEOPLE    = "05e81fcf-620c-46e4-af66-e07d82d2ba85"
VIEW_AUSENCIAS = "ed074dce-4db5-4c55-a478-3d70d9bb01cc"
VIEW_PERIODOS  = "3d151cae-f52d-4ab9-be27-fad23a39ed93"

# ---------------------------------------------------------------------------
# Routing: valores do campo Select que indicam CLT/Férias.
# Tudo que não estiver aqui vai para a tabela PJ (ausencia_p_j / periodo_aquisitivo_p_j).
# Ajuste conforme necessário após ver os dados reais.
# ---------------------------------------------------------------------------
FERIAS_TYPES = {"Férias", "Férias CLT", "ferias", "FERIAS"}

# ---------------------------------------------------------------------------
# Column definitions
# ---------------------------------------------------------------------------

PESSOA_COLUMNS = {
    "id":           {"field_id": None, "extract": None, "default": "__record_id__"},
    "nome_completo":{"field_id": "7d7200f3-a3a8-4809-95dd-b87263e3392e", "extract": "string"},
    "created_at":   {"field_id": None, "extract": None, "default": "__timestamptz__"},
    "updated_at":   {"field_id": None, "extract": None, "default": "__timestamptz__"},
}

# Campo usado para routing das Ausências: "Tipos de ausência"
AUSENCIAS_TIPO_FIELD = "6ce0f6db-477f-452f-acb8-a6edc47135ab"

AUSENCIA_PJ_COLUMNS = {
    "id":                   {"field_id": None,                                   "extract": None,     "default": "__record_id__"},
    "pessoa_id":            {"field_id": "989ca5ac-d98d-4f2f-94f6-bceab843e6a0", "extract": "link"},
    "contrato_id":          {"field_id": None,                                   "extract": None},
    "periodo_aquisitivo_id":{"field_id": "b1d1d1b8-9fd2-483d-b1e9-f24c66a6a59a", "extract": "link"},
    "periodo_inicio":       {"field_id": "faa7601f-b85f-4cc1-8a97-cfb6fe5143a9", "extract": "date"},
    "periodo_fim":          {"field_id": "4ed2fcb1-d0c7-4f28-b17c-d0bb6cda9a4b", "extract": "date"},
    "duracao_dias":         {"field_id": "4dca2faa-1e4b-49aa-8d59-92c36a23ce5c", "extract": "number"},
    "valor_recesso":        {"field_id": None,                                   "extract": None},
    "mes_ano_referencia":   {"field_id": None,                                   "extract": None},
    "status":               {"field_id": "15869c32-6949-46e6-88f1-274aff3a4b68", "extract": "select"},
    "observacoes":          {"field_id": "6536fbce-ea8d-4273-8d1a-151bd462fcdc", "extract": "string"},
    "comentario_gestor":    {"field_id": None,                                   "extract": None},
    "comentario_rh":        {"field_id": None,                                   "extract": None},
    "aprovado_gestor_em":   {"field_id": None,                                   "extract": None},
    "aprovado_rh_em":       {"field_id": None,                                   "extract": None},
    "created_at":           {"field_id": None,                                   "extract": None,     "default": "__timestamptz__"},
    "updated_at":           {"field_id": None,                                   "extract": None,     "default": "__timestamptz__"},
    "pagamento_gerado":     {"field_id": None,                                   "extract": None,     "default": "false"},
    "motivo_reprovacao":    {"field_id": None,                                   "extract": None},
}

SOLICITACAO_FERIAS_COLUMNS = {
    "id":                   {"field_id": None,                                   "extract": None,     "default": "__record_id__"},
    "pessoa_id":            {"field_id": "989ca5ac-d98d-4f2f-94f6-bceab843e6a0", "extract": "link"},
    "contrato_id":          {"field_id": None,                                   "extract": None},
    "periodo_aquisitivo_id":{"field_id": "b1d1d1b8-9fd2-483d-b1e9-f24c66a6a59a", "extract": "link"},
    "tipo_solicitacao":     {"field_id": "77755404-3639-4a13-b8e8-e9d1485cbf17", "extract": "select"},
    "titulo_ausencia":      {"field_id": "a4c17a91-04dd-42bb-a7e6-4a5fb3c3b757", "extract": "formula"},
    "inicio_ausencia":      {"field_id": "faa7601f-b85f-4cc1-8a97-cfb6fe5143a9", "extract": "date"},
    "fim_ausencia":         {"field_id": "4ed2fcb1-d0c7-4f28-b17c-d0bb6cda9a4b", "extract": "date"},
    "data_retorno":         {"field_id": None,                                   "extract": None},
    "dias_selecionados":    {"field_id": "4dca2faa-1e4b-49aa-8d59-92c36a23ce5c", "extract": "number"},
    "dias_vendidos":        {"field_id": None,                                   "extract": None,     "default": "0"},
    "adiantamento_13":      {"field_id": None,                                   "extract": None},
    "valor_ferias":         {"field_id": None,                                   "extract": None},
    "mes_ano_referencia":   {"field_id": None,                                   "extract": None},
    "status":               {"field_id": "15869c32-6949-46e6-88f1-274aff3a4b68", "extract": "select"},
    "observacoes":          {"field_id": "6536fbce-ea8d-4273-8d1a-151bd462fcdc", "extract": "string"},
    "comentario_gestor":    {"field_id": None,                                   "extract": None},
    "comentario_rh":        {"field_id": None,                                   "extract": None},
    "aprovado_gestor_em":   {"field_id": None,                                   "extract": None},
    "aprovado_rh_em":       {"field_id": None,                                   "extract": None},
    "data_cancelamento":    {"field_id": None,                                   "extract": None},
    "motivo_reprovacao":    {"field_id": None,                                   "extract": None},
    "validacoes":           {"field_id": None,                                   "extract": None},
    "created_at":           {"field_id": None,                                   "extract": None,     "default": "__timestamptz__"},
    "updated_at":           {"field_id": None,                                   "extract": None,     "default": "__timestamptz__"},
}

# Campo usado para routing dos Períodos: "Tipo de Ausência"
PERIODOS_TIPO_FIELD = "ae3d9774-57b1-4980-b972-8fb2e6fc7561"

PERIODO_PJ_COLUMNS = {
    "id":                        {"field_id": None,                                   "extract": None,   "default": "__record_id__"},
    "pessoa_id":                 {"field_id": "5b87e6c9-3084-4151-aa1e-59cbbb763967", "extract": "link"},
    "data_inicio_aquisitivo":    {"field_id": "0be4d6ca-c178-475d-a24b-c33ab1e30dd5", "extract": "date"},
    "data_fim_aquisitivo":       {"field_id": "bbe13d26-3ddf-43b8-a896-b3c9c888ef4c", "extract": "date"},
    "situacao_periodo":          {"field_id": "ddcc1a3e-772b-4f1a-bff6-9361b5df9f03", "extract": "select"},
    "data_inicio_recessivo":     {"field_id": None,                                   "extract": None},
    "data_fim_recessivo":        {"field_id": None,                                   "extract": None},
    "quantidade_dias_adquiridos":{"field_id": "a9ddc2d7-b9c0-4e56-9d62-d942cb36be82", "extract": "number"},
    "quantidade_dias_utilizados":{"field_id": "0a8a79ae-28f2-40d3-8fef-57d8893b5c41", "extract": "number"},
    "saldo_dias":                {"field_id": "715fc845-540a-4e9c-ab22-459778f92544", "extract": "number"},
    "motivo_recessao":           {"field_id": None,                                   "extract": None},
    "data_concessao_recesso":    {"field_id": None,                                   "extract": None},
    "created_at":                {"field_id": None,                                   "extract": None,   "default": "__timestamptz__"},
    "updated_at":                {"field_id": None,                                   "extract": None,   "default": "__timestamptz__"},
}

PERIODO_CLT_COLUMNS = {
    "id":                        {"field_id": None,                                   "extract": None,   "default": "__record_id__"},
    "pessoa_id":                 {"field_id": "5b87e6c9-3084-4151-aa1e-59cbbb763967", "extract": "link"},
    "contrato_id":               {"field_id": None,                                   "extract": None},
    "data_inicio_aquisitivo":    {"field_id": "0be4d6ca-c178-475d-a24b-c33ab1e30dd5", "extract": "date"},
    "data_fim_aquisitivo":       {"field_id": "bbe13d26-3ddf-43b8-a896-b3c9c888ef4c", "extract": "date"},
    "data_inicio_concessivo":    {"field_id": None,                                   "extract": None},
    "data_fim_concessivo":       {"field_id": None,                                   "extract": None},
    "situacao_periodo":          {"field_id": "ddcc1a3e-772b-4f1a-bff6-9361b5df9f03", "extract": "select"},
    "quantidade_dias_adquiridos":{"field_id": "87e7ddc0-5072-4c26-8bf6-bf5976899f64", "extract": "number"},
    "quantidade_dias_utilizados":{"field_id": "0a8a79ae-28f2-40d3-8fef-57d8893b5c41", "extract": "number"},
    "quantidade_dias_vendidos":  {"field_id": None,                                   "extract": None,   "default": "0"},
    "saldo_dias":                {"field_id": "715fc845-540a-4e9c-ab22-459778f92544", "extract": "number"},
    "tipo_periodo":              {"field_id": "ae3d9774-57b1-4980-b972-8fb2e6fc7561", "extract": "select"},
    "observacoes_rh":            {"field_id": None,                                   "extract": None},
    "motivo_inativacao":         {"field_id": None,                                   "extract": None},
    "created_at":                {"field_id": None,                                   "extract": None,   "default": "__timestamptz__"},
    "updated_at":                {"field_id": None,                                   "extract": None,   "default": "__timestamptz__"},
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
        if strategy == "select":
            return raw[0].get("select_option_display_name", "") if isinstance(raw, list) and raw else ""
        if strategy == "link":
            return raw[0].get("foreign_record_id", "") if isinstance(raw, list) and raw else ""
        if strategy == "attachment":
            return raw[0].get("filename", "") if isinstance(raw, list) and raw else ""
    except Exception:
        return ""
    return ""


def build_row(record, columns, ts):
    record_id = record.get("id", "")
    data = record.get("data", {})
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


def get_tipo_ausencia(record, field_id):
    """Extrai o valor do campo Select de tipo para routing."""
    raw = record.get("data", {}).get(field_id)
    if isinstance(raw, list) and raw:
        return raw[0].get("select_option_display_name", "")
    return ""


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

    # Build uuid_map (zazos_id → supabase_id)
    with open(args.supabase_profiles) as f:
        supabase_by_email = {r['email']: r['id'] for r in csv.DictReader(f) if r.get('email')}
    with open(args.zazos_profiles) as f:
        zazos_profiles = list(csv.DictReader(f))
    uuid_map = {}
    for r in zazos_profiles:
        email = r.get('email', '')
        uuid_map[r['id']] = supabase_by_email[email] if (email and email in supabase_by_email) else r['id']

    with requests.Session() as session:
        session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/json"})

        # ----------------------------------------------------------------
        # pessoa
        # ----------------------------------------------------------------
        print("\nBuscando 'pessoa' (view people)...")
        view = request_with_retry(session, f"{BASE_URL}/builder/views/{VIEW_PEOPLE}")
        people_records = view.get("records", [])
        print(f"  {len(people_records)} registros encontrados.")

        ts = now_iso()
        p_rows = []
        seen_ids = set()
        for record in people_records:
            row = build_row(record, PESSOA_COLUMNS, ts)
            row['id'] = uuid_map.get(row['id'], row['id'])
            if row['id'] not in seen_ids:
                seen_ids.add(row['id'])
                p_rows.append(row)

        out = os.path.join(args.output_dir, "pessoa.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(PESSOA_COLUMNS.keys()))
            writer.writeheader()
            writer.writerows(p_rows)
        print(f"  {len(p_rows)} rows → {out}")

        # ----------------------------------------------------------------
        # ausencias (routing por AUSENCIAS_TIPO_FIELD)
        # ----------------------------------------------------------------
        print("\nBuscando 'ausencias' (view ausencias)...")
        view = request_with_retry(session, f"{BASE_URL}/builder/views/{VIEW_AUSENCIAS}")
        ausencia_records = view.get("records", [])
        print(f"  {len(ausencia_records)} registros encontrados.")

        # Log tipos encontrados para conferência
        tipos_vistos = set()
        for r in ausencia_records:
            tipos_vistos.add(get_tipo_ausencia(r, AUSENCIAS_TIPO_FIELD))
        print(f"  Tipos de ausência encontrados: {sorted(tipos_vistos)}")
        print(f"  → Serão roteados para solicitacao_ferias: {sorted(FERIAS_TYPES & tipos_vistos)}")
        print(f"  → Restante vai para ausencia_p_j")

        ts = now_iso()
        pj_rows = []
        ferias_rows = []
        for record in ausencia_records:
            tipo = get_tipo_ausencia(record, AUSENCIAS_TIPO_FIELD)
            if tipo in FERIAS_TYPES:
                row = build_row(record, SOLICITACAO_FERIAS_COLUMNS, ts)
                row['pessoa_id'] = uuid_map.get(row['pessoa_id'], row['pessoa_id'])
                ferias_rows.append(row)
            else:
                row = build_row(record, AUSENCIA_PJ_COLUMNS, ts)
                row['pessoa_id'] = uuid_map.get(row['pessoa_id'], row['pessoa_id'])
                pj_rows.append(row)

        out = os.path.join(args.output_dir, "ausencia_p_j.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(AUSENCIA_PJ_COLUMNS.keys()))
            writer.writeheader()
            writer.writerows(pj_rows)
        print(f"  {len(pj_rows)} rows → ausencia_p_j.csv")

        out = os.path.join(args.output_dir, "solicitacao_ferias.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(SOLICITACAO_FERIAS_COLUMNS.keys()))
            writer.writeheader()
            writer.writerows(ferias_rows)
        print(f"  {len(ferias_rows)} rows → solicitacao_ferias.csv")

        # ----------------------------------------------------------------
        # periodos aquisitivos (routing por PERIODOS_TIPO_FIELD)
        # ----------------------------------------------------------------
        print("\nBuscando 'periodos aquisitivos'...")
        view = request_with_retry(session, f"{BASE_URL}/builder/views/{VIEW_PERIODOS}")
        periodo_records = view.get("records", [])
        print(f"  {len(periodo_records)} registros encontrados.")

        tipos_vistos = set()
        for r in periodo_records:
            tipos_vistos.add(get_tipo_ausencia(r, PERIODOS_TIPO_FIELD))
        print(f"  Tipos de período encontrados: {sorted(tipos_vistos)}")
        print(f"  → Serão roteados para periodo_aquisitivo (CLT): {sorted(FERIAS_TYPES & tipos_vistos)}")
        print(f"  → Restante vai para periodo_aquisitivo_p_j")

        ts = now_iso()
        periodo_pj_rows = []
        periodo_clt_rows = []
        for record in periodo_records:
            tipo = get_tipo_ausencia(record, PERIODOS_TIPO_FIELD)
            if tipo in FERIAS_TYPES:
                row = build_row(record, PERIODO_CLT_COLUMNS, ts)
                row['pessoa_id'] = uuid_map.get(row['pessoa_id'], row['pessoa_id'])
                periodo_clt_rows.append(row)
            else:
                row = build_row(record, PERIODO_PJ_COLUMNS, ts)
                row['pessoa_id'] = uuid_map.get(row['pessoa_id'], row['pessoa_id'])
                periodo_pj_rows.append(row)

        out = os.path.join(args.output_dir, "periodo_aquisitivo_p_j.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(PERIODO_PJ_COLUMNS.keys()))
            writer.writeheader()
            writer.writerows(periodo_pj_rows)
        print(f"  {len(periodo_pj_rows)} rows → periodo_aquisitivo_p_j.csv")

        out = os.path.join(args.output_dir, "periodo_aquisitivo.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(PERIODO_CLT_COLUMNS.keys()))
            writer.writeheader()
            writer.writerows(periodo_clt_rows)
        print(f"  {len(periodo_clt_rows)} rows → periodo_aquisitivo.csv")

    print("\nConcluído.")
    print("\nATENÇÃO: verifique os logs de routing acima.")
    print("Se os tipos não batem, ajuste FERIAS_TYPES no topo do script e reexecute.")


if __name__ == "__main__":
    main()
