#!/usr/bin/env python3
"""
Gera CSVs para o módulo pessoas/lemon a partir do Zazos.
Tabelas: profiles, dependentes, empresas, contratos.
Tabelas sem sheet equivalente (n1, n2, n3, senioridade, cargos, parceiros) são ignoradas.

PKs bigint (dependentes, empresas, contratos) recebem ID sequencial.

Usage:
    python generate_csvs_pessoas.py [--token <token>] [--output-dir <path>]
    ZAZOS_TOKEN=... python generate_csvs_pessoas.py
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone

import requests

BASE_URL = "https://aws-production-api.zazos.com/v1"
DEFAULT_OUTPUT_DIR = "output/lemon/pessoas"
MAX_RETRIES = 5
RETRY_BACKOFF = 2.0

SCHEMA_MAPPING = {
    "profiles": {
        "view_id": "a0586d63-6849-4b42-8f7c-6dabb9a07a73",
        "pk": "uuid",
        "columns": {
            "id":                           {"field_id": None,                                    "extract": None,         "default": "__record_id__"},
            "created_at":                   {"field_id": None,                                    "extract": None,         "default": "__timestamptz__"},
            "updated_at":                   {"field_id": None,                                    "extract": None,         "default": "__timestamptz__"},
            "email":                        {"field_id": "f163c8f3-a374-49ce-8c23-8ab0b2afeb86",  "extract": "string"},
            "full_name":                    {"field_id": "42dde05d-0757-402d-8151-a08ef47ce7f4",  "extract": "string"},
            "avatar_url":                   {"field_id": "15e26ae7-4390-472d-b80c-decfdedc0ac9",  "extract": "coverimage"},
            "n1_id":                        {"field_id": None,                                    "extract": None},
            "n2_id":                        {"field_id": None,                                    "extract": None},
            "n3_id":                        {"field_id": None,                                    "extract": None},
            "cnh_categoria":                {"field_id": None,                                    "extract": None},
            "cnh_num":                      {"field_id": "bbe300fd-c21f-46e5-bfbd-f828094a7015",  "extract": "string"},
            "ctps_emissao":                 {"field_id": "85114225-8969-41e0-9c62-c4c619b557bf",  "extract": "date"},
            "ctps_estado":                  {"field_id": "f2eb30bf-15e2-430f-a12e-f6ed8af4f68a",  "extract": "string"},
            "ctps_num":                     {"field_id": "e0f3d93c-6161-4f30-96e8-ec86f7a327b7",  "extract": "string"},
            "ctps_serie":                   {"field_id": "a2a35302-1da0-4a22-88a9-4876c08c98f9",  "extract": "string"},
            "doc_militar_num":              {"field_id": "687911c8-d3ff-4fb6-8236-259906ab25ac",  "extract": "string"},
            "pis_num":                      {"field_id": "6a3a36e6-85aa-4711-be65-605350e8048d",  "extract": "string"},
            "titulo_eleitor_num":           {"field_id": "46dfd20b-08fd-465b-83c8-16a493ce375a",  "extract": "string"},
            "titulo_eleitor_secao":         {"field_id": "21752e40-895e-402f-94a7-ae3ed71b58fc",  "extract": "string"},
            "titulo_eleitor_zona":          {"field_id": "d1fed270-02b7-4ec7-9496-b3c9a1c087bd",  "extract": "string"},
            "cpf":                          {"field_id": "6230c438-53fe-4394-b150-e6876515e424",  "extract": "string"},
            "genero":                       {"field_id": "9dc8fc17-0dbf-45e8-a002-54e7e556741d",  "extract": "select"},
            "estado_civil":                 {"field_id": "7c492fe2-236b-4ec1-bc8d-6a6c8df4065a",  "extract": "select"},
            "nome_mae":                     {"field_id": "1183d50e-a69d-4289-9b6d-847b7d145476",  "extract": "string"},
            "nome_pai":                     {"field_id": "a9d4d7d6-2b9b-42e6-a7b4-b22a3c6e3f20",  "extract": "string"},
            "raca_etnia":                   {"field_id": "1267f731-3a52-42d4-bc88-8549e5bd5dea",  "extract": "select"},
            "pcd":                          {"field_id": "7b95ff33-3d3f-4c50-a5c4-343dd2302f1d",  "extract": "bool_select"},
            "pcd_tipo":                     {"field_id": None,                                    "extract": None},
            "pcd_observacoes":              {"field_id": "7fc7f5e6-89f9-4091-a0a8-23ab6681f2e0",  "extract": "string"},
            "rg_numero":                    {"field_id": "c4da02c0-8690-4ca1-8315-1cb1fccd567f",  "extract": "string"},
            "rg_orgao_emissor":             {"field_id": "cb6ad4cc-c7ba-4c1b-a118-b9e52d7dd5cf",  "extract": "string"},
            "rg_uf":                        {"field_id": None,                                    "extract": None},
            "rg_data_emissao":              {"field_id": "d281aa41-762c-41fd-9ab4-e3f0e3f1ea6b",  "extract": "date"},
            "formacao_instituicao":         {"field_id": None,                                    "extract": None},
            "formacao_curso":               {"field_id": "df231e83-441d-4708-980a-e39520ff9ed6",  "extract": "string"},
            "formacao_ano_conclusao":       {"field_id": None,                                    "extract": None},
            "banco":                        {"field_id": "3cf1aeed-1ee6-439e-9c9c-90705892e35f",  "extract": "string"},
            "agencia":                      {"field_id": "45b26f50-d1aa-4981-975f-1ed73bbb7c22",  "extract": "string"},
            "numero_conta":                 {"field_id": "8b0cba46-47d1-4bbf-8921-ec7838561e50",  "extract": "string"},
            "tipo_conta":                   {"field_id": "12b170be-7975-4236-9577-5eabcc8d387d",  "extract": "select"},
            "chave_pix":                    {"field_id": "4f35d6ec-6ef6-4cb7-aa58-5915a14b2da9",  "extract": "string"},
            "endereco_logradouro":          {"field_id": "ed667b7c-9859-442f-a52f-c8d92a939bc6",  "extract": "string"},
            "endereco_numero":              {"field_id": "8f215c8c-86a8-4b26-8c1e-21e7d12aee26",  "extract": "string"},
            "endereco_complemento":         {"field_id": "f1c8c6f9-8733-4aaa-956e-42964cbc285a",  "extract": "string"},
            "endereco_bairro":              {"field_id": "2e0a0cfe-0103-402b-b03f-8300761f8227",  "extract": "string"},
            "endereco_cidade":              {"field_id": "d99d9684-2b5c-4659-bcb7-27d381452778",  "extract": "string"},
            "endereco_uf":                  {"field_id": "962745f3-40b9-4f29-8f63-6693e4722637",  "extract": "string"},
            "endereco_cep":                 {"field_id": "cee6fd29-049a-4229-bfdb-0fb7c1921ccc",  "extract": "string"},
            "contato_emergencia_nome":      {"field_id": None,                                    "extract": None},
            "contato_emergencia_parentesco":{"field_id": None,                                    "extract": None},
            "contato_emergencia_telefone":  {"field_id": None,                                    "extract": None},
            "telefone_residencial":         {"field_id": "71e906a4-13c7-41ce-91db-76e707ff043d",  "extract": "string"},
            "leader_id":                    {"field_id": "7232d8a6-9291-44b0-80f9-0dd0c5f378a8",  "extract": "link"},
        },
    },
    "dependentes": {
        "view_id": "238abb2c-00f4-48db-93d0-8180779563f9",
        "pk": "bigint",
        "columns": {
            "id":                   {"field_id": None,                                    "extract": None,         "default": "__seq__"},
            "nome":                 {"field_id": "909bf0ba-75d2-4997-8839-93a62b20d79d",  "extract": "string"},
            "colaborador":          {"field_id": "af43a475-12ac-4d4a-ad7a-ac8f13320d65",  "extract": "link"},
            "tipo_de_relacao":      {"field_id": "369fd368-2f97-42da-ba37-08010f3d3ec5",  "extract": "select"},
            "dependente_direto":    {"field_id": "c305acb4-9db4-4198-b265-72a6af467b69",  "extract": "bool_select"},
            "data_de_nascimento":   {"field_id": "e1638d32-285e-41b8-84dc-fb7ab985e9cf",  "extract": "date"},
            "created_at":           {"field_id": None,                                    "extract": None,         "default": "__timestamptz__"},
            "rg":                   {"field_id": "c9cb6995-d98b-449e-9366-9d3f4af1fa57",  "extract": "string"},
            "cpf":                  {"field_id": "8f8b078e-df05-479f-babe-322da7b5ecfd",  "extract": "string"},
            "sexo":                 {"field_id": None,                                    "extract": None},
            "possui_deficiencia":   {"field_id": "01a60c7b-fe22-42b5-b40f-312fcc53cb32",  "extract": "bool_select"},
            "declarar_ir":          {"field_id": "e7e12958-3af6-45b0-8bee-85b28dc48fab",  "extract": "bool_select"},
            "plano_saude":          {"field_id": "c053d06d-2862-418c-82ac-c951c67af9ef",  "extract": "bool_select"},
            "salario_familia":      {"field_id": None,                                    "extract": None},
            "pensao_alimenticia":   {"field_id": None,                                    "extract": None},
        },
    },
    "empresas": {
        "view_id": "d0c9bbc8-b931-4862-9d69-f149c86087ab",
        "pk": "bigint",
        "columns": {
            "id":               {"field_id": None,                                    "extract": None,       "default": "__seq__"},
            "created_at":       {"field_id": None,                                    "extract": None,       "default": "__timestamptz__"},
            "cnpj":             {"field_id": "91c33029-fb0a-409f-a658-1aa88813a7f6",  "extract": "string"},
            "razao_social":     {"field_id": "6d04196a-8067-4c62-af12-86b2a3499eab",  "extract": "string"},
            "cnae_principal":   {"field_id": "b7b3e365-5dd3-4165-bc5e-5880a47c0b88",  "extract": "string"},
            "nome_fantasia":    {"field_id": "5cb7898e-f9a2-4432-9d8a-e12fb0429906",  "extract": "string"},
            "cnae_secundario":  {"field_id": "48fb7355-8190-4f89-924c-979211898dec",  "extract": "string"},
            "data_abertura":    {"field_id": "181d8b58-6e87-42ea-b228-76802e184a19",  "extract": "date"},
            "logradouro":       {"field_id": "c3655fe4-9f82-475f-9ca9-ed80848b5879",  "extract": "string"},
            "complemento":      {"field_id": "f3baf425-dbbb-4429-ba5c-b6b43492402d",  "extract": "string"},
            "cep":              {"field_id": "01fa9380-3360-4a51-aacb-e8f47c3f8563",  "extract": "string"},
            "bairro":           {"field_id": "653d9ec6-0d26-4254-8169-1e394e39fdad",  "extract": "string"},
            "cidade":           {"field_id": "4774f6f3-c488-4881-b0f1-063fb3de1def",  "extract": "string"},
            "uf":               {"field_id": "b6876274-53f8-4cbe-aef8-5b9d4f4bc2ca",  "extract": "string"},
            "colaborador_id":   {"field_id": "b080c185-a5ed-4011-a31c-1326af28e3cd",  "extract": "link"},
            "banco_nome":       {"field_id": "e8fb62d3-513d-4080-bea2-4c7787b38437",  "extract": "string"},
            "banco_num":        {"field_id": "e2b955b4-0568-4618-8992-aa168a6d86fd",  "extract": "number"},
            "banco_tipo_conta": {"field_id": "72d8ecd2-11d6-4905-9067-ad4e3a920776",  "extract": "select"},
            "banco_agencia":    {"field_id": "9100ec4a-c7b1-4647-8e5e-8a635a889e27",  "extract": "string"},
            "banco_conta":      {"field_id": "1c5f70e8-3b69-47e4-93c0-d50768f8f81c",  "extract": "string"},
            "banco_pix":        {"field_id": "4c3ac53d-784e-44d3-8339-a01a669e388f",  "extract": "string"},
        },
    },
    "contratos": {
        "view_id": "9ec85b9d-1350-4423-be2c-5d9b8cb4cbe6",
        "pk": "bigint",
        "columns": {
            "id":               {"field_id": None,                                    "extract": None,        "default": "__seq__"},
            "created_at":       {"field_id": None,                                    "extract": None,        "default": "__timestamptz__"},
            "status":           {"field_id": "4b4e8e9f-75a0-4a58-8432-c66c54786b93",  "extract": "select"},
            "tipo_contrato":    {"field_id": "5f64f68c-4d64-4726-aa47-99da1844513c",  "extract": "select"},
            "data_inicio":      {"field_id": "9739f36f-89a4-4574-accd-c16d380ee2d3",  "extract": "date"},
            "data_fim":         {"field_id": "de45cc0d-b5c9-409c-a471-9da84fb35220",  "extract": "date"},
            "salario":          {"field_id": "4b1a3492-18cf-4a61-bc95-272025e15d38",  "extract": "number"},
            "colaborador_id":   {"field_id": "e0f9380b-832d-4c14-b21b-de22980f2171",  "extract": "link"},
            "notas":            {"field_id": "1910057a-cac0-40c1-aa56-085b3fbeb25c",  "extract": "markdown"},
            "salario_variavel": {"field_id": None,                                    "extract": None},
            "cargo":            {"field_id": "ce6362dc-0a47-43b0-b07f-09b40d451182",  "extract": "string"},
        },
    },
}


# ---------------------------------------------------------------------------
# Extractors
# ---------------------------------------------------------------------------

def extract_value(raw, strategy):
    if raw is None:
        return ""
    try:
        if strategy == "string":
            return raw.get("value", "") if isinstance(raw, dict) else str(raw)
        if strategy == "number":
            return raw.get("value", "") if isinstance(raw, dict) else raw
        if strategy == "date":
            return raw.get("value", "") if isinstance(raw, dict) else raw
        if strategy == "markdown":
            return str(raw) if raw else ""
        if strategy == "formula":
            return raw.get("value", "") if isinstance(raw, dict) else raw
        if strategy == "select":
            return raw[0].get("select_option_display_name", "") if isinstance(raw, list) and raw else ""
        if strategy == "bool_select":
            if isinstance(raw, list) and raw:
                val = raw[0].get("select_option_display_name", "").strip().lower()
                return "true" if val in ("sim", "yes", "true", "1") else "false" if val in ("não", "nao", "no", "false", "0") else ""
            return ""
        if strategy == "link":
            return raw[0].get("foreign_record_id", "") if isinstance(raw, list) and raw else ""
        if strategy == "lookup":
            if isinstance(raw, list) and raw:
                inner = raw[0].get("foreign_record_display_name")
                if isinstance(inner, list) and inner:
                    return inner[0].get("foreign_record_display_name", "")
                return inner or ""
            return ""
        if strategy == "attachment":
            return raw[0].get("filename", "") if isinstance(raw, list) and raw else ""
        if strategy == "coverimage":
            if isinstance(raw, list) and raw:
                return raw[0].get("url", raw[0].get("filename", ""))
            if isinstance(raw, dict):
                return raw.get("url", raw.get("filename", ""))
            return ""
    except Exception:
        return ""
    return ""


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


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
    args = parser.parse_args()

    token = args.token or os.environ.get("ZAZOS_TOKEN")
    if not token:
        sys.exit("Erro: informe --token ou defina ZAZOS_TOKEN.")

    os.makedirs(args.output_dir, exist_ok=True)

    with requests.Session() as session:
        session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/json"})

        for table_name, mapping in SCHEMA_MAPPING.items():
            view_id = mapping["view_id"]
            columns = mapping["columns"]
            is_bigint = mapping["pk"] == "bigint"

            print(f"\nBuscando '{table_name}' (view {view_id})...")
            view = request_with_retry(session, f"{BASE_URL}/builder/views/{view_id}")
            records = view.get("records", [])
            print(f"  {len(records)} registros encontrados.")

            out_path = os.path.join(args.output_dir, f"{table_name}.csv")
            with open(out_path, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=list(columns.keys()))
                writer.writeheader()

                for seq, record in enumerate(records, start=1):
                    record_id = record.get("id", "")
                    data = record.get("data", {})
                    row = {}

                    for col_name, col_def in columns.items():
                        default = col_def.get("default", "")

                        if default == "__record_id__":
                            row[col_name] = record_id
                            continue
                        if default == "__seq__":
                            row[col_name] = seq
                            continue
                        if default == "__timestamptz__":
                            row[col_name] = now_iso()
                            continue
                        if col_def["field_id"] is None:
                            row[col_name] = default
                            continue

                        raw = data.get(col_def["field_id"])
                        row[col_name] = extract_value(raw, col_def["extract"])

                    writer.writerow(row)

            print(f"  Salvo em {out_path}")

    print("\nConcluído.")


if __name__ == "__main__":
    main()
