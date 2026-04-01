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
        "view_id": "05e81fcf-620c-46e4-af66-e07d82d2ba85",
        "pk": "uuid",
        "columns": {
            "id":                           {"field_id": None,                                    "extract": None,         "default": "__record_id__"},
            "created_at":                   {"field_id": None,                                    "extract": None,         "default": "__timestamptz__"},
            "updated_at":                   {"field_id": None,                                    "extract": None,         "default": "__timestamptz__"},
            "email":                        {"field_id": "379bfd57-5ff6-4331-a84e-773875595493",  "extract": "string"},
            "full_name":                    {"field_id": "7d7200f3-a3a8-4809-95dd-b87263e3392e",  "extract": "string"},
            "avatar_url":                   {"field_id": "bd7ba7b2-28fd-4115-999b-cee2be2bf7ad",  "extract": "coverimage"},
            "n1_id":                        {"field_id": None,                                    "extract": None},
            "n2_id":                        {"field_id": None,                                    "extract": None},
            "n3_id":                        {"field_id": None,                                    "extract": None},
            "cnh_categoria":                {"field_id": "a7188063-3cd6-48f7-8058-bc24c89a22ff",  "extract": "string"},
            "cnh_num":                      {"field_id": "f81f3832-2643-41c2-9242-1da968ae4109",  "extract": "string"},
            "ctps_emissao":                 {"field_id": "0d66349f-edc0-4de7-b530-be792aab28d6",  "extract": "date"},
            "ctps_estado":                  {"field_id": "f1181a04-b49d-4f78-918a-f3f48683e40f",  "extract": "string"},
            "ctps_num":                     {"field_id": "ee6fe15b-c628-4c73-81b3-b1e476465716",  "extract": "string"},
            "ctps_serie":                   {"field_id": "e25dc355-252b-4172-9e53-566f70b20ae6",  "extract": "string"},
            "doc_militar_num":              {"field_id": "fb27b9fc-716c-4eea-b053-927ad660fee3",  "extract": "string"},
            "pis_num":                      {"field_id": "28346852-0eb0-4c35-9bd0-68adaa26b19e",  "extract": "string"},
            "titulo_eleitor_num":           {"field_id": "2b1bd869-5839-4439-998e-c985a363b9e1",  "extract": "string"},
            "titulo_eleitor_secao":         {"field_id": "a2d10b26-7e0c-4084-981f-4e0ed251e6a2",  "extract": "string"},
            "titulo_eleitor_zona":          {"field_id": "58895d58-ac91-43d7-932d-6e6e61874360",  "extract": "string"},
            "cpf":                          {"field_id": "4333ed3b-56ea-48ac-806d-949fd9492070",  "extract": "string"},
            "genero":                       {"field_id": "a754b998-fcb2-4fd9-9dd9-52d6270f40ce",  "extract": "select"},
            "estado_civil":                 {"field_id": "78393797-10cb-481d-a361-70df3a372896",  "extract": "select"},
            "nome_mae":                     {"field_id": "31c2ff23-710f-4408-a2eb-c47a1cb3a890",  "extract": "string"},
            "nome_pai":                     {"field_id": "d535e3b2-0540-47e6-8ae7-06716e2105db",  "extract": "string"},
            "raca_etnia":                   {"field_id": "307d8fe2-3499-498b-a6ac-02b378618177",  "extract": "select"},
            "pcd":                          {"field_id": "8205d406-bd3e-4f35-a467-fea5cbcda758",  "extract": "bool_select"},
            "pcd_tipo":                     {"field_id": None,                                    "extract": None},
            "pcd_observacoes":              {"field_id": "f0c44507-757c-4391-a591-acf5915083b0",  "extract": "string"},
            "rg_numero":                    {"field_id": "3f79ec5d-c270-4c3b-b8b9-f4dae8d21078",  "extract": "string"},
            "rg_orgao_emissor":             {"field_id": "de7c57e2-d335-478c-bb44-c00d3477c027",  "extract": "string"},
            "rg_uf":                        {"field_id": "4ec30e4b-2e49-40d6-847c-e31ed3799b41",  "extract": "string"},
            "rg_data_emissao":              {"field_id": "bfdd0824-2f9a-490a-8b58-2c91851c20c8",  "extract": "date"},
            "formacao_instituicao":         {"field_id": "8dd6b1d1-2706-455e-810f-bdf8992155fa",  "extract": "string"},
            "formacao_curso":               {"field_id": "8631cdff-4cf3-466e-baed-be07df8dcfaa",  "extract": "string"},
            "formacao_ano_conclusao":       {"field_id": "5c8542d0-254f-4888-a86d-4265bbc149e0",  "extract": "number"},
            "banco":                        {"field_id": "2cb3a587-209c-4016-9119-f1474ae4e733",  "extract": "string"},
            "agencia":                      {"field_id": "e7ce11e6-781f-4626-a0eb-e9e0ab315302",  "extract": "string"},
            "numero_conta":                 {"field_id": "9ef17ec9-9b39-46f2-854c-3d2a9528505f",  "extract": "string"},
            "tipo_conta":                   {"field_id": "54f3e932-bc91-46f7-9d00-d46dc6ac655d",  "extract": "select"},
            "chave_pix":                    {"field_id": "ee3a7346-146f-48ac-bb68-be8c821b9fe6",  "extract": "string"},
            "endereco_logradouro":          {"field_id": "6814d0af-827f-4ade-b070-8a6b19a97b88",  "extract": "string"},
            "endereco_numero":              {"field_id": "1414941f-75ac-46be-9255-466f82db5e10",  "extract": "string"},
            "endereco_complemento":         {"field_id": "be68c1dd-90c9-4ff7-a8bf-bcec06e537ae",  "extract": "string"},
            "endereco_bairro":              {"field_id": "f5f31a85-e2c4-4e81-86be-b3e4bd1493dc",  "extract": "string"},
            "endereco_cidade":              {"field_id": "0a5d9aa9-1f4c-48ba-89f1-3d7fae87d9a0",  "extract": "string"},
            "endereco_uf":                  {"field_id": "4769dcc9-e4b5-49db-a434-3defe859f83c",  "extract": "select"},
            "endereco_cep":                 {"field_id": "384424a1-6507-4760-b9d8-7e033cea68a2",  "extract": "string"},
            "contato_emergencia_nome":      {"field_id": "d560c772-2f6e-467b-9d8c-e669f43aba56",  "extract": "string"},
            "contato_emergencia_parentesco":{"field_id": "9e7bd90d-1cb7-4caa-89b5-d435e6c66244",  "extract": "string"},
            "contato_emergencia_telefone":  {"field_id": "a695db9c-6c37-458e-8e17-94e6d1da2019",  "extract": "string"},
            "telefone_residencial":         {"field_id": "99f6dfe7-fd02-4e19-9675-700d5e9af8b5",  "extract": "string"},
            "leader_id":                    {"field_id": "53b89e19-86f8-4de6-9d86-f9386206d148",  "extract": "link"},
        },
    },
    "dependentes": {
        "view_id": "4d4962e0-53ac-425f-b9c7-9b95be6d5f85",
        "pk": "bigint",
        "columns": {
            "id":                   {"field_id": None,                                    "extract": None,         "default": "__seq__"},
            "nome":                 {"field_id": "9a4d9fac-898b-4e4c-a1c7-2fbc7e80c7be",  "extract": "string"},
            "colaborador":          {"field_id": "f148fa15-f6e1-4de9-bf26-5b09fe625d33",  "extract": "link"},
            "tipo_de_relacao":      {"field_id": "aa3d6cec-67aa-4173-a4c2-f334e6f91800",  "extract": "select"},
            "dependente_direto":    {"field_id": "b0c003d8-77dd-4cc7-952c-48fb8cb0c5ca",  "extract": "select"},
            "data_de_nascimento":   {"field_id": "a3814a31-3c22-4ed4-b166-01a2b1664708",  "extract": "date"},
            "created_at":           {"field_id": None,                                    "extract": None,         "default": "__timestamptz__"},
            "rg":                   {"field_id": "fcce64a3-0040-4525-8bd1-d4da87e5ca99",  "extract": "string"},
            "cpf":                  {"field_id": "b2275c7e-f660-4914-abe1-962a0f9b293d",  "extract": "string"},
            "sexo":                 {"field_id": None,                                    "extract": None},
            "possui_deficiencia":   {"field_id": None,                                    "extract": None},
            "declarar_ir":          {"field_id": None,                                    "extract": None},
            "plano_saude":          {"field_id": None,                                    "extract": None},
            "salario_familia":      {"field_id": None,                                    "extract": None},
            "pensao_alimenticia":   {"field_id": None,                                    "extract": None},
        },
    },
    "empresas": {
        "view_id": "fa29b894-25ed-48f7-b1dd-a635ce3b4211",
        "pk": "bigint",
        "columns": {
            "id":               {"field_id": None,                                    "extract": None,       "default": "__seq__"},
            "created_at":       {"field_id": None,                                    "extract": None,       "default": "__timestamptz__"},
            "cnpj":             {"field_id": "453f4349-2ef5-4310-b9e4-aaa830d21ad1",  "extract": "string"},
            "razao_social":     {"field_id": "5ed26893-ebd4-40e9-b188-97d86c56dcdb",  "extract": "string"},
            "cnae_principal":   {"field_id": "2bce9f9c-b5c4-4f15-84b7-ba67ea00860e",  "extract": "string"},
            "nome_fantasia":    {"field_id": "54e1afc6-8874-4f18-bdcf-1b43ec8de78a",  "extract": "string"},
            "cnae_secundario":  {"field_id": "0546afc0-e285-49ce-bb62-fa91286e3884",  "extract": "string"},
            "data_abertura":    {"field_id": "7a8f50e6-55eb-4704-9481-f9fa6ed15742",  "extract": "date"},
            "logradouro":       {"field_id": "b7a43728-f27b-4bf4-890c-135747a91d49",  "extract": "string"},
            "complemento":      {"field_id": "efcb80f6-82bb-4a5d-bebd-77f690adc905",  "extract": "string"},
            "cep":              {"field_id": "0a8cbcab-b1da-48a3-ab94-5d5d63c01e02",  "extract": "string"},
            "bairro":           {"field_id": "48709455-3823-45ae-a80e-3bb59c50c304",  "extract": "string"},
            "cidade":           {"field_id": "a65ae96e-3a7c-4a47-81fa-a7b6c8743127",  "extract": "string"},
            "uf":               {"field_id": "0872237d-3f40-45fb-bfaa-47cd2257e5f7",  "extract": "string"},
            "colaborador_id":   {"field_id": "babe6104-78d9-4152-b096-9a3e10a92e7f",  "extract": "link"},
            "banco_nome":       {"field_id": "439b2a59-ea67-4758-b2a6-35b85f175f15",  "extract": "string"},
            "banco_num":        {"field_id": "d58fa49b-786c-4002-9149-3a4215294af8",  "extract": "number"},
            "banco_tipo_conta": {"field_id": "8a5703b6-93d1-465b-8ded-f318dbcee6a4",  "extract": "select"},
            "banco_agencia":    {"field_id": "335d24fd-f137-492d-b3a6-d5b701f6e51e",  "extract": "string"},
            "banco_conta":      {"field_id": "60841211-ca0b-40b6-b150-6d48c37d9245",  "extract": "string"},
            "banco_pix":        {"field_id": "ed70ddc2-b3a2-4567-a0d7-35a3cc1b8b39",  "extract": "string"},
        },
    },
    "contratos": {
        "view_id": "8790fb58-511b-45dc-8a21-8d9f3270cf04",
        "pk": "bigint",
        "columns": {
            "id":               {"field_id": None,                                    "extract": None,        "default": "__seq__"},
            "created_at":       {"field_id": None,                                    "extract": None,        "default": "__timestamptz__"},
            "status":           {"field_id": "58f228c4-88cf-4753-8028-35e8d55efb76",  "extract": "select"},
            "tipo_contrato":    {"field_id": "13d12872-a518-44be-a655-48409455e3e8",  "extract": "select"},
            "data_inicio":      {"field_id": "040f6b22-f1d4-4952-bf61-6d7279d464e8",  "extract": "date"},
            "data_fim":         {"field_id": "7aa164a0-9849-45db-96e4-8e96084c46ca",  "extract": "date"},
            "salario":          {"field_id": "a71e0222-ea84-4d3b-92fe-719b35c8d934",  "extract": "number"},
            "colaborador_id":   {"field_id": "719ace5f-aa64-43c3-8c9b-0a47b457df4f",  "extract": "link"},
            "notas":            {"field_id": "43afd11d-68db-47da-9411-cf6c514cfddc",  "extract": "markdown"},
            "salario_variavel": {"field_id": None,                                    "extract": None},
            "cargo":            {"field_id": None,                                    "extract": None},
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
