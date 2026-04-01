#!/usr/bin/env python3
"""
Gera um mapping.json com sugestões de mapeamento entre schema.json (Supabase)
e sheets_default_views.json (Zazos), usando similaridade de nomes.

O arquivo gerado deve ser revisado e editado antes de usar no generate_csvs_generic.py.

Usage:
    python suggest_mapping.py \
        --schema output/<cliente>/<modulo>/schema.json \
        --sheets output/<cliente>/sheets_default_views.json \
        --output output/<cliente>/<modulo>/mapping.json
"""

import argparse
import json
import re
import unicodedata


# ---------------------------------------------------------------------------
# Normalização para comparação de nomes
# ---------------------------------------------------------------------------

def normalize(text):
    """Remove acentos, lowercase, troca _ e - por espaço."""
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower().replace("_", " ").replace("-", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def score(col_name, field_name):
    """Score de similaridade simples entre nome de coluna e nome de campo."""
    a = normalize(col_name)
    b = normalize(field_name)
    if a == b:
        return 100
    if a in b or b in a:
        return 80
    # Palavras em comum
    words_a = set(a.split())
    words_b = set(b.split())
    common = words_a & words_b
    if common:
        return 50 + 10 * len(common)
    return 0


# ---------------------------------------------------------------------------
# Inferência de estratégia de extração pelo tipo do campo Zazos
# ---------------------------------------------------------------------------

ZAZOS_TYPE_TO_EXTRACT = {
    "String":          "string",
    "Number":          "number",
    "Date":            "date",
    "Select":          "select",
    "MultipleSelect":  "select",
    "Link":            "link",
    "Lookup":          "string",
    "Formula":         "formula",
    "Attachment":      "attachment",
    "Markdown":        "markdown",
    "Checklist":       None,
    "CoverImage":      None,
    "User":            None,
}

# Quando a coluna schema é uuid e o tipo Zazos é Link → FK
def infer_extract(schema_col_type, zazos_type, col_name):
    if zazos_type == "Link":
        # link_array se o nome sugere múltiplos
        if any(k in col_name.lower() for k in ["_ids", "s_id", "array", "lista"]):
            return "link_array"
        return "link"
    return ZAZOS_TYPE_TO_EXTRACT.get(zazos_type, "string")


# ---------------------------------------------------------------------------
# Busca do melhor campo Zazos para cada coluna do schema
# ---------------------------------------------------------------------------

def find_best_field(col_name, schema_col, all_fields):
    """
    Retorna o melhor campo Zazos para a coluna do schema.
    all_fields: lista de {"sheet_name", "field_id", "field_name", "field_type"}
    """
    # Colunas especiais — sem mapeamento
    if col_name in ("id", "created_at", "updated_at"):
        return None

    best = None
    best_score = 0

    for f in all_fields:
        s = score(col_name, f["field_name"])
        if s > best_score:
            best_score = s
            best = f

    if best and best_score >= 50:
        return best, best_score
    return None, 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema",  required=True)
    parser.add_argument("--sheets",  required=True)
    parser.add_argument("--output",  required=True)
    args = parser.parse_args()

    with open(args.schema) as f:
        schema = json.load(f)
    with open(args.sheets) as f:
        sheets = json.load(f)

    # Indexar todos os campos de todos os sheets
    all_fields = []
    for sheet in sheets:
        for field in sheet.get("fields", []):
            all_fields.append({
                "sheet_name":    sheet["sheet_name"],
                "sheet_id":      sheet["sheet_id"],
                "view_id":       sheet["default_view_id"],
                "records_count": sheet["records_count"],
                "field_id":      field["id"],
                "field_name":    field["name"],
                "field_type":    field["type"],
            })

    mapping = {
        "output_dir": f"output/<cliente>/{schema.get('name', 'modulo')}",
        "uuid_remap": {
            "enabled": False,
            "_comment": "Se True, remapeia UUIDs de pessoa_id via email. Preencha os paths abaixo.",
            "supabase_profiles": "output/<cliente>/pessoas/profiles_rows.csv",
            "zazos_profiles":    "output/<cliente>/pessoas/profiles.csv"
        },
        "tables": []
    }

    for table_name, table_def in schema.get("tables", {}).items():
        columns_def = table_def.get("columns", {})

        # Tentar descobrir qual sheet melhor corresponde a esta tabela
        table_candidates = {}
        for col_name, col_def in columns_def.items():
            if col_name in ("id", "created_at", "updated_at"):
                continue
            best, s = find_best_field(col_name, col_def, all_fields)
            if best:
                key = best["view_id"]
                table_candidates[key] = table_candidates.get(key, 0) + s

        best_view_id = max(table_candidates, key=table_candidates.get) if table_candidates else None
        best_sheet = next((s for s in sheets if s["default_view_id"] == best_view_id), None)

        # Campos do sheet vencedor (para sugestões mais precisas)
        sheet_fields = []
        if best_sheet:
            for field in best_sheet.get("fields", []):
                sheet_fields.append({
                    "sheet_name":    best_sheet["sheet_name"],
                    "sheet_id":      best_sheet["sheet_id"],
                    "view_id":       best_sheet["default_view_id"],
                    "records_count": best_sheet["records_count"],
                    "field_id":      field["id"],
                    "field_name":    field["name"],
                    "field_type":    field["type"],
                })

        # Montar colunas
        columns_out = {}
        for col_name, col_def in columns_def.items():
            col_type = col_def.get("type", "text")

            if col_name == "id":
                columns_out[col_name] = {
                    "default": "__record_id__",
                    "_comment": "UUID do registro Zazos. uuid_remap:true para remapear via email."
                }
                continue
            if col_name in ("created_at", "updated_at"):
                columns_out[col_name] = {"default": "__timestamptz__"}
                continue

            # Buscar no sheet vencedor, fallback para todos
            best, s = find_best_field(col_name, col_def, sheet_fields or all_fields)

            if best and s >= 50:
                extract = infer_extract(col_type, best["field_type"], col_name)
                entry = {
                    "field_id": best["field_id"],
                    "extract":  extract,
                    "_suggestion": f"{best['field_name']} ({best['field_type']}) — score {s}"
                }
                if col_type == "uuid" and best["field_type"] == "Link":
                    entry["uuid_remap"] = False
                    entry["_comment"] = "Setar uuid_remap:true se esta FK aponta para pessoa/profile"
            else:
                entry = {
                    "field_id": None,
                    "extract":  None,
                    "default":  "",
                    "_suggestion": "NAO ENCONTRADO — preencher manualmente"
                }

            columns_out[col_name] = entry

        table_entry = {
            "name":    table_name,
            "view_id": best_view_id or "PREENCHER",
            "_sheet":  best_sheet["sheet_name"] if best_sheet else "NAO IDENTIFICADO",
            "_records": best_sheet["records_count"] if best_sheet else 0,
            "columns": columns_out,
        }

        mapping["tables"].append(table_entry)
        print(f"  {table_name:30} → {best_sheet['sheet_name'] if best_sheet else 'NAO IDENTIFICADO'}")

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)

    print(f"\nMapping gerado em: {args.output}")
    print("Revise o arquivo, corrija os campos com '_suggestion: NAO ENCONTRADO'")
    print("e remova as chaves '_comment' e '_suggestion' antes de rodar o generate_csvs_generic.py")


if __name__ == "__main__":
    main()
