# Runbook: Migração Zazos → Supabase

Guia passo a passo para migrar dados de um workspace Zazos para o Supabase.

---

## Pré-requisitos

- Python 3.9+
- Dependências: `pip install requests`
- Acesso ao workspace Zazos (conta com permissão de leitura)
- Acesso ao projeto Supabase de destino

---

## Passo 1 — Obter o token do Zazos

1. Abra o Zazos no Chrome e faça login
2. Abra o DevTools → aba **Network**
3. Recarregue a página
4. Clique em qualquer requisição GET para `aws-production-api.zazos.com`
5. Em **Request Headers**, copie o valor de `authorization` (começa com `Bearer eyJ...`)

> O token expira. Se der erro 401 ao rodar os scripts, repita este passo.

---

## Passo 2 — Buscar todos os sheets do workspace

```bash
ZAZOS_TOKEN="Bearer eyJ..." python3 fetch_sheets.py \
  --output output/<cliente>/sheets_default_views.json
```

Isso gera um JSON com todos os sheets, seus campos e IDs de view padrão.
Guarde esse arquivo — ele é a referência para o mapeamento.

---

## Passo 3 — Identificar os sheets relevantes para o módulo

Abra o `sheets_default_views.json` e localize os sheets que correspondem ao app
que você quer migrar. Use o `system_name` como guia:

| App novo (Supabase) | Sheet(s) Zazos típicos |
|---|---|
| pessoas / z_shared | `people`, `contracts`, `companies`, `other_people` |
| admissao / z_admissao | `onboarding` |
| pagamentos / z_financeiro | `payments` |
| ferias / z_recessos | `paid_time_off`, `paid_time_off_periodos_aquisitivos_sheet` |

Anote o `default_view_id` de cada sheet relevante.

---

## Passo 4 — Entender o schema de destino

Cada módulo tem um `schema.json` em `output/<cliente>/<modulo>/schema.json`.

Para cada tabela no schema, você precisa identificar qual sheet Zazos tem os dados
e qual `field_id` corresponde a cada coluna.

**Como encontrar o field_id certo:**

No `sheets_default_views.json`, cada sheet tem uma lista de `fields`:
```json
{ "id": "abc123", "type": "String", "name": "Nome Completo" }
```

O `id` aqui é o `field_id` que vai no script.

**Estratégias de extração por tipo de campo Zazos:**

| Tipo Zazos | Estratégia no script |
|---|---|
| String | `"string"` |
| Number | `"number"` |
| Date | `"date"` |
| Select | `"select"` |
| Link (FK simples) | `"link"` → retorna o `foreign_record_id` |
| Link (FK múltipla) | `"link_array"` → retorna JSON array de IDs |
| Formula | `"formula"` |
| Lookup | `"string"` ou `"formula"` |
| Attachment | `"attachment"` → retorna o filename |
| Markdown | `"markdown"` |

---

## Passo 5 — Criar o script `generate_csvs_<modulo>.py`

Use um dos scripts existentes como template (ex: `generate_csvs_admissao.py`).

A estrutura é sempre a mesma:

```python
VIEW_X = "<default_view_id do sheet>"

TABELA_COLUMNS = {
    "coluna_destino": {
        "field_id": "<field_id do Zazos>",  # None se não tiver equivalente
        "extract": "<estratégia>",
        "default": "<valor padrão>"          # opcional
    },
    ...
}
```

**Defaults especiais:**
- `"__record_id__"` → usa o ID do registro Zazos como UUID
- `"__timestamptz__"` → usa o timestamp atual (ISO 8601)

**UUID remapping** (quando pessoa_id precisa bater com o Supabase):
Passe `--supabase-profiles` e `--zazos-profiles` e use `uuid_map` para remapear FKs.

---

## Passo 6 — Rodar o script

```bash
ZAZOS_TOKEN="Bearer eyJ..." python3 generate_csvs_<modulo>.py \
  --output-dir output/<cliente>/<modulo>
```

Verifique os arquivos gerados em `output/<cliente>/<modulo>/`.

---

## Passo 7 — Importar no Supabase

**Ordem de importação** (respeitar FKs):

1. Tabelas sem dependências (ex: `profiles`, `pessoa`, `competencias`)
2. Tabelas que dependem das anteriores (ex: `contratos` → depende de `pessoas`)
3. Tabelas de última ordem (ex: `ausencia_p_j` → depende de `periodo_aquisitivo_p_j`)

No Supabase: **Table Editor → [tabela] → Insert → Import data from CSV**

---

## Passo 8 — Erros comuns e como resolver

### Duplicate key (unique constraint)
```
ERROR: 23505: duplicate key value violates unique constraint "..."
```
**Causa:** Após remap de UUID, dois registros Zazos viraram o mesmo no Supabase.
**Fix:** Deduplicar o CSV pela chave composta afetada:
```python
seen = set()
deduped = [r for r in rows if (r['col_a'], r['col_b']) not in seen
           and not seen.add((r['col_a'], r['col_b']))]
```

### Foreign key violation
```
ERROR: 23503: insert or update violates foreign key constraint "..."
DETAIL: Key (x_id)=(abc) is not present in table "y"
```
**Causa:** O registro referenciado não existe na tabela de destino.
**Fix:** Nulificar os IDs órfãos:
```python
valid = {r['id'] for r in csv.DictReader(open('tabela_pai.csv'))}
for r in rows:
    if r['fk_col'] not in valid:
        r['fk_col'] = ''
```

### Check constraint violation
```
ERROR: 23514: new row violates check constraint "tabela_coluna_check"
```
**Causa:** Valor do CSV não está entre os permitidos pela constraint.
**Fix:** Consulte a constraint no Supabase:
```sql
SELECT pg_get_constraintdef(c.oid)
FROM pg_constraint c
JOIN pg_class t ON c.conrelid = t.oid
JOIN pg_namespace n ON t.relnamespace = n.oid
WHERE n.nspname = '<schema>' AND t.relname = '<tabela>'
  AND c.conname = '<nome_da_constraint>';
```
Depois mapeie os valores do CSV para os valores aceitos.

### FK para `public.profiles` (auth users)
Alguns módulos têm FKs para `public.profiles` (usuários autenticados do Supabase).
Esses IDs **não** existem automaticamente — nulificar se o UUID não estiver na lista
de usuários existentes.

---

## Referência rápida

```
traduction/
├── fetch_sheets.py                  # Passo 2: busca todos os sheets
├── generate_csvs.py                 # Template: módulo despesas
├── generate_csvs_pessoas.py         # Template: módulo pessoas
├── generate_csvs_pagamentos.py      # Template: módulo pagamentos
├── generate_csvs_admissao.py        # Template: módulo admissão
├── generate_csvs_ferias.py          # Template: módulo férias/ausências
├── output/
│   ├── lemon/                       # Workspace Lemon
│   │   ├── sheets_default_views.json
│   │   └── <modulo>/
│   │       ├── schema.json          # Schema de destino
│   │       └── *.csv                # CSVs gerados
│   └── ngcash/                      # Workspace NgCash
│       └── sheets_default_views.json
└── MIGRATION_RUNBOOK.md             # Este arquivo
```
