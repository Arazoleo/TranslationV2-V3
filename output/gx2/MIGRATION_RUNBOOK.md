# Migration Runbook — GX2 (Zazos → Supabase)

> Última atualização: 2026-04-01
> Ferramenta: `generate_csvs_generic.py --mapping <path> --token <token>`
> Token GX2: ver `.env` ou histórico de sessão

---

## Visão Geral

| # | Módulo | Schema Supabase | Status | Tabelas importadas |
|---|--------|----------------|--------|--------------------|
| 1 | pessoas | z_shared | ✅ Concluído | profiles, cargos, empresas, contratos, dependentes |
| 2 | pagamentos | z_pagamentospj | ✅ Concluído | profiles, competencias, pagamentos_p_j |
| 3 | movimentacoes | z_movimentacoes | ✅ Concluído | profiles, movimentacao |
| 4 | gestao_de_beneficios | z_beneficios | ✅ Concluído | beneficio, adesao_beneficio |
| 5 | despesas | lemonenergy_despesas | ✅ Concluído | expenses |
| 6 | desligamentos | z_desligamento | ✅ Concluído | profiles, desligamentos |
| 7 | admissao | z_admissao | ✅ Concluído | profiles, admissao |
| 8 | ferias_e_ausencias | z_recessos | ✅ Concluído | pessoa, periodo_aquisitivo, solicitacao_ferias, feriados_nacionais |

---

## Módulo 1 — pessoas (`z_shared`)

**Sheets:** Pessoas (288), Cargos 197, Empresas (108), Contratos (629), Dependentes e Conexões (220)
**Mapping:** `output/gx2/pessoas/mapping.json`

**Import order:** `profiles` → `cargos` → `empresas` → `contratos` → `dependentes`

**Post-processing aplicado:**
- `empresas`: dedup por `colaborador_id` (4 duplicatas → 104 linhas)
- `contratos.status`: `'Ativo'` → `'ativo'`, `'Não ativo'` → `'inativo'`

**PKs:** profiles usa `__record_id__` (UUID); cargos, empresas, contratos, dependentes usam `__seq__` (bigint sequencial)

---

## Módulo 2 — pagamentos (`z_pagamentospj`)

**Sheets:** Pessoas (288), Folha (20), Pagamentos (1118)
**Mapping:** `output/gx2/pagamentos/mapping.json`

**Import order:** `profiles` → `competencias` → `pagamentos_p_j`

**Post-processing aplicado:**
- `competencias.csv`: gerado a partir de `folha_raw.csv` — extrai mês/ano únicos das datas, atribui novos UUIDs (16 competências, 2024-11 a 2026-02)
- `pagamentos_p_j.csv`: filtrado para apenas linhas PJ (597 linhas), coluna `tipo_contrato` removida

---

## Módulo 3 — movimentacoes (`z_movimentacoes`)

**Sheets:** Pessoas (288), Movimentações (51)
**Mapping:** `output/gx2/movimentacoes/mapping.json`

**Import order:** `profiles` → `movimentacao`

**Post-processing aplicado:**
- `tipo_movimentacao`: mapeado de valores truncados para strings completas do constraint
- `tipo_contrato_anterior/novo`: valores `'Equity'` e `'Coop'` → NULL (fora do constraint)

**FKs nulas:** cargo_anterior_id, cargo_novo_id, marca, departamento, unidade_negocio, empresa_contratante, manager_anterior_id, manager_novo_id

---

## Módulo 4 — gestao_de_beneficios (`z_beneficios`)

**Sheets:** Benefícios (10), Adesão a benefícios (220)
**Mapping:** `output/gx2/gestao_de_beneficios/mapping.json`

**Import order:** `beneficio` → `adesao_beneficio`

**Post-processing aplicado:**
- `beneficio.tipo`: mapeado de nomes Zazos para slugs do constraint (`vale_transporte`, `saude`, etc.)
- `beneficio.status`: normalizado para lowercase

---

## Módulo 5 — despesas (`lemonenergy_despesas`)

**Sheets:** Despesas e Reembolsos (108)
**Mapping:** `output/gx2/despesas/mapping.json`

**Import order:** `expenses`

**Post-processing:** coluna `expense_category` removida (não existe na tabela real)

---

## Módulo 6 — desligamentos (`z_desligamento`)

**Sheets:** Pessoas (288), Offboarding (165)
**Mapping:** `output/gx2/desligamentos/mapping.json`

**Import order:** `profiles` → `desligamentos`

**Post-processing aplicado:**
- `tipo_desligamento`: `'Término de Contrato'` → `'Término de contrato'` (lowercase c)

**Sheets ignoradas:** "Desligamentos antigos" (432 records) — apenas campos string, sem links para profiles

---

## Módulo 7 — admissao (`z_admissao`)

**Sheets:** Pessoas (288), Admissão (131)
**Mapping:** `output/gx2/admissao/mapping.json`

**Import order:** `profiles` → `admissao`

**Notas:** `tipo_contrato` inclui `'Cooperado'` — verificar se constraint aceita; demais campos (tipo_admissao_id, n1/n2/n3, cargo_id, beneficios_ids, etapa_atual_id, gestor_id) → NULL

---

## Módulo 8 — ferias_e_ausencias (`z_recessos`)

**Sheets:** Pessoas (288), Período aquisitivo (90), Férias (252), Feriados (11)
**Mapping:** `output/gx2/ferias_e_ausencias/mapping.json`

**Import order:** `pessoa` → `periodo_aquisitivo` → `solicitacao_ferias` → `feriados_nacionais`

**Post-processing aplicado:**
- `pessoa.genero`: `'Homem Cis'`→`'Masculino'`, `'Mulher Cis'`→`'Feminino'`, `'Prefiro não declarar'`→`'Prefiro não informar'`
- `pessoa.status`: `'Desligado'`→`'Inativo'`, `'Externo'`→`'Inativo'`
- `pessoa.estado_civil`: normalizado para sufixo `(a)` conforme constraint
- `pessoa.endereco_uf`: trim de espaços; valores inválidos (`Florida`, `Goiás`, etc.) → NULL
- `periodo_aquisitivo.situacao_periodo`: `'Não Ativo'`→`'Historico'`
- `periodo_aquisitivo`: dedup por `data_inicio_aquisitivo` (90 → 78 linhas)
- `solicitacao_ferias.status`: mapeado para slugs do constraint (`Usufruida`, `Em_Analise`, etc.)
- `solicitacao_ferias.tipo_solicitacao`: `'Pausa Programada'`→`'Recesso'`
- `solicitacao_ferias.periodo_aquisitivo_id`: 1 FK órfã → NULL
- `feriados_nacionais.ano`: derivado da coluna `data` (year extract)

**Tabelas não migradas:** atestados, ausencia_p_j, periodo_aquisitivo_p_j, contrato, configs
**Pendências:** ver PENDENCIAS.md seções 4 e 5

---

## Notas Gerais

- `uuid_remap: false` em todos os módulos — instância Supabase nova, sem remapeamento necessário
- Colunas prefixadas com `_` são ignoradas pelo gerador (metadados)
- FK para `public.profiles` → NULL em todos os módulos até setup de auth
- Sentinelas disponíveis: `__record_id__` (UUID PK), `__seq__` (bigint PK sequencial), `__timestamptz__`
