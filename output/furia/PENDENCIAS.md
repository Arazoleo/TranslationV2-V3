# Pendências — Furia

Itens que não puderam ser importados durante a migração e requerem ação manual posterior.

---

## gestao_de_beneficios

### z_beneficios.profiles — FK para public.profiles (auth users)

**Problema:** `z_beneficios.profiles.id` tem FK para `public.profiles.id` (tabela de usuários autenticados do Supabase). A tabela `public.profiles` ainda está vazia — os usuários de auth não foram criados.

**Arquivos pendentes:**
- `output/furia/gestao_de_beneficios/profiles.csv` → `z_beneficios.profiles`
- `output/furia/gestao_de_beneficios/adesao_beneficio.csv` → `z_beneficios.adesao_beneficio` (depende de profiles via `pessoa_id`)
- `output/furia/gestao_de_beneficios/update_adesao_data_adesao.sql` — rodar após importar adesao_beneficio

**Fix:** Após configurar os usuários de auth (Supabase Auth), importar na ordem:
1. `profiles.csv` → `z_beneficios.profiles`
2. `adesao_beneficio.csv` → `z_beneficios.adesao_beneficio`
3. Rodar `update_adesao_data_adesao.sql`

---

## pessoas

### z_shared.profiles — leader_id (FK para public.profiles)

**Problema:** `leader_id` referencia `public.profiles.id`. Coluna foi removida do CSV de importação pois todos os valores estavam vazios. Pode ser populada via SQL após setup de auth se necessário.

### z_shared.profiles — n1_id, n2_id, n3_id (bigint)

**Problema:** Colunas sem dados no Zazos. Foram removidas do CSV. Podem ser populadas manualmente ou via SQL após importação das tabelas `n1`, `n2`, `n3`.

---

## pagamentos

### z_pagamentospj.pagamentos_p_j — valor_mensal e total

**Problema:** Colunas numéricas removidas do CSV por conterem valores vazios misturados com válidos.

**Fix:** Rodar após importação:
- `output/furia/pagamentos/update_pagamentos_valor_mensal.sql` (3066 UPDATEs)
- `output/furia/pagamentos/update_pagamentos_total.sql` (3073 UPDATEs)

### z_pagamentospj.profiles — sem FK para public.profiles

Sem bloqueio — esta tabela não referencia public.profiles, pode ser importada a qualquer momento.

---

## ferias_e_ausencias

### z_recessos — colunas removidas do CSV (UPDATE SQL pendente)

Rodar após importação:
- `update_pessoa_estado_civil.sql` — 338 UPDATEs
- `update_pessoa_raca_etnia.sql` — 340 UPDATEs
- `update_pessoa_endereco_uf.sql` — 224 UPDATEs
- `update_pessoa_tipo_conta.sql` — 309 UPDATEs
- `update_contrato_salario.sql` — 208 UPDATEs
- `update_contrato_status.sql` — 1174 UPDATEs
- `update_ausencia_duracao.sql` — 72 UPDATEs
- `update_ausencia_periodo_id.sql` — 180 UPDATEs (filtrado; 361 FKs órfãs descartadas pelo dedup de periodo_aquisitivo_p_j)

### z_recessos — tabelas não migradas

- `solicitacao_ferias` — sem sheet CLT no Zazos
- `periodo_aquisitivo` — sem sheet CLT no Zazos
- `atestados` — campos obrigatórios de arquivo (arquivo_path, arquivo_nome) sem equivalente no Zazos
- `feriados_nacionais` — constraint unique em `ano` inviabiliza importação (todos os registros são de 2025)
- `configs` — não migrar

### z_recessos.periodo_aquisitivo_p_j — dedup agressivo

398 registros deduplicados para 103 (unique em pessoa_id + data_inicio_aquisitivo). 295 períodos descartados — ausências que referenciavam esses períodos ficaram sem periodo_aquisitivo_id.

---

## desligamentos

### z_desligamento — dados extraídos do sheet Pessoas

Os dados de desligamento estão no sheet **Pessoas** (não no sheet Offboarding que tem 0 registros).
356 registros com `data_desligamento` preenchida foram extraídos.

**Arquivos gerados:**
- `output/furia/desligamentos/profiles.csv` → `z_desligamento.profiles` (537 rows, sem FK bloqueante)
- `output/furia/desligamentos/desligamentos.csv` → `z_desligamento.desligamentos` (356 rows)

**BLOQUEADO — FK para public.profiles:** `desligamentos_pessoa_id_fkey` referencia `public.profiles` (auth users), que ainda está vazia. Mesmo bloqueio do z_beneficios.

**Fix:** Após configurar os usuários de auth (Supabase Auth), importar na ordem:
1. `profiles.csv` → `z_desligamento.profiles` ✅ (já importado)
2. `desligamentos.csv` → `z_desligamento.desligamentos`
3. Rodar `update_tipo_desligamento.sql` (353 UPDATEs)

**Observações:**
- `desligamentos.id` = `desligamentos.pessoa_id` = UUID do registro Pessoas (relação 1:1)
- `tipo_desligamento` mapeado: Involuntário→`Involuntário (sem justa causa)`, Voluntário→`Voluntário`, Acordo→`Acordo`; 3 registros sem tipo ficam NULL
- `comentarios` = Motivo do Desligamento do Zazos (ex: "Novo Desafio", "Redução de Quadro")
- `responsavel_rh_id`, `valor_rescisao`, `etapa` ficam vazios — preencher manualmente se necessário
- `historico_desligamento`, `template_email`, `configs` — não migrados (sem equivalente no Zazos)

---

## admissao

### z_admissao.admissao — colunas não migradas

**Arquivos gerados:**
- `output/furia/admissao/profiles.csv` → `z_admissao.profiles` (537 rows) ✅
- `output/furia/admissao/admissao.csv` → `z_admissao.admissao` (210 rows) ✅

**Colunas não migradas:**

| Coluna | Motivo |
|---|---|
| `gestor_id` | FK para `public.profiles` — bloqueado até setup de auth |
| `responsavel_rh_id` | Todos os 218 registros estavam vazios no Zazos |
| `tipo_admissao_id` | Referencia `tipo_admissao` — tabela de configuração de produto, sem dados no Zazos |
| `etapa_atual_id` | Referencia `etapa_admissao` — tabela de etapas de workflow, sem dados no Zazos |
| `n1_id / n2_id / n3_id` | bigint sequencial vs UUID do Zazos — sem mapeamento direto |
| `cargo_id` | Select texto no Zazos, uuid no destino — sem mapeamento |
| `beneficios_ids` | Checklist texto no Zazos, jsonb de UUIDs no destino — sem mapeamento |
| `primeiro_emprego_clt` | Campo inexistente no Zazos |

**Tabelas de configuração não migradas** (preencher manualmente):
`tipo_admissao`, `etapa_admissao`, `pergunta_admissao`, `resposta_admissao`, `template_contrato`, `template_email`, `configs`

**Fix parcial após setup de auth:**
- Popular `gestor_id` via SQL cruzando com `public.profiles`

---

## comissionamento

### furia_comissionamento — FKs para public.profiles bloqueadas

**Arquivos gerados:**
- `output/furia/comissionamento/centro_de_custo.csv` → `furia_comissionamento.centro_de_custo` (48 rows)
- `output/furia/comissionamento/solicitacoes.csv` → `furia_comissionamento.solicitacoes` (8 rows)

**Ordem de importação:** centro_de_custo → solicitacoes

**Colunas não migradas (todas FK para public.profiles — BLOQUEADO):**
`vendedor_id`, `responsavel_rh_id`, `responsavel_cmo_id`, `responsavel_financeiro_id`, `responsavel_juridico_id`

**Fix:** Após setup de auth, popular via SQL cruzando nomes/emails com public.profiles.

**Outras colunas ausentes:**
- `valor_nf`, `valor_recebido_cliente` — todos vazios no Zazos (numeric, dropped)
- `url_contrato`, `url_nota_fiscal`, `url_termo_juridico`, `url_planilha_extra` — arquivos Attachment sem URL exportável
- `perc_licenciamento`, `perc_midia`, `perc_patrocinio`, `data_envio_juridico`, `observacoes_vendedor` — sem campo equivalente no Zazos

---

## processos

### furia_processos — colunas não migradas

**Arquivos gerados:**
- `output/furia/processos/area.csv` → `furia_processos.area` (71 rows) ✅
- `output/furia/processos/centro_de_custo.csv` → `furia_processos.centro_de_custo` (48 rows) ✅
- `output/furia/processos/softwares.csv` → `furia_processos.softwares` (7 rows) ✅
- `output/furia/processos/admissoes.csv` → `furia_processos.admissoes` (218 rows) ✅

**Ordem de importação:** area → centro_de_custo → softwares → admissoes → SQLs

**SQLs pendentes (rodar após importar admissoes):**
- `update_area_id.sql` — 173 UPDATEs
- `update_centro_de_custo_id.sql` — 217 UPDATEs

**Colunas não migradas:**

| Coluna | Motivo |
|---|---|
| `solicitante_id`, `pessoa_id`, `lider_id`, `responsavel_rh_id` | FK para `public.profiles` — bloqueado até setup de auth |
| `lista_de_acessos`, `lista_de_softwares`, `lista_de_equipamentos` | `text[]` — campo MultipleSelect/Checklist sem extractor; usam DB default `'{}'` |
| `beneficios_contratados`, `contratos_checklist` | `text[]` — Checklist sem extractor; usam DB default `'{}'` |
| `andamento_geral` | `smallint` calculado por fórmula — usa DB default `0` |

**Tabelas não migradas:**
- `tasks_internas` — sem equivalente no Zazos
- `configs` — não migrar
