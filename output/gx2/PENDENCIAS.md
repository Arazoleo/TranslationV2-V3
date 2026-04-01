# Pendências GX2 — Pós-Migração

> Última atualização: 2026-04-01

---

## 1. FKs para `public.profiles` (bloqueadas até setup de auth)

Todas as colunas abaixo foram importadas como NULL porque a tabela `public.profiles` é populada via autenticação e ainda não existe. Após o setup de auth, atualizar com UPDATE baseado em matching por email ou nome.

| Schema | Tabela | Coluna |
|--------|--------|--------|
| z_admissao | admissao | responsavel_rh_id |
| z_admissao | admissao | gestor_id |
| z_desligamento | desligamentos | responsavel_rh_id |
| z_movimentacoes | movimentacao | manager_anterior_id |
| z_movimentacoes | movimentacao | manager_novo_id |

---

## 2. FKs para tabelas bigint (UUID→bigint mismatch)

Os IDs do Zazos são UUID mas as tabelas de destino usam bigint PK gerado por sequência. Não foi possível mapear automaticamente. Deixar NULL por ora; popular via script de matching por nome após migração completa.

| Schema | Tabela | Coluna | Tabela destino |
|--------|--------|--------|----------------|
| z_shared | contratos | cargo_id | z_shared.cargos |
| z_movimentacoes | movimentacao | cargo_anterior_id | z_shared.cargos |
| z_movimentacoes | movimentacao | cargo_novo_id | z_shared.cargos |
| z_movimentacoes | movimentacao | marca | (desconhecida) |
| z_movimentacoes | movimentacao | departamento | (desconhecida) |
| z_movimentacoes | movimentacao | unidade_negocio | (desconhecida) |
| z_movimentacoes | movimentacao | empresa_contratante | z_shared.empresas |

---

## 3. Colunas sem dado Zazos (sem fonte disponível)

Campos que existem no schema mas não têm equivalente mapeável no Zazos. Deixados como NULL/default.

| Schema | Tabela | Coluna | Motivo |
|--------|--------|--------|--------|
| z_admissao | admissao | tipo_admissao_id | sem campo no Zazos |
| z_admissao | admissao | n1_id, n2_id, n3_id | sem campo no Zazos |
| z_admissao | admissao | cargo_id | UUID→bigint mismatch |
| z_admissao | admissao | beneficios_ids | sem campo no Zazos |
| z_admissao | admissao | etapa_atual_id | sem campo no Zazos |
| z_beneficios | adesao_beneficio | pessoa_id | sem campo no Zazos |
| z_beneficios | adesao_beneficio | valor_beneficio_id | sem campo no Zazos |
| z_recessos | periodo_aquisitivo | pessoa_id | campo não exposto pela API do Zazos |

---

## 4. `z_recessos.periodo_aquisitivo` — pessoa_id nula

A view do Zazos para "Período aquisitivo" não retorna o campo link "Pessoa" nos dados da API. Todos os 78 registros foram importados com `pessoa_id = NULL`.

**Como corrigir pós-import:**
```sql
-- Após importar solicitacao_ferias, fazer UPDATE via join:
UPDATE z_recessos.periodo_aquisitivo pa
SET pessoa_id = sf.pessoa_id
FROM z_recessos.solicitacao_ferias sf
WHERE sf.periodo_aquisitivo_id = pa.id
  AND pa.pessoa_id IS NULL;
```

---

## 5. `z_recessos.feriados_nacionais` — constraint `ano` unique

O constraint `feriados_nacionais_ano_key` é `UNIQUE` em `ano`. Todos os 11 feriados são de 2025, portanto apenas 1 registro foi importado com sucesso. Os outros 10 falharam silenciosamente (dependendo do modo de import).

**Ação:** Verificar se o constraint `ano` deve ser único individualmente ou em composição com `data`. Se for erro de schema, remover o constraint e reimportar os 11 feriados.

---

## 6. Tabelas não migradas — `z_recessos`

| Tabela | Motivo |
|--------|--------|
| atestados | Campos obrigatórios (`tipo_atestado`, `data_inicio`, `data_fim`, `arquivo_path`, `arquivo_nome`) sem dado no Zazos |
| ausencia_p_j | Sem sheet correspondente no Zazos |
| periodo_aquisitivo_p_j | Sem sheet correspondente no Zazos (específico para PJ) |
| contrato | Sem mapeamento claro para z_recessos.contrato |
| configs | Tabela de configuração, não migrar |

---

## 7. Modules não iniciados

Verificar se existem outros módulos/schemas no GX2 além dos 8 migrados:

- [ ] Confirmar com cliente quais schemas estão em uso além dos 8 módulos migrados
