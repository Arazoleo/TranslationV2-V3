# Pendências pós-migração — Cubos

## 1. FKs de perfil (todas as tabelas)

Todas as colunas que referenciam `public.profiles` estão **null** porque a tabela de profiles depende do setup de auth do Supabase. Após popular `public.profiles`, atualizar as FKs abaixo:

| Módulo | Tabela | Coluna |
|---|---|---|
| admissao | admissao | pessoa_id, responsavel_rh_id, gestor_id |
| desligamento | desligamentos | colaborador_profile_id |
| ferias | periodo_aquisitivo | colaborador_profile_id |
| ferias | ferias | colaborador_profile_id |
| ferias | day_off | colaborador_profile_id |
| pesquisa_de_clima | pesquisa_de_clima | colaborador_profile_id |
| engajamentos | termometro_cubico | colaborador_profile_id |
| engajamentos | pesquisa_avd | colaborador_profile_id |
| engajamentos | check_in | colaborador_profile_id |
| engajamentos | avaliacao_checkin | colaborador_profile_id |
| engajamentos | indicacao_awards | colaborador_profile_id |
| engajamentos | votacao_awards | colaborador_profile_id |
| engajamentos | nps_plan | colaborador_profile_id |
| treinamento | programas_de_capacitacoes | colaborador_profile_id |
| performance | reports | person_profile_id, manager_profile_id |
| performance | reviews | person_profile_id, reviewer_profile_id |
| performance | cycle_notes | person_profile_id, from_profile_id |
| despesas | budgets | responsible_profile_id, aligned_with_profile_id |
| despesas | expenses | expense_person |

## 2. Trofeus de awards (engajamentos)

As colunas `trofeu_*_id` em `indicacao_awards` e `votacao_awards` estão null. No Zazos essas colunas são campos Select com nome do colaborador — não há UUID disponível para extrair.

**Resolução:** após popular `public.profiles`, fazer lookup por nome para preencher os IDs.

Colunas afetadas em `indicacao_awards`:
- trofeu_esponja_id
- trofeu_saiu_da_caixa_id
- trofeu_nao_tem_medo_id
- trofeu_cerejinha_id
- trofeu_maestria_id
- trofeu_extra_indicado_id

Colunas afetadas em `votacao_awards`:
- trofeu_esponja_id
- trofeu_saiu_da_caixa_id
- trofeu_nao_tem_medo_id
- trofeu_cerejinha_id
- trofeu_maestria_id

## 3. Budgets (despesas)

A sheet **Budgets** no Zazos está vazia — nenhum registro foi importado. A tabela existe no Supabase mas está sem dados.

Consequência: `expenses.budget_id` está null em todos os 711 registros.

**Resolução:** popular `budgets` manualmente ou via nova exportação quando os dados existirem, depois atualizar `expenses.budget_id`.

Obs: o campo `is_active` no Zazos é um Select ("Sim"/"Não") — precisará de post-processing para converter para boolean antes de importar.

## 4. Campos não migrados (sem equivalente no Zazos)

| Módulo | Tabela | Coluna | Motivo |
|---|---|---|---|
| performance | reports | peers | jsonb — Link no Zazos sem extração UUID disponível |
| performance | reports | liderados | jsonb — Link no Zazos sem extração UUID disponível |
| ferias | ferias | periodo_aquisitivo_id | FK mapeada via UUID do Zazos — verificar integridade |
| performance | reviews | report_id | FK mapeada via UUID do Zazos — verificar integridade |
| despesas | expenses | cost_center | Lookup — desnormalizado, não extraído |
| despesas | expenses | expense_receipt | Attachment — URL não extraída |
| treinamento | programas_de_capacitacoes | tipo | Campo inferido pós-geração (valor: 'sponge' para todos) — revisar registros de capacitação |
| cycle_notes | cycle_notes | period | text[] — MultipleSelect sem suporte de extração |
