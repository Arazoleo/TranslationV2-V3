# NgCash — Pendências de Importação

Itens que não foram importados por dependências de dados externos ou ausência de fonte no Zazos.

---

## Dependência principal: `public.profiles` (auth)

Várias tabelas referenciam `public.profiles`, que é populada pelo Supabase Auth quando os usuários fazem login. Enquanto os usuários não existirem no auth, as tabelas abaixo ficam bloqueadas.

**Solução geral:** após os usuários se cadastrarem no app, rodar um script de match por email:
```
Zazos email → public.profiles UUID → INSERT na tabela alvo
```

---

## Por módulo

### `z_despesas` — despesas

| Coluna | Tabela | Bloqueio |
|---|---|---|
| `expense_person` | `expenses` | FK → `public.profiles` (auth) |

**O que fazer:** após auth populado, UPDATE em `expenses` cruzando email do colaborador (disponível no Zazos) com UUID de `public.profiles`.

---

### `z_admissao` — admissao

| Tabela | Bloqueio |
|---|---|
| `gestor_id` em `admissao` | FK → `public.profiles` (auth) |
| `tipo_admissao` | Sem dados no Zazos — criar manualmente no app |
| `etapa_admissao` | Sem dados no Zazos — criar manualmente no app |
| `pergunta_admissao` | Sem dados no Zazos — criar manualmente no app |
| `resposta_admissao` | Depende de `admissao` + `pergunta_admissao` |
| `template_contrato` | Sem dados no Zazos |
| `template_email` | Sem dados no Zazos |

---

### `z_academy` — academy

| Tabela | Bloqueio |
|---|---|
| `profiles` | FK → `public.profiles` (auth) |
| `course_enrollments` | FK → `z_academy.profiles` |
| `video_progress` | FK → `z_academy.profiles` + `videos` |
| `modules` | Sem dados no Zazos — criar manualmente no app |
| `contents` | FK → `modules` (sem dados) |
| `content_videos` | FK → `contents` + `videos` |
| `videos` | `storage_path` NOT NULL — requer migração de arquivos para Supabase Storage |

**Check Treinamentos (806 registros)** — contém o histórico de completions por colaborador. Pode ser importado para `course_enrollments` após `profiles` ser populado via auth. Os dados estão disponíveis no Zazos (view: `d2311730`).

**Videos** — os arquivos de vídeo estão como attachments no Zazos. Para migrar: baixar do Zazos → upload no Supabase Storage → gerar `storage_path` → importar `videos`.

---

### `z_avd` — avd

| Tabela | Bloqueio |
|---|---|
| `profiles` | FK → `public.profiles` (auth) |
| `avaliacoes` | FK → `z_avd.profiles` + `ciclos` + `formularioperfil` |
| `relatorio` | FK → `z_avd.profiles` + `ciclos` |
| `respostas` | FK → `avaliacoes` + `perguntas` |
| `perguntas` | Sem dados no Zazos (depende de `temas` ✅ + `escalas` ❌) |
| `escalas` | Sem dados no Zazos — criar manualmente |
| `opcoes_de_resposta` | FK → `escalas` |
| `formularios` | Sem dados no Zazos — criar manualmente |
| `formularioperfil` | FK → `formularios` + `perfis_de_avaliacao` |
| `perfis_de_avaliacao` | Sem dados no Zazos — criar manualmente |
| `areas` | Sem dados no Zazos |
| `times` | Sem dados no Zazos |
| `grupos_calibragem` | Sem dados no Zazos |
| `entregas` | FK → `z_avd.profiles` + `relatorio` |
| `desenvolvimento` | FK → `z_avd.profiles` + `relatorio` |
| `elementos` | FK → `formularios` + `perguntas` |
| `bu_n1` | Sem dados no Zazos |

**Avaliações de Competência (1136 registros)** — o histórico completo de AVDs existe no Zazos (view: `72256ac4`). Pode ser importado para `avaliacoes` e `respostas` após: `profiles` (auth), `ciclos` ✅, `formularioperfil` e `escalas` estarem populados.

---

## Resumo de prioridades pós-auth

1. Popular `public.profiles` via login dos usuários no app
2. `z_academy.profiles` ← match email → UUID
3. `z_avd.profiles` ← match email → UUID
4. `z_despesas.expenses` ← UPDATE `expense_person` via email
5. `z_academy.course_enrollments` ← Check Treinamentos (806 registros prontos no Zazos)
6. `z_avd.avaliacoes` ← Avaliações de Competência (1136 registros prontos no Zazos) — após escalas/formulários criados manualmente
