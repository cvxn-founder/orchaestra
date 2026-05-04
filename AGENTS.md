# AGENTS.md — orchaestra

Terminal TUI AI orchestrator (Ink + React) with 73 tools across 8 categories,
streaming GLM/DeepSeek/GPT/Flux, plan/exec mode toggling, image generation,
ClickHouse admin, data science (stats/ML), and a provider-driven model registry.

## Quick start

```bash
bun start          # runs bun run index.tsx
```

Auth for GLM/Flux is read from `user/models.toml` (auto-created on first run).
OpenAI uses `~/.codex/auth.json` (OAuth). DeepSeek uses `DEEPSEEK_API_KEY` env.

## Tool system (73 tools)

```
tools/
  index.ts                 — registry, buildToolSchemas(), executeTool()
  io/                      — read_file, write_file, edit_file, list_dir, grep, glob
    _resolve.ts            — smart path resolver (handles /index.tsx → cwd)
  shell/                   — shell
  git/                     — git_diff, git_log, git_status
  web/                     — web_fetch, web_search (DuckDuckGo)
  project/                 — read_json, run_tests, typecheck, package_info
  meta/                    — ask_user, write_plan, remember
  compute/                 — Core compute tools + data science
    _stats.ts              — t/F/chi-square CDFs, p-values, effect sizes (verified)
    _ch_client.ts          — ClickHouse query builder for ds tools
    stack_detect.ts        — Tech stack detection (10 frameworks, 7 CSS approaches)
    element_semantic.ts    — UI element naming/classification
    ngmi_ascii.ts          — ASCII art text rendering
    autoschema_discover.ts — 2,700-line CSV schema discovery
    factsearch_parse.ts    — LLM fact parsing
    element_inspect.ts     — A11y audit, layout detection, wrapper classification
    element_uilang.ts      — UILang DSL generation
    element_componentize.ts — Duplicate component detection
    element_edit.ts        — Edit request markdown formatting
    polymarket_scorer.ts   — Whale score (0-100) + tier
    polymarket_ctf.ts      — CTF token ID derivation
    ds_ch_describe.ts      — [Tier 1] Descriptive stats via CH
    ds_ch_ttest.ts         — [Tier 1] Student's t-test via CH
    ds_ch_mann_whitney.ts  — [Tier 1] Mann-Whitney U via CH
    ds_ch_lead_lag.ts      — [Tier 1] Cross-correlation at lags via CH
    ds_ch_event_study.ts   — [Tier 1] Pre/post event analysis via CH
    ds_ch_correlation_matrix.ts — [Tier 1] Pairwise correlation matrix via CH
    ds_ch_rolling_window.ts — [Tier 1] Moving avg/sum/Z-score via CH
    ds_ch_anomaly.ts       — [Tier 1] Z-score anomaly detection via CH
    ds_ch_welch_ttest.ts   — [Tier 1] Welch's t-test via CH
    ds_ch_local_correlate.ts — [Tier 2] CH + local: r, R², p-value, CI
    ds_ch_local_regression.ts — [Tier 2] CH + local: coeffs, R², F-stat
    ds_ch_local_logistic.ts — [Tier 2] CH + local: odds ratios, pseudo-R²
    ds_local_chi_square.ts — [Tier 3] Local TS: χ², Cramér's V
    ds_local_bootstrap.ts  — [Tier 3] Local TS: bootstrap CI
    ds_local_anova.ts      — [Tier 3] Local TS: one-way ANOVA
    ds_local_effect_size.ts — [Tier 3] Local TS: Cohen's d, Hedges' g
    ds_local_permutation_test.ts — [Tier 3] Local TS: empirical p-value
  admin/                    — WBF Admin Control Plane REST tools
    _client.ts             — Shared adminFetch() with CF-Access + Bearer auth
    users.ts               — 9 user management tools
    d1.ts                  — 4 D1 database tools
    r2.ts                  — 3 R2 read tools
    r2_write.ts            — 3 R2 write/delete tools
    clickhouse.ts          — 6 ClickHouse tools (schema, search, preview, relations, aggregate, query)
    misc.ts                — audit log
  sync.ts                  — Compares tool names + content diffs vs webuildfast
  test.ts                  — Tool test runner: bun tools/test.ts
```

## Model routing

Three providers: **openai** (OAuth → codex Responses API), **deepseek** (API key → Chat Completions), **glm** + **wbf_flux** (admin API key with CF-Access headers).

GLM endpoint: `POST https://admin-api.webuildfast.ai/admin/models/glm-4.7-flash/run`
Flux endpoint: `POST https://admin-api.webuildfast.ai/admin/models/flux-2-klein-*/run`

Provider config supports: `oauth` / `api_key` types, `auth_path`, `key_env`, `admin_tui_key`, `api_keys[]`, `cf_client_id`, `cf_client_secret`. Model config supports: `endpoint`, `suppress_model`, `flux_image`.

## UI features

- **Splash**: Gradient BigText ("block" font, atlas gradient)
- **Markdown**: Bold, italic, code, headings, lists, blockquotes, clickable file paths (OSC 8)
- **JSON tables**: Auto-detects arrays/objects and renders as aligned columns
- **Tool palette**: `$` to open, type to filter, arrow keys + enter to select
- **Commands**: `/tools` catalog, `/model` picker, `/plan` `/exec` toggle
- **Image display**: ink-picture for flux/gpt-image-2 output
- **ConfirmInput**: @inkjs/ui for tool approval (y/n)
- **Spinner**: During streaming
- **StatusMessage**: Error display
- **Badge**: Mode/provider tags in header

## Data science tiers

| Tier | Runtime | Tools | What it does |
|------|---------|-------|-------------|
| 1 | ClickHouse SQL | ds_ch_* | All compute in CH via native functions |
| 2 | CH + local TS | ds_ch_local_* | CH aggregates, local p-values/R² |
| 3 | Local TS | ds_local_* | CH fetches data, local computes |
| 4 | WASM (planned) | ds_wasm_* | PCA, K-means, survival, XGBoost |

## Message memory

Capped at 150 messages. System prompt preserved, oldest non-system dropped.
Tool results truncated to 4000 chars before feeding back to model.

## Key bug fixes

1. **tool_call_id mismatch (DeepSeek)**: Fixed by storing toolCalls on Message
2. **Responses API endpoint**: OAuth routed through chatgpt.com/backend-api/codex
3. **Fullscreen resize**: process.stdout resize listener + process.stdout.rows for initial
4. **Splash ASCII on narrow terminals**: Hidden when cols < 60, BigText fallback
5. **Glob regex**: Character-by-character approach
6. **Auth priority**: OAuth → key_env → admin_tui_key → api_keys[]
7. **Model → admin API**: GLM/Flux moved to admin-api.webuildfast.ai with CF-Access
8. **Tool result overflow**: Truncated to 4000 chars for model context
9. **File path resolution**: Smart resolver handles /index.tsx → cwd join
10. **Toggle loop**: Command palette dismisses before toggling plan/exec
11. **Token counter**: Fallback estimate when API doesn't return usage
