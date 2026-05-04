# AGENTS.md — orchaestra

Terminal TUI chatbot (Ink + React) with streaming GPT/DeepSeek, 24 tools across
7 categories, plan/exec mode toggling, image generation, and a provider-driven
model registry.

## Quick start

```bash
bun start          # runs bun run index.tsx
```

On first run, `user/models.toml` is auto-created. Auth is read from
`~/.codex/auth.json` (OAuth for openai) or env vars (API keys for deepseek).

## File map

```
index.tsx                  — entire TUI app (~1050 lines)
  - Components: SplashScreen, Header, MessageBubble, ModelPicker, ToolConfirm,
    StatusBar, ConfigMenu, CommandPalette
  - App modes: splash, chat, command-palette, config, confirm-tool, model-picker
  - State: model, activeMode (plan/exec), messages[], mode, isLoading, termRows/Cols
  - Routing: streamModel() → oauth (codex Responses API SSE) | api_key (Chat Completions SSE)
  - Auth: resolveProviderAuth() — OAuth token from auth_path, API key fallback from key_env
  - Parsing: parseResponsesSSE() (oauth), parseChatSSE() (api_key)

tools/
  index.ts                 — registry, buildToolSchemas(), executeTool()
  io/                      — read_file, write_file, edit_file, list_dir, grep, glob
  shell/                   — shell
  git/                     — git_diff, git_log, git_status
  web/                     — web_fetch, web_search (DuckDuckGo, no API key)
  project/                 — read_json, run_tests, typecheck, package_info
  meta/                    — ask_user, write_plan, remember
  compute/                 — stack_detect, element_semantic, ngmi_ascii,
                             autoschema_discover (2,700-line port from webuildfast),
                             factsearch_parse
  test.ts                  — tool test runner: bun tools/test.ts
  sync.ts                  — compares tool names vs webuildfast: bun tools/sync.ts

orchaestra-ascii.txt       — ASCII art banner for splash screen
user/models.toml           — provider + model definitions (auto-created)
user/user.json             — persisted config: planModel, execModel
user/images/               — generated images from gpt-image-2
user/memory/               — persisted facts from remember tool
```

## Model routing

### OAuth path (openai provider)

```
POST https://chatgpt.com/backend-api/codex/responses
Headers: Authorization: Bearer <access_token>
         Accept: text/event-stream
         OpenAI-Beta: responses=experimental
Body: { model, input: [...], instructions, stream: true, store: false, tools: [...] }
Parse: parseResponsesSSE() — "response.output_text.delta" events
```

Token read from `~/.codex/auth.json` (`tokens.access_token`). No API key needed.

### API key path (deepseek provider)

```
POST https://api.deepseek.com/v1/chat/completions
Headers: Authorization: Bearer <api_key>
Body: { model, messages: [...], stream: true, tools: [...], thinking: { type: "disabled" } }
Parse: parseChatSSE() — standard Chat Completions SSE
```

API key from `DEEPSEEK_API_KEY` env var.

### Provider config (user/models.toml)

```toml
[providers.openai]
type = "oauth"
base_url = "https://chatgpt.com/backend-api/codex"
auth_path = "~/.codex/auth.json"

[providers.deepseek]
type = "api_key"
base_url = "https://api.deepseek.com/v1"
key_env = "DEEPSEEK_API_KEY"
```

Available models: gpt-5.5, gpt-5.4, gpt-5.3-codex, gpt-5.3-codex-spark,
deepseek-v4-pro, deepseek-v4-flash, gpt-image-2.

## Tool system

Each tool exports `{ name, description, inputSchema, run }`. Adding a tool:
1. Create `tools/<category>/<name>.ts`
2. Import and add to `RAW` array in `tools/index.ts`
3. Done — `buildToolSchemas()` picks it up automatically

Tool execution flow:
1. Model emits tool call in SSE stream → stored in `pendingTool` + attached
   to assistant message as `toolCalls[]`
2. ToolConfirm overlay shows tool name + args
3. User approves → `executeTool(name, args)` dispatches to handler
4. Result sent as tool-role message with `tool_call_id` in next API call
5. Deny → user-role message (prevents DeepSeek tool_call_id mismatch)

Testing: `bun tools/test.ts` (all 24 tools), `bun tools/test.ts grep` (single).

## Key bug fixes (chronological)

1. **tool_call_id mismatch (DeepSeek)**: Assistant messages missing `tool_calls`
   in history. Fixed by storing `toolCalls` on the Message when `onToolCall`
   fires, and including `tool_calls` array in both history builders.

2. **Responses API endpoint**: Originally used `api.openai.com/v1/chat/completions`
   with OAuth token → 429. Fixed by routing OAuth through
   `chatgpt.com/backend-api/codex/responses` with proper headers (Accept: SSE,
   OpenAI-Beta).

3. **Fullscreen resize**: `useStdout` was read once at mount. Fixed with
   `process.stdout.on("resize")` listener updating `termRows`/`termCols` state.

4. **Splash ASCII on narrow terminals**: Hidden when `termCols < 80`, replaced
   with text-only `◈ ORCHAESTRA`.

5. **Glob regex**: `globToRegex` had escaping bug. Rewrote with character-by-
   character approach instead of regex-based replacement.

6. **Auth priority**: Now OAuth first, API key fallback. Provider config accepts
   both `auth_path` and `key_env` simultaneously.

## State shape (App component)

```
activeMode: "plan" | "exec"    — plan mode uses planModel, exec uses execModel
model: string                   — active model ID
messages: Message[]             — full conversation (system, user, assistant, tool)
mode: AppMode                   — splash|chat|command-palette|config|confirm-tool|model-picker
pendingTool: ToolCall | null    — tool awaiting user approval
termRows, termCols: number      — terminal dimensions (responsive)
tokensIn, tokensOut: number     — cumulative token counters
isLoading: boolean              — prevents double-sends
```

## Commands

Type `/` to open the command palette. Arrow-key navigate, enter to select.
Commands: Change Model (sub-menu), Toggle Plan/Exec, Configuration, Clear Chat, Exit.
Fallback text commands: /model, /plan, /exec, /config, /clear, /exit, /quit.

## Image generation (gpt-image-2)

Uses the codex Responses API. Images saved to `user/images/img_{ts}_{n}.png`.
Clickable OSC 8 hyperlinks displayed in chat (iTerm2, Kitty, WezTerm, VSCode).

## Plan / Execution mode

`[plan]` (magenta) for thinking, `[exec]` (yellow) for action. Each has its
own model configured in `user/user.json` (planModel / execModel). Toggle via
`/plan`, `/exec`, or "Toggle Plan/Exec" in command palette.

## Known issues

- No conversation persistence — chat lost on exit (only config persists).
- Image generation is non-streaming — no progress during generation.
- `parseChatSSE` doesn't distinguish `finish_reason: "tool_calls"` vs `"stop"`.
- Only one tool (`shell`) used to exist; now 24, but tool call handling
  assumes single-tool-call-per-turn.
- OAuth token doesn't have `api.responses.write` scope; relies on codex endpoint
  which may change.
- `grep` tool's fallback `walk` uses `require()` in an ESM context (works in Bun
  but not standard Node ESM).
