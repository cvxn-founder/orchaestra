#!/usr/bin/env node
import React, { useState, useEffect, useRef, useCallback } from "react";
import { render, Box, Text, useInput, useApp, useStdout } from "ink";
import TextInput from "ink-text-input";
import { Select, ConfirmInput, Spinner, StatusMessage, Badge } from "@inkjs/ui";
import Image, { TerminalInfoProvider } from "ink-picture";
import Gradient from "ink-gradient";
import BigText from "ink-big-text";
import { readFileSync, writeFileSync, existsSync, mkdirSync } from "fs";
import { homedir } from "os";
import { join } from "path";
import { buildToolSchemas, executeTool, TOOL_MAP, TOOL_MODULES } from "./tools";

// ─── Types ────────────────────────────────────────────────────────────────────

type Role = "user" | "assistant" | "system" | "tool";

interface ProviderConfig {
  type: "oauth" | "api_key";
  base_url: string;
  auth_path?: string;
  key_env?: string;
  api_keys?: string[];
  admin_tui_key?: string;
  cf_client_id?: string;
  cf_client_secret?: string;
}

interface ModelDef {
  id: string;
  provider: string;
  label: string;
  endpoint?: string;
  suppress_model?: boolean;
  flux_image?: boolean;
}

interface ModelRegistry {
  providers: Record<string, ProviderConfig>;
  models: ModelDef[];
}

interface Message {
  id: string;
  role: Role;
  content: string;
  model?: string;
  toolName?: string;
  toolCallId?: string;
  toolCalls?: ToolCall[];
  isStreaming?: boolean;
  imagePaths?: string[];
  revisedPrompt?: string;
}

interface ToolCall {
  id: string;
  name: string;
  arguments: string;
}

type AppMode = "chat" | "confirm-tool" | "model-picker" | "splash" | "config" | "command-palette" | "tools-catalog" | "tool-palette";

interface UserConfig {
  defaultModel?: string;
  planModel?: string;
  execModel?: string;
}

interface CommandDef {
  id: string;
  label: string;
  desc: string;
  sub?: "model-picker" | null;
}

const COMMANDS: CommandDef[] = [
  { id: "model",  label: "Change Model",  desc: "Switch AI model",           sub: "model-picker" },
  { id: "toggle", label: "Toggle Plan/Exec", desc: "Switch between plan and exec mode", sub: null },
  { id: "config", label: "Configuration", desc: "Settings & preferences",     sub: null },
  { id: "clear",  label: "Clear Chat",    desc: "Wipe message history",       sub: null },
  { id: "exit",   label: "Exit",          desc: "Quit the application",       sub: null },
];

// ─── Paths ────────────────────────────────────────────────────────────────────

const ROOT_DIR = import.meta.dirname;
const USER_DIR = join(ROOT_DIR, "user");
const CONFIG_PATH = join(USER_DIR, "user.json");
const MODELS_TOML_PATH = join(USER_DIR, "models.toml");
const IMAGE_DIR = join(USER_DIR, "images");

// ─── Models registry ──────────────────────────────────────────────────────────

const DEFAULT_MODELS_TOML = `# Model provider configuration
# Provider types: oauth (reads token from auth_path) or api_key (reads from key_env)

[providers.openai]
type = "oauth"
base_url = "https://chatgpt.com/backend-api/codex"
auth_path = "~/.codex/auth.json"

[providers.deepseek]
type = "api_key"
base_url = "https://api.deepseek.com/v1"
key_env = "DEEPSEEK_API_KEY"

[[models]]
id = "glm-4.7-flash"
provider = "glm"
label = "GLM 4.7 Flash  (webuildfast)"
endpoint = "https://admin-api.webuildfast.ai/admin/models/glm-4.7-flash/run"
suppress_model = true

[[models]]
id = "gpt-5.5"
provider = "openai"
label = "GPT-5.5  (ChatGPT OAuth)"

[[models]]
id = "gpt-5.4"
provider = "openai"
label = "GPT-5.4  (ChatGPT OAuth)"

[[models]]
id = "deepseek-v4-pro"
provider = "deepseek"
label = "DeepSeek V4 Pro  (API key)"

[[models]]
id = "deepseek-v4-flash"
provider = "deepseek"
label = "DeepSeek V4 Flash  (API key)"

[[models]]
id = "gpt-5.3-codex"
provider = "openai"
label = "GPT-5.3 Codex  (ChatGPT OAuth)"

[[models]]
id = "gpt-5.3-codex-spark"
provider = "openai"
label = "GPT-5.3 Codex Spark  (ChatGPT OAuth)"

[providers.glm]
type = "api_key"
base_url = "https://admin-api.webuildfast.ai"
admin_tui_key = "wbf_admin_1a0775eb4eca7052b72bd0ae25690b70649170d8bf8d2e1321e104f1fd0449e2"
key_env = "ADMIN_TUI_KEY"
cf_client_id = "68c6292d29a523f7e114ac4e7b266ddf.access"
cf_client_secret = "b102874b807b593e368f7266b2c40166d1d041d3454443d29bcd725e82d38c97"

[providers.wbf_flux]
type = "api_key"
base_url = "https://admin-api.webuildfast.ai"
admin_tui_key = "wbf_admin_1a0775eb4eca7052b72bd0ae25690b70649170d8bf8d2e1321e104f1fd0449e2"
key_env = "ADMIN_TUI_KEY"
cf_client_id = "68c6292d29a523f7e114ac4e7b266ddf.access"
cf_client_secret = "b102874b807b593e368f7266b2c40166d1d041d3454443d29bcd725e82d38c97"

[[models]]
id = "flux-2-klein-4b"
provider = "wbf_flux"
label = "Flux 2 Klein 4B  (webuildfast)"
endpoint = "https://admin-api.webuildfast.ai/admin/models/flux-2-klein-4b/run"
suppress_model = true
flux_image = true

[[models]]
id = "flux-2-klein-9b"
provider = "wbf_flux"
label = "Flux 2 Klein 9B  (webuildfast)"
endpoint = "https://admin-api.webuildfast.ai/admin/models/flux-2-klein-9b/run"
suppress_model = true
flux_image = true

[[models]]
id = "gpt-image-2"
provider = "openai"
label = "GPT Image 2  (ChatGPT OAuth)"

[admin]
admin_tui_key = "wbf_admin_1a0775eb4eca7052b72bd0ae25690b70649170d8bf8d2e1321e104f1fd0449e2"
cf_client_id = "68c6292d29a523f7e114ac4e7b266ddf.access"
cf_client_secret = "b102874b807b593e368f7266b2c40166d1d041d3454443d29bcd725e82d38c97"
`;

function loadRegistry(): ModelRegistry {
  try {
    if (!existsSync(MODELS_TOML_PATH)) {
      if (!existsSync(USER_DIR)) mkdirSync(USER_DIR, { recursive: true });
      writeFileSync(MODELS_TOML_PATH, DEFAULT_MODELS_TOML, "utf8");
    }
    const raw = readFileSync(MODELS_TOML_PATH, "utf8");
    const parsed = (Bun as any).TOML.parse(raw) as any;
    const providers: Record<string, ProviderConfig> = {};
    for (const [name, cfg] of Object.entries(parsed.providers ?? {})) {
      const c = cfg as any;
      providers[name] = { type: c.type, base_url: c.base_url, auth_path: c.auth_path, key_env: c.key_env, api_keys: c.api_keys, admin_tui_key: c.admin_tui_key, cf_client_id: c.cf_client_id, cf_client_secret: c.cf_client_secret };
    }
    const models: ModelDef[] = (parsed.models ?? []).map((m: any) => ({
      id: m.id, provider: m.provider, label: m.label, endpoint: m.endpoint, suppress_model: m.suppress_model, flux_image: m.flux_image,
    }));
    return { providers, models };
  } catch { return { providers: {}, models: [] }; }
}

function resolveAuthPath(p: string): string {
  if (p.startsWith("~/")) return join(homedir(), p.slice(2));
  return p;
}

function resolveProviderAuth(provider: ProviderConfig): string | null {
  // Try OAuth token first
  if (provider.auth_path) {
    try {
      const path = resolveAuthPath(provider.auth_path);
      if (existsSync(path)) {
        const auth = JSON.parse(readFileSync(path, "utf8"));
        const token = auth?.tokens?.access_token;
        if (token) return token;
      }
    } catch {}
  }
  // Fall back to API key from env
  if (provider.key_env) {
    const key = process.env[provider.key_env];
    if (key) return key;
  }
  // Fall back to admin_tui_key from config
  if (provider.admin_tui_key) return provider.admin_tui_key;
  // Fall back to api_keys array (pick one randomly for load balancing)
  if (provider.api_keys && provider.api_keys.length > 0) {
    return provider.api_keys[Math.floor(Math.random() * provider.api_keys.length)];
  }
  return null;
}

function loadConfig(): UserConfig {
  try {
    if (!existsSync(CONFIG_PATH)) return {};
    return JSON.parse(readFileSync(CONFIG_PATH, "utf8"));
  } catch { return {}; }
}

function saveConfig(cfg: UserConfig) {
  if (!existsSync(USER_DIR)) mkdirSync(USER_DIR, { recursive: true });
  const existing = loadConfig();
  const merged = { ...existing, ...cfg };
  writeFileSync(CONFIG_PATH, JSON.stringify(merged, null, 2), "utf8");
}

const REGISTRY: ModelRegistry = loadRegistry();
const ALL_MODEL_IDS: string[] = REGISTRY.models.map(m => m.id);
const MODEL_LABELS: Record<string, string> = {};
for (const m of REGISTRY.models) MODEL_LABELS[m.id] = m.label;

function getProviderType(modelId: string): string | null {
  const def = REGISTRY.models.find(m => m.id === modelId);
  if (!def) return null;
  return REGISTRY.providers[def.provider]?.type ?? null;
}

// ─── API calls ────────────────────────────────────────────────────────────────

function randomId() { return Math.random().toString(36).slice(2, 10); }

interface StreamCallbacks {
  onDelta: (text: string) => void;
  onToolCall: (tc: ToolCall) => void;
  onImage: (paths: string[], revisedPrompt: string) => void;
  onDone: (usage?: { input: number; output: number }) => void;
  onError: (err: string) => void;
}


function isImageModel(modelId: string) { return modelId.includes("image"); }

function osc8Link(filePath: string, label: string): string {
  return `]8;;file://${filePath}\\${label}]8;;\\`;
}

function cfHeaders(provider: ProviderConfig): Record<string, string> {
  const h: Record<string, string> = {};
  if (provider.cf_client_id) h["CF-Access-Client-Id"] = provider.cf_client_id;
  if (provider.cf_client_secret) h["CF-Access-Client-Secret"] = provider.cf_client_secret;
  return h;
}

function extractApiError(body: string, status: number): string {
  try {
    const j = JSON.parse(body);
    // webuildfast: { error: "message" } or { ok: false, error: "message" }
    if (typeof j.error === "string") return j.error;
    // OpenAI: { error: { message: "..." } }
    if (j.error?.message) return j.error.message;
    // OpenAI Responses API: { detail: "..." }
    if (j.detail) return j.detail;
  } catch {}
  return `${status}: ${body.slice(0, 200)}`;
}

async function streamModel(
  modelId: string,
  messages: Array<{ role: string; content: string }>,
  cb: StreamCallbacks,
  registry: ModelRegistry,
) {
  const def = registry.models.find(m => m.id === modelId);
  if (!def) { cb.onError(`Unknown model: ${modelId}`); return; }

  const provider = registry.providers[def.provider];
  if (!provider) { cb.onError(`Unknown provider: ${def.provider}`); return; }

  const auth = resolveProviderAuth(provider);
  if (!auth) {
    cb.onError(`No auth for ${def.provider} — ${provider.auth_path ? "check auth_path" : ""} ${provider.key_env ? "set " + provider.key_env : ""} ${provider.api_keys?.length ? "api_keys present but all failed" : ""}`);
    return;
  }

  // ── Image generation path (Responses API, non-streaming) ──
  if (isImageModel(modelId)) {
    const lastMsg = messages[messages.length - 1];
    const prompt = lastMsg?.content ?? "an image";
    const body = { model: modelId, input: prompt, tools: [{ type: "image_generation", action: "generate" }] };

    let resp: Response;
    try {
      resp = await fetch(def.endpoint || `${provider.base_url}/responses`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${auth}`, ...cfHeaders(provider) },
        body: JSON.stringify(body),
      });
    } catch (e: any) { cb.onError(e.message); return; }

    if (!resp.ok) { const t = await resp.text(); cb.onError(extractApiError(t, resp.status)); return; }

    try {
      const data: any = await resp.json();
      const imageOutputs = (data.output ?? []).filter((o: any) => o.type === "image_generation_call");
      if (imageOutputs.length === 0) { cb.onError("No image generated"); return; }

      if (!existsSync(IMAGE_DIR)) mkdirSync(IMAGE_DIR, { recursive: true });
      const paths: string[] = [];
      for (let i = 0; i < imageOutputs.length; i++) {
        const b64 = imageOutputs[i].result;
        if (!b64) continue;
        const ts = Date.now();
        const filename = `img_${ts}_${i}.png`;
        const filePath = join(IMAGE_DIR, filename);
        writeFileSync(filePath, Buffer.from(b64, "base64"));
        paths.push(filePath);
      }
      const revised = imageOutputs[0]?.revised_prompt ?? prompt;
      cb.onImage(paths, revised);
      cb.onDone();
    } catch (e: any) { cb.onError(e.message); }
    return;
  }

  // ── Flux image generation path (non-streaming, custom body format) ──
  if (def.flux_image) {
    const lastMsg = messages[messages.length - 1];
    const prompt = lastMsg?.content ?? "a beautiful image";
    const body: Record<string, any> = { prompt, width: 1024, height: 1024, steps: 25 };

    let resp: Response;
    try {
      resp = await fetch(def.endpoint || `${provider.base_url}/chat/completions`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${auth}`, ...cfHeaders(provider) },
        body: JSON.stringify(body),
      });
    } catch (e: any) { cb.onError(e.message); return; }

    if (!resp.ok) { const t = await resp.text(); cb.onError(extractApiError(t, resp.status)); return; }

    try {
      const data: any = await resp.json();
      const b64 = data?.result_raw?.image;
      if (!b64) { cb.onError("No image in flux response"); return; }

      if (!existsSync(IMAGE_DIR)) mkdirSync(IMAGE_DIR, { recursive: true });
      const ts = Date.now();
      const filename = `flux_${ts}.jpg`;
      const filePath = join(IMAGE_DIR, filename);
      writeFileSync(filePath, Buffer.from(b64, "base64"));
      cb.onImage([filePath], prompt);
      cb.onDone();
    } catch (e: any) { cb.onError(e.message); }
    return;
  }

  // ── OAuth path: codex Responses API (SSE streaming) ──
  if (provider.type === "oauth") {
    const input = messages.map(m => {
      const r: any = { role: m.role === "system" ? "developer" : m.role, content: m.content };
      if (m.role === "tool" && m.toolCallId) r.tool_call_id = m.toolCallId;
      return r;
    });

    const respTools = buildToolSchemas().map(t => ({
      type: "function" as const,
      name: t.function.name,
      description: t.function.description,
      parameters: t.function.parameters,
    }));

    const reqBody: Record<string, any> = {
      model: modelId,
      input,
      instructions: "You are a helpful coding assistant.",
      stream: true,
      store: false,
    };
    if (respTools.length > 0) reqBody.tools = respTools;

    let resp: Response;
    try {
      resp = await fetch(def.endpoint || `${provider.base_url}/responses`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${auth}`,
          Accept: "text/event-stream",
          "OpenAI-Beta": "responses=experimental",
          ...cfHeaders(provider),
        },
        body: JSON.stringify(reqBody),
      });
    } catch (e: any) { cb.onError(e.message); return; }

    if (!resp.ok) {
      const t = await resp.text();
      cb.onError(extractApiError(t, resp.status));
      return;
    }

    await parseResponsesSSE(resp, cb);
    return;
  }

  // ── API-key path: Chat Completions SSE (DeepSeek etc.) ──
  const body: Record<string, any> = {
    messages,
    stream: true,
    max_tokens: 8192,
    tools: buildToolSchemas(),
  };
  if (!def.suppress_model) body.model = modelId;

  if (def.provider === "deepseek") body.thinking = { type: "disabled" };

  let resp: Response;
  try {
    resp = await fetch(def.endpoint || `${provider.base_url}/chat/completions`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${auth}`, ...cfHeaders(provider) },
      body: JSON.stringify(body),
    });
  } catch (e: any) { cb.onError(e.message); return; }

  if (!resp.ok) {
    const t = await resp.text();
    cb.onError(extractApiError(t, resp.status));
    return;
  }

  await parseChatSSE(resp, cb);
}

async function parseResponsesSSE(resp: Response, cb: StreamCallbacks) {
  const reader = resp.body!.getReader();
  const dec = new TextDecoder();
  let buf = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });

    // Parse SSE: lines starting with "data:" contain JSON
    const lines = buf.split("\n");
    buf = lines.pop() ?? "";

    for (const line of lines) {
      const t = line.trim();
      if (!t.startsWith("data:")) continue;
      const raw = t.slice(5).trim();
      if (!raw) continue;
      try {
        const ev = JSON.parse(raw);
        if (ev.type === "response.output_text.delta") {
          cb.onDelta(ev.delta ?? "");
        }
        if (ev.type === "response.completed") {
          const u = ev.response?.usage;
          cb.onDone(u ? { input: u.input_tokens ?? 0, output: u.output_tokens ?? 0 } : undefined);
          return;
        }
      } catch {}
    }
  }
  cb.onDone();
}

async function parseChatSSE(resp: Response, cb: StreamCallbacks) {
  const reader = resp.body!.getReader();
  const dec = new TextDecoder();
  let buf = "";
  let toolCallsAcc: Record<number, ToolCall> = {};

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    const lines = buf.split("\n");
    buf = lines.pop() ?? "";

    for (const line of lines) {
      const t = line.trim();
      if (!t.startsWith("data:")) continue;
      const raw = t.slice(5).trim();
      if (raw === "[DONE]") { cb.onDone(); return; }
      let chunk: any;
      try { chunk = JSON.parse(raw); } catch { continue; }

      const choice = chunk.choices?.[0];
      if (!choice) continue;
      const delta = choice.delta ?? {};

      if (typeof delta.content === "string" && delta.content) {
        cb.onDelta(delta.content);
      }

      if (Array.isArray(delta.tool_calls)) {
        for (const tc of delta.tool_calls) {
          const i = tc.index ?? 0;
          if (!toolCallsAcc[i]) toolCallsAcc[i] = { id: tc.id ?? randomId(), name: "", arguments: "" };
          if (tc.function?.name) toolCallsAcc[i].name += tc.function.name;
          if (tc.function?.arguments) toolCallsAcc[i].arguments += tc.function.arguments;
        }
      }

      if (choice.finish_reason) {
        const tools = Object.values(toolCallsAcc);
        for (const tc of tools) cb.onToolCall(tc);
        cb.onDone(chunk.usage ? { input: chunk.usage.prompt_tokens, output: chunk.usage.completion_tokens } : undefined);
        return;
      }
    }
  }
  cb.onDone();
}

// ─── Components ───────────────────────────────────────────────────────────────

const BORDER = "─";

function Header({ model, activeMode, width }: { model: string; activeMode: "plan" | "exec"; width: number }) {
  const ptype = getProviderType(model);
  const tag = ptype === "oauth" ? "oauth" : ptype === "api_key" ? "key" : "?";
  const tagColor = ptype === "oauth" ? "green" : "cyan";
  const modeColor = activeMode === "plan" ? "magenta" : "yellow";
  return (
    <Box flexDirection="column">
      <Text color="gray">{BORDER.repeat(width)}</Text>
      <Box justifyContent="space-between" width={width}>
        <Text bold color="white"> ◈ orchaestra</Text>
        <Text> </Text>
        <Box>
          <Badge color={modeColor}>{activeMode}</Badge>
          <Text> </Text>
          <Text color="white">{model}</Text>
          <Text>  </Text>
          <Badge color={tagColor}>{tag}</Badge>
          <Text>  </Text>
          <Text color="gray">/ for commands</Text>
        </Box>
      </Box>
      <Text color="gray">{BORDER.repeat(width)}</Text>
    </Box>
  );
}

// ─── Markdown formatter ───────────────────────────────────────────────────────

type InlineSeg = { text: string; bold?: boolean; italic?: boolean; code?: boolean; link?: string };

const PATH_RE = /(\/(?:[\w.-]+\/)+[\w.-]+(?:\.[\w]+)?)/g;

function splitPaths(text: string): InlineSeg[] {
  const segs: InlineSeg[] = [];
  let last = 0;
  let m;
  while ((m = PATH_RE.exec(text)) !== null) {
    if (m.index > last) segs.push({ text: text.slice(last, m.index) });
    segs.push({ text: m[0], link: m[0] });
    last = m.index + m[0].length;
  }
  if (last < text.length) segs.push({ text: text.slice(last) });
  return segs.length > 0 ? segs : [{ text }];
}

function parseInline(text: string): InlineSeg[] {
  const segs: InlineSeg[] = [];
  let i = 0;
  let cur = "";
  const push = () => { if (cur) { segs.push(...splitPaths(cur)); cur = ""; } };

  while (i < text.length) {
    // Inline code: `text`
    if (text[i] === "`" && text[i + 1] !== "`") {
      const end = text.indexOf("`", i + 1);
      if (end > i) {
        push();
        segs.push({ text: text.slice(i + 1, end), code: true });
        i = end + 1;
        continue;
      }
    }
    // Bold: **text**
    if (text[i] === "*" && text[i + 1] === "*" && text[i + 2] !== "*") {
      const end = text.indexOf("**", i + 2);
      if (end > i) {
        push();
        segs.push({ text: text.slice(i + 2, end), bold: true });
        i = end + 2;
        continue;
      }
    }
    // Italic: *text* (but not **)
    if (text[i] === "*" && text[i + 1] !== "*") {
      const end = text.indexOf("*", i + 1);
      if (end > i && text[end - 1] !== " ") {
        push();
        segs.push({ text: text.slice(i + 1, end), italic: true });
        i = end + 1;
        continue;
      }
    }
    cur += text[i];
    i++;
  }
  push();
  return segs;
}

function InlineText({ segs }: { segs: InlineSeg[] }) {
  return (
    <Text>
      {segs.map((s, i) => {
        let color: string | undefined;
        if (s.code) color = "yellow";
        if (s.link) color = "cyan";
        return s.link
          ? <Text key={i} color="cyan">{osc8Link(s.link, s.text)}</Text>
          : <Text key={i} bold={s.bold} italic={s.italic} color={color}>{s.text}</Text>;
      })}
    </Text>
  );
}

function JsonTable({ data }: { data: Record<string, any>[] }) {
  if (!data.length) return <Text color="gray">(empty)</Text>;
  const keys = Object.keys(data[0]);
  const maxW = Math.min(40, Math.floor(80 / keys.length));
  const fmt = (v: any): string => {
    if (v === null || v === undefined) return "";
    if (typeof v === "object") return JSON.stringify(v).slice(0, maxW);
    return String(v).slice(0, maxW);
  };
  return (
    <Box flexDirection="column">
      {/* Header */}
      <Box>
        {keys.map(k => (
          <Box key={k} width={maxW + 1}>
            <Text bold color="cyan">{k.slice(0, maxW).padEnd(maxW)}</Text>
          </Box>
        ))}
      </Box>
      <Text color="gray">{"─".repeat(Math.min(keys.length * (maxW + 1), 120))}</Text>
      {/* Rows */}
      {data.slice(0, 50).map((row, i) => (
        <Box key={i}>
          {keys.map(k => (
            <Box key={k} width={maxW + 1}>
              <Text>{fmt(row[k]).padEnd(maxW)}</Text>
            </Box>
          ))}
        </Box>
      ))}
      {data.length > 50 && <Text color="gray">… {data.length - 50} more rows</Text>}
    </Box>
  );
}

function FormattedContent({ text }: { text: string }) {
  if (!text) return <Text>…</Text>;

  // Detect JSON array/object and render as table
  const trimmed = text.trim();
  if (trimmed.startsWith("[") || trimmed.startsWith("{")) {
    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed) && parsed.length > 0 && typeof parsed[0] === "object") {
        return <JsonTable data={parsed} />;
      }
      if (!Array.isArray(parsed) && typeof parsed === "object" && parsed !== null) {
        // Check for nested arrays of objects — render those as tables
        for (const [k, v] of Object.entries(parsed)) {
          if (Array.isArray(v) && v.length > 0 && typeof v[0] === "object") {
            return (
              <Box flexDirection="column">
                <Text bold color="cyan">{k}:</Text>
                <JsonTable data={v} />
              </Box>
            );
          }
          // Nested object with array inside: e.g. { data: { tables: [...] } }
          if (typeof v === "object" && v !== null && !Array.isArray(v)) {
            for (const [nk, nv] of Object.entries(v as Record<string, any>)) {
              if (Array.isArray(nv) && nv.length > 0 && typeof nv[0] === "object") {
                return (
                  <Box flexDirection="column">
                    <Text bold color="cyan">{k}.{nk}:</Text>
                    <JsonTable data={nv} />
                  </Box>
                );
              }
            }
          }
        }
        return (
          <Box flexDirection="column">
            {Object.entries(parsed).map(([k, v]) => (
              <Box key={k}>
                <Text bold color="cyan">{k}: </Text>
                <Text>{typeof v === "string" ? v : typeof v === "number" ? String(v) : JSON.stringify(v).slice(0, 120)}</Text>
              </Box>
            ))}
          </Box>
        );
      }
    } catch {}
  }

  // Split into code blocks and regular text
  const parts: { type: "text" | "code"; content: string }[] = [];
  let remaining = text;
  while (remaining.length > 0) {
    const tick = remaining.indexOf("```");
    if (tick === -1) {
      parts.push({ type: "text", content: remaining });
      break;
    }
    if (tick > 0) parts.push({ type: "text", content: remaining.slice(0, tick) });
    const endTick = remaining.indexOf("```", tick + 3);
    if (endTick === -1) {
      // Unclosed code block — render rest as code
      parts.push({ type: "code", content: remaining.slice(tick + 3) });
      break;
    }
    parts.push({ type: "code", content: remaining.slice(tick + 3, endTick) });
    remaining = remaining.slice(endTick + 3);
  }

  return (
    <Box flexDirection="column">
      {parts.map((part, pi) => {
        if (part.type === "code") {
          const lines = part.content.split("\n");
          return (
            <Box key={pi} flexDirection="column" marginY={1}>
              {lines.map((line, li) => (
                <Text key={li} color="gray" dimColor>{line || " "}</Text>
              ))}
            </Box>
          );
        }
        // Block-level parsing for regular text
        return (
          <Box key={pi} flexDirection="column">
            {part.content.split("\n").map((line, li) => {
              const trimmed = line.trimStart();
              const indent = line.length - trimmed.length;

              // Heading
              const headingMatch = trimmed.match(/^(#{1,4})\s+(.+)/);
              if (headingMatch) {
                return (
                  <Box key={li} marginLeft={0} marginTop={li > 0 ? 1 : 0}>
                    <InlineText segs={[{ text: headingMatch[2], bold: true }]} />
                  </Box>
                );
              }

              // Unordered list
              const ulMatch = trimmed.match(/^[\*\-\+]\s+(.+)/);
              if (ulMatch) {
                return (
                  <Box key={li} marginLeft={indent + 2}>
                    <Text color="gray">• </Text>
                    <InlineText segs={parseInline(ulMatch[1])} />
                  </Box>
                );
              }

              // Ordered list
              const olMatch = trimmed.match(/^\d+[\.\)]\s+(.+)/);
              if (olMatch) {
                const num = trimmed.match(/^(\d+)/)![0];
                return (
                  <Box key={li} marginLeft={indent + 2}>
                    <Text color="gray">{num}. </Text>
                    <InlineText segs={parseInline(olMatch[1])} />
                  </Box>
                );
              }

              // Blockquote
              const bqMatch = trimmed.match(/^>\s?(.*)/);
              if (bqMatch) {
                return (
                  <Box key={li} marginLeft={2}>
                    <Text color="gray" dimColor>│ </Text>
                    <InlineText segs={parseInline(bqMatch[1])} />
                  </Box>
                );
              }

              // Empty line
              if (trimmed === "") {
                return <Box key={li} height={1} />;
              }

              // Regular paragraph line
              return (
                <Box key={li} marginLeft={indent > 0 ? Math.min(indent, 4) : 0}>
                  <InlineText segs={parseInline(trimmed || line)} />
                </Box>
              );
            })}
          </Box>
        );
      })}
    </Box>
  );
}

function MessageBubble({ msg }: { msg: Message }) {
  if (msg.role === "system") return null;

  if (msg.role === "tool") {
    return (
      <Box flexDirection="column" marginBottom={1}>
        <Text color="gray">  ╰ </Text>
        <Box marginLeft={4} borderStyle="round" borderColor="gray" paddingX={1}>
          <FormattedContent text={msg.content} />
          {msg.imagePaths && msg.imagePaths.length > 0 && (
            <Box flexDirection="column" marginTop={1}>
              {msg.imagePaths.map((p, i) => {
                const filename = p.split("/").pop() ?? `image_${i}.png`;
                return (
                  <Box key={i} flexDirection="column">
                    <Image src={p} width={40} height={15} />
                    <Box>
                      <Text color="gray">  </Text>
                      <Text color="cyan">{osc8Link(p, `🖼 ${filename}`)}</Text>
                    </Box>
                  </Box>
                );
              })}
              {msg.revisedPrompt && (
                <Text color="gray" dimColor>  {msg.revisedPrompt}</Text>
              )}
            </Box>
          )}
        </Box>
      </Box>
    );
  }

  const isUser = msg.role === "user";
  return (
    <Box flexDirection="column" marginBottom={1}>
      <Box marginLeft={0}>
        <Text color={isUser ? "yellow" : "cyan"} bold>
          {isUser ? "you" : `SURFACE[${msg.model ?? "ai"}]`}
        </Text>
        {msg.isStreaming && <Spinner />}
      </Box>
      <Box marginLeft={isUser ? 4 : 2} marginRight={isUser ? 0 : 4}>
        {isUser
          ? <Text color="white" wrap="wrap">{msg.content || (msg.isStreaming ? "…" : "")}</Text>
          : <FormattedContent text={msg.content || (msg.isStreaming ? "…" : "")} />
        }
      </Box>
    </Box>
  );
}

function ModelPicker({
  current,
  onSelect,
  onCancel,
}: {
  current: string;
  onSelect: (m: string) => void;
  onCancel: () => void;
}) {
  useInput((_input, key) => { if (key.escape) onCancel(); });

  const options = ALL_MODEL_IDS.map(m => ({
    label: (MODEL_LABELS[m] ?? m) + (m === current ? "  (current)" : ""),
    value: m,
  }));

  return (
    <Box flexDirection="column" borderStyle="round" borderColor="cyan" paddingX={2} paddingY={1}>
      <Text bold color="cyan"> Select model</Text>
      <Text color="gray"> ↑↓ navigate · type to filter · enter select · esc cancel</Text>
      <Select options={options} defaultValue={current} onChange={onSelect} visibleOptionCount={8} />
    </Box>
  );
}

function formatToolDisplay(name: string, args: Record<string, any>): { header: string; lines: string[] } {
  const header = name;
  const lines: string[] = [];

  const show = (key: string, label?: string) => {
    const v = args[key];
    if (v === undefined || v === null) return;
    if (typeof v === "string" && v.length > 300) {
      lines.push(`  ${label ?? key}: ${v.slice(0, 300)}…`);
    } else if (typeof v === "object") {
      lines.push(`  ${label ?? key}: ${JSON.stringify(v).slice(0, 200)}`);
    } else {
      lines.push(`  ${label ?? key}: ${String(v)}`);
    }
  };

  switch (name) {
    case "shell":
      if (args.command) lines.push(`  ${args.command}`);
      else show("command");
      break;
    case "read_file":
      show("file_path", "file");
      show("offset");
      show("limit");
      break;
    case "write_file":
      show("file_path", "file");
      if (args.content) lines.push(`  content: ${String(args.content).slice(0, 200)}…`);
      break;
    case "edit_file":
      show("file_path", "file");
      show("old_string", "replace");
      show("new_string", "with");
      break;
    case "list_dir":
      show("path");
      break;
    case "grep":
      show("pattern");
      show("path", "in");
      break;
    case "glob":
      show("pattern");
      show("path", "in");
      break;
    case "git_diff": case "git_log": case "git_status":
      show("repo_path", "repo");
      if (Object.keys(args).length === 0) lines.push("  (no args)");
      break;
    case "web_fetch":
      show("url");
      show("prompt");
      break;
    case "web_search":
      show("query");
      break;
    case "read_json":
      show("file_path", "file");
      show("query");
      break;
    case "run_tests": case "typecheck": case "package_info":
      if (Object.keys(args).length === 0) lines.push("  (no args)");
      else for (const [k, v] of Object.entries(args)) lines.push(`  ${k}: ${String(v).slice(0, 100)}`);
      break;
    case "ask_user":
      show("question");
      break;
    case "write_plan":
      show("plan", "plan preview");
      break;
    case "remember":
      show("fact");
      break;
    case "stack_detect":
      show("packageJson", "package.json");
      show("files", "file list");
      break;
    case "element_semantic":
      show("tag");
      show("attrs", "attributes");
      break;
    case "ngmi_ascii":
      show("text");
      show("font");
      break;
    case "autoschema_discover":
      if (Array.isArray(args.files)) lines.push(`  files: ${args.files.length} file(s)`);
      else show("files");
      break;
    case "factsearch_parse":
      show("llm_response", "response");
      show("url");
      break;
    case "clickhouse_search":
      show("database"); show("table"); show("searchColumns", "search cols"); show("query");
      break;
    case "clickhouse_preview":
      show("database"); show("table"); show("columns"); show("limit");
      break;
    case "clickhouse_schema":
      show("database"); show("table");
      break;
    case "clickhouse_relations":
      show("database"); show("tables", "compare tables");
      break;
    case "clickhouse_aggregate":
      show("database"); show("table"); show("groupBy", "group by"); show("aggregations", "aggs");
      break;
    case "clickhouse_query":
      show("database"); show("table"); show("selectColumns", "columns"); show("where");
      break;
    case "admin_r2_upload": case "admin_r2_put":
      show("key");
      break;
    case "admin_r2_delete":
      show("key");
      break;
    default:
      for (const [k, v] of Object.entries(args)) {
        lines.push(`  ${k}: ${typeof v === "string" ? v.slice(0, 200) : JSON.stringify(v).slice(0, 200)}`);
      }
  }

  return { header, lines };
}

function ToolConfirm({
  toolCall,
  onApprove,
  onDeny,
}: {
  toolCall: ToolCall;
  onApprove: () => void;
  onDeny: () => void;
}) {
  let args: any = {};
  try { args = JSON.parse(toolCall.arguments); } catch {}
  const display = formatToolDisplay(toolCall.name, args);

  return (
    <Box flexDirection="column" borderStyle="round" borderColor="yellow" paddingX={2} paddingY={1}>
      <Text bold color="yellow"> ⚠  Tool: {display.header}</Text>
      <Text> </Text>
      {display.lines.map((line, i) => (
        <Text key={i} color="white">{line}</Text>
      ))}
      <Text> </Text>
      <ConfirmInput onConfirm={onApprove} onCancel={onDeny} />
    </Box>
  );
}

// ─── Tool grouping for catalog / palette ──────────────────────────────────────

interface ToolItem { name: string; args: string; category: string }
interface CatGroup { section: string; categories: { name: string; tools: ToolItem[] }[] }

const TOOL_GROUPS = [
  {
    section: "Orchestra TUI",
    categories: [
      { name: "io", tools: ["read_file", "write_file", "edit_file", "list_dir", "grep", "glob"] },
      { name: "shell", tools: ["shell"] },
      { name: "git", tools: ["git_diff", "git_log", "git_status"] },
      { name: "web", tools: ["web_fetch", "web_search"] },
      { name: "project", tools: ["read_json", "run_tests", "typecheck", "package_info"] },
      { name: "meta", tools: ["ask_user", "write_plan", "remember"] },
      { name: "betterux", tools: ["element_inspect", "element_uilang", "element_componentize", "element_edit", "stack_detect", "element_semantic"] },
      { name: "admin", tools: ["admin_users_list", "admin_user_create", "admin_user_update", "admin_user_disable", "admin_user_delete_plan", "admin_user_hard_delete", "admin_user_files", "admin_user_files_delete_plan", "admin_user_files_hard_delete", "admin_d1_tables", "admin_d1_schema", "admin_d1_rows", "admin_d1_select", "clickhouse_search", "clickhouse_preview", "clickhouse_schema", "clickhouse_relations", "clickhouse_aggregate", "clickhouse_query", "admin_r2_list", "admin_r2_metadata", "admin_r2_content", "admin_r2_upload", "admin_r2_put", "admin_r2_delete", "admin_audit_log"] },
    ],
  },
  {
    section: "Surface (ontologer)",
    categories: [
      { name: "compute", tools: ["ngmi_ascii", "autoschema_discover", "factsearch_parse", "polymarket_scorer", "polymarket_ctf"] },
    ],
  },
];

function buildCatGroups(filter: string): CatGroup[] {
  const f = filter.toLowerCase();
  const results: CatGroup[] = [];

  for (const group of TOOL_GROUPS) {
    const cats: CatGroup["categories"] = [];
    for (const cat of group.categories) {
      const matching = cat.tools
        .filter(t => !f || t.includes(f))
        .map(t => {
          const mod = TOOL_MAP.get(t);
          const required = (mod?.inputSchema?.required as string[]) ?? [];
          const args = required.length > 0 ? required.join(", ") : "(no required args)";
          return { name: t, args, category: cat.name };
        });
      if (matching.length > 0) cats.push({ name: cat.name, tools: matching });
    }
    if (cats.length > 0) results.push({ section: group.section, categories: cats });
  }

  return results;
}

function ToolsView({
  filter,
  onSelect,
  onCancel,
  maxRows,
}: {
  filter: string;
  onSelect?: (slug: string) => void;
  onCancel: () => void;
  maxRows: number;
}) {
  const groups = buildCatGroups(filter);

  // Flatten categories across all groups for left-panel navigation
  interface CatEntry { section: string; catName: string; toolCount: number }
  const catList: CatEntry[] = [];
  for (const g of groups) {
    for (const c of g.categories) {
      catList.push({ section: g.section, catName: c.name, toolCount: c.tools.length });
    }
  }

  const [catIdx, setCatIdx] = useState(0);
  const [toolIdx, setToolIdx] = useState(0);
  const [panel, setPanel] = useState<"left" | "right">("left");
  const isSelectable = !!onSelect;
  const visible = maxRows - 6;

  // Current category's tools
  const curCat = catList[catIdx];
  const curTools = curCat
    ? groups.find(g => g.section === curCat.section)?.categories.find(c => c.name === curCat.catName)?.tools ?? []
    : [];

  // Clamp toolIdx when category changes
  if (toolIdx >= curTools.length) setToolIdx(Math.max(0, curTools.length - 1));

  useInput((input, key) => {
    if (key.escape) { onCancel(); return; }
    if (key.leftArrow) { setPanel("left"); return; }
    if (key.rightArrow) { setPanel(isSelectable ? "right" : "left"); return; }
    if (key.downArrow) {
      if (panel === "left") setCatIdx(i => Math.min(catList.length - 1, i + 1));
      else setToolIdx(i => Math.min(curTools.length - 1, i + 1));
    }
    if (key.upArrow) {
      if (panel === "left") setCatIdx(i => Math.max(0, i - 1));
      else setToolIdx(i => Math.max(0, i - 1));
    }
    if (key.return && isSelectable && curTools.length > 0) {
      onSelect(curTools[panel === "right" ? toolIdx : 0]?.name ?? curTools[0].name);
    }
  });

  // Left panel width: 30 chars for category names
  const leftW = 28;

  return (
    <Box flexDirection="column" borderStyle="round" borderColor="cyan" paddingX={1} paddingY={1}>
      <Box>
        <Text bold color="cyan">{isSelectable ? " Tool Palette" : " Tools Catalog"}</Text>
        {filter && <Text color="gray">  filter: {filter}</Text>}
        <Text color="gray">  ({catList.reduce((s, c) => s + c.toolCount, 0)} tools)</Text>
      </Box>
      <Text color="gray"> ←→ panel  ↑↓ navigate  enter {isSelectable ? "select" : "close"}  esc close</Text>
      <Text> </Text>
      <Box flexDirection="row">
        {/* Left panel: categories */}
        <Box flexDirection="column" width={leftW} borderStyle="round" borderColor="gray" paddingX={1}>
          {catList.map((cat, i) => {
            const isActive = i === catIdx;
            let shownSection = "";
            if (i === 0 || cat.section !== catList[i - 1]?.section) {
              shownSection = cat.section;
            }
            return (
              <Box key={`${cat.section}/${cat.catName}`} flexDirection="column">
                {shownSection !== "" && (
                  <Box marginTop={i > 0 ? 1 : 0}>
                    <Text color="magenta" bold>── {shownSection}</Text>
                  </Box>
                )}
                <Box marginLeft={2}>
                  <Text color={isActive && panel === "left" ? "cyan" : "gray"}>
                    {isActive && panel === "left" ? "▶ " : "  "}
                  </Text>
                  <Text color={isActive ? "white" : "gray"} bold={isActive}>
                    {cat.catName}/
                  </Text>
                  <Text color="gray" dimColor> ({cat.toolCount})</Text>
                </Box>
              </Box>
            );
          })}
        </Box>
        <Text>  </Text>
        {/* Right panel: tools in selected category */}
        <Box flexDirection="column" flexGrow={1}>
          {curCat && (
            <Box marginBottom={1}>
              <Text color="gray" dimColor>{curCat.section} / {curCat.catName}/</Text>
            </Box>
          )}
          {curTools.slice(0, visible).map((t, i) => {
            const isActive = i === toolIdx && panel === "right";
            return (
              <Box key={t.name}>
                <Text color={isActive ? "cyan" : "gray"}>{isActive ? "▶ " : "  "}</Text>
                <Text color={isActive ? "white" : "gray"} bold={isActive}>
                  ${t.name}
                </Text>
                <Text color="gray" dimColor>  {t.args}</Text>
              </Box>
            );
          })}
          {curTools.length > visible && (
            <Text color="gray" dimColor>  … {curTools.length - visible} more ↓ …</Text>
          )}
        </Box>
      </Box>
    </Box>
  );
}

function StatusBar({ tokensIn, tokensOut, msgCount }: { tokensIn: number; tokensOut: number; msgCount: number }) {
  return (
    <Box>
      <Text color="gray">  ↑{tokensIn} ↓{tokensOut} tokens  ·  {msgCount} messages</Text>
    </Box>
  );
}

function SplashScreen({ onEnter, width }: { onEnter: () => void; width: number }) {
  useInput((_input, _key) => { onEnter(); });

  return (
    <Box flexDirection="column" alignItems="center" justifyContent="center" height={24}>
      <Text>{"\n"}</Text>
      {width >= 60 ? (
        <Gradient name="atlas">
          <BigText text="ORCHAESTRA" font="ansi shadow" />
        </Gradient>
      ) : (
        <Gradient name="atlas">
          <BigText text=" ORCH" font="simple" />
        </Gradient>
      )}
      <Text> </Text>
      <Box flexDirection="column" alignItems="center">
        <Text color="white" bold> Terminal AI Orchestrator</Text>
        <Text color="gray"> v0.2.0  ·  GPT-5.5 / DeepSeek V4 / GLM 4.7 / Image Gen</Text>
        <Text> </Text>
        <Text color="yellow"> Press any key to continue</Text>
      </Box>
    </Box>
  );
}

function ConfigMenu({
  config,
  onSave,
  onCancel,
}: {
  config: UserConfig;
  onSave: (cfg: UserConfig) => void;
  onCancel: () => void;
}) {
  const [planIdx, setPlanIdx] = useState(
    config.planModel ? Math.max(0, ALL_MODEL_IDS.indexOf(config.planModel)) : 0,
  );
  const [execIdx, setExecIdx] = useState(
    config.execModel ? Math.max(0, ALL_MODEL_IDS.indexOf(config.execModel)) : 0,
  );
  const [fieldIdx, setFieldIdx] = useState(0);
  const fields = ["plan-model", "exec-model", "save"];

  useInput((input, key) => {
    if (key.escape || input === "q") { onCancel(); return; }
    if (key.upArrow) setFieldIdx(i => Math.max(0, i - 1));
    if (key.downArrow) setFieldIdx(i => Math.min(fields.length - 1, i + 1));
    if (key.return || input === " ") {
      if (fields[fieldIdx] === "plan-model") setPlanIdx(i => (i + 1) % ALL_MODEL_IDS.length);
      else if (fields[fieldIdx] === "exec-model") setExecIdx(i => (i + 1) % ALL_MODEL_IDS.length);
      else if (fields[fieldIdx] === "save") {
        onSave({ planModel: ALL_MODEL_IDS[planIdx], execModel: ALL_MODEL_IDS[execIdx] });
      }
    }
  });

  return (
    <Box flexDirection="column" borderStyle="round" borderColor="cyan" paddingX={2} paddingY={1}>
      <Text bold color="cyan"> ⚙  Configuration</Text>
      <Text color="gray"> ↑↓ navigate  ·  enter/space toggle  ·  esc back</Text>
      <Text> </Text>

      <Box>
        <Text color={fieldIdx === 0 ? "cyan" : "gray"}>
          {fieldIdx === 0 ? "▶ " : "  "}
        </Text>
        <Box flexDirection="column">
          <Text color={fieldIdx === 0 ? "white" : "gray"} bold={fieldIdx === 0}>
            Plan Model
          </Text>
          <Text color="gray">  {MODEL_LABELS[ALL_MODEL_IDS[planIdx]] ?? ALL_MODEL_IDS[planIdx]}</Text>
        </Box>
      </Box>

      <Text> </Text>

      <Box>
        <Text color={fieldIdx === 1 ? "cyan" : "gray"}>
          {fieldIdx === 1 ? "▶ " : "  "}
        </Text>
        <Box flexDirection="column">
          <Text color={fieldIdx === 1 ? "white" : "gray"} bold={fieldIdx === 1}>
            Exec Model
          </Text>
          <Text color="gray">  {MODEL_LABELS[ALL_MODEL_IDS[execIdx]] ?? ALL_MODEL_IDS[execIdx]}</Text>
        </Box>
      </Box>

      <Text> </Text>

      <Box>
        <Text color={fieldIdx === 2 ? "green" : "gray"}>
          {fieldIdx === 2 ? "▶ " : "  "}
        </Text>
        <Text color={fieldIdx === 2 ? "green" : "gray"} bold={fieldIdx === 2}>
          Save &amp; Exit
        </Text>
        <Text color="gray">  (writes user/user.json)</Text>
      </Box>
    </Box>
  );
}

function CommandPalette({
  currentModel,
  onSelectModel,
  onToggleMode,
  onConfig,
  onClear,
  onExit,
  onCancel,
}: {
  currentModel: string;
  onSelectModel: (m: string) => void;
  onToggleMode: () => void;
  onConfig: () => void;
  onClear: () => void;
  onExit: () => void;
  onCancel: () => void;
}) {
  const [level, setLevel] = useState<"commands" | "model-picker">("commands");

  useInput((_input, key) => { if (key.escape) level === "model-picker" ? setLevel("commands") : onCancel(); });

  const cmdOptions = COMMANDS.map(c => ({ label: `/${c.id}  —  ${c.label}`, value: c.id }));

  const handleCmd = (value: string) => {
    if (value === "model") setLevel("model-picker");
    else if (value === "toggle") onToggleMode();
    else if (value === "config") onConfig();
    else if (value === "clear") onClear();
    else if (value === "exit") onExit();
  };

  if (level === "model-picker") {
    const modelOpts = ALL_MODEL_IDS.map(m => ({
      label: (MODEL_LABELS[m] ?? m) + (m === currentModel ? "  (current)" : ""),
      value: m,
    }));
    return (
      <Box flexDirection="column" borderStyle="round" borderColor="cyan" paddingX={2} paddingY={1}>
        <Text bold color="cyan"> Select Model</Text>
        <Text color="gray"> type to filter · enter select · esc back</Text>
        <Select options={modelOpts} defaultValue={currentModel} onChange={onSelectModel} visibleOptionCount={10} />
      </Box>
    );
  }

  return (
    <Box flexDirection="column" borderStyle="round" borderColor="cyan" paddingX={2} paddingY={1}>
      <Text bold color="cyan"> Commands</Text>
      <Text color="gray"> type to filter · enter select · esc cancel</Text>
      <Select options={cmdOptions} onChange={handleCmd} visibleOptionCount={6} />
    </Box>
  );
}

// ─── Main App ─────────────────────────────────────────────────────────────────

function App() {
  const { exit } = useApp();
  const { stdout } = useStdout();
  const [termRows, setTermRows] = useState(stdout?.rows ?? process.stdout.rows ?? 24);
  const [termCols, setTermCols] = useState(stdout?.columns ?? process.stdout.columns ?? 80);

  useEffect(() => {
    const onResize = () => {
      if (stdout) {
        setTermRows(stdout.rows);
        setTermCols(stdout.columns);
      }
    };
    process.stdout.on("resize", onResize);
    return () => { process.stdout.off("resize", onResize); };
  }, [stdout]);

  const userConfig = loadConfig();
  const planModel = userConfig.planModel && ALL_MODEL_IDS.includes(userConfig.planModel)
    ? userConfig.planModel : ALL_MODEL_IDS[0] ?? "glm-4.7-flash";
  const execModel = userConfig.execModel && ALL_MODEL_IDS.includes(userConfig.execModel)
    ? userConfig.execModel : ALL_MODEL_IDS[0] ?? "glm-4.7-flash";

  const [activeMode, setActiveMode] = useState<"plan" | "exec">("exec");
  const [model, setModel] = useState<string>(execModel);
  const [messages, setMessages] = useState<Message[]>([
    { id: "sys", role: "system", content: "You are a coding and data analysis tool. Rules: (1) No emojis. (2) No verbose language. Keep facts short. No extra words. (3) Talk in imperative without full sentences. Bad: \"Ah yes! You're absolutely right.\" Good: \"Correct.\" Bad: \"Let me also check the database.\" Good: \"Checking database.\" (4) When using tools: call tool → read result → IMMEDIATELY call next tool. Chain: schema → preview → search → analyze. Never stop after one result — keep drilling until question is answered." },
  ]);
  const [input, setInput] = useState("");
  const [mode, setMode] = useState<AppMode>("splash");
  const [isLoading, setIsLoading] = useState(false);
  const [pendingTool, setPendingTool] = useState<ToolCall | null>(null);
  const [tokensIn, setTokensIn] = useState(0);
  const [tokensOut, setTokensOut] = useState(0);
  const [error, setError] = useState<string | null>(null);

  const streamingIdRef = useRef<string | null>(null);

  const toggleActiveMode = useCallback(() => {
    setActiveMode(prev => {
      const next = prev === "plan" ? "exec" : "plan";
      setModel(next === "plan" ? planModel : execModel);
      return next;
    });
  }, [planModel, execModel]);

  const visibleMessages = messages.filter(m => m.role !== "system");

  const MAX_MESSAGES = 150;

  function addMessage(msg: Omit<Message, "id">) {
    const id = randomId();
    setMessages(prev => {
      const next = [...prev, { ...msg, id }];
      // Keep system prompt + last N messages to cap memory
      if (next.length > MAX_MESSAGES) {
        const systemMsgs = next.filter(m => m.role === "system");
        const rest = next.filter(m => m.role !== "system");
        const trimmed = rest.slice(rest.length - (MAX_MESSAGES - systemMsgs.length));
        return [...systemMsgs, ...trimmed];
      }
      return next;
    });
    return id;
  }

  function updateMessage(id: string, update: Partial<Message>) {
    setMessages(prev => prev.map(m => m.id === id ? { ...m, ...update } : m));
  }

  const sendMessage = useCallback(async (userText: string) => {
    if (!userText.trim() || isLoading) return;
    setError(null);

    // Commands
    if (userText.startsWith("/")) {
      const cmd = userText.trim().toLowerCase();
      if (cmd === "/model") { setMode("model-picker"); return; }
      if (cmd === "/plan") { if (activeMode !== "plan") toggleActiveMode(); return; }
      if (cmd === "/exec") { if (activeMode !== "exec") toggleActiveMode(); return; }
      if (cmd === "/config") { setMode("config"); return; }
      if (cmd === "/exit" || cmd === "/quit") { exit(); return; }
      if (cmd === "/clear") { setMessages(prev => prev.filter(m => m.role === "system")); return; }
      if (cmd === "/tools") { setMode("tools-catalog"); return; }
      setError(`Unknown command: ${userText}`);
      return;
    }

    // $toolname hint — tell the model which tool to use
    let toolHint: string | null = null;
    let contentText = userText;
    if (userText.startsWith("$")) {
      const space = userText.indexOf(" ");
      const slug = space > 0 ? userText.slice(1, space) : userText.slice(1);
      if (TOOL_MAP.has(slug)) {
        toolHint = slug;
        contentText = space > 0 ? userText.slice(space + 1) : "";
      }
    }

    addMessage({ role: "user", content: userText });
    setIsLoading(true);

    const history = messages
      .map(m => {
        const h: any = { role: m.role, content: m.content };
        if (m.role === "tool" && m.toolCallId) h.tool_call_id = m.toolCallId;
        if (m.role === "assistant" && m.toolCalls?.length) {
          h.tool_calls = m.toolCalls.map(tc => ({
            id: tc.id, type: "function",
            function: { name: tc.name, arguments: tc.arguments },
          }));
        }
        return h;
      });
    if (toolHint) {
      history.push({ role: "system", content: `Use the ${toolHint} tool to handle the following request.` });
    }
    history.push({ role: "user", content: contentText });

    const assistantId = randomId();
    setMessages(prev => [...prev, {
      id: assistantId, role: "assistant", content: "", model, isStreaming: true,
    }]);
    streamingIdRef.current = assistantId;

    const cb: StreamCallbacks = {
      onDelta(text) {
        setMessages(prev => prev.map(m =>
          m.id === assistantId ? { ...m, content: m.content + text } : m
        ));
      },
      onToolCall(tc) {
        setPendingTool(tc);
        setMessages(prev => prev.map(m =>
          m.id === assistantId ? { ...m, toolCalls: [...(m.toolCalls ?? []), tc] } : m
        ));
        setMode("confirm-tool");
      },
      onImage(paths, revised) {
        addMessage({ role: "tool", content: "Image generated", imagePaths: paths, revisedPrompt: revised });
        setMessages(prev => prev.map(m =>
          m.id === assistantId ? { ...m, isStreaming: false, content: m.content || "Image generated" } : m
        ));
        setIsLoading(false);
        streamingIdRef.current = null;
      },
      onDone(usage) {
        setMessages(prev => {
          const msg = prev.find(m => m.id === assistantId);
          const estOut = msg ? Math.ceil(msg.content.length / 4) : 0;
          if (usage) { setTokensIn(n => n + usage.input); setTokensOut(n => n + usage.output); }
          else { setTokensIn(n => n + Math.ceil(userText.length / 4)); setTokensOut(n => n + estOut); }
          return prev.map(m => m.id === assistantId ? { ...m, isStreaming: false } : m);
        });
        setIsLoading(false);
        streamingIdRef.current = null;
      },
      onError(err) {
        setError(err);
        setMessages(prev => prev.map(m =>
          m.id === assistantId ? { ...m, content: `Error: ${err}`, isStreaming: false } : m
        ));
        setIsLoading(false);
      },
    };

    await streamModel(model, history as any, cb, REGISTRY);
  }, [messages, model, isLoading, exit]);

  const handleToolApprove = useCallback(async () => {
    if (!pendingTool) return;
    setMode("chat");

    const toolMsgId = addMessage({
      role: "tool", content: `…running ${pendingTool.name}…`,
      toolName: pendingTool.name, toolCallId: pendingTool.id,
    });

    const output = await executeTool(pendingTool.name, pendingTool.arguments);

    updateMessage(toolMsgId, { content: output, toolCallId: pendingTool.id });

    // Continue conversation with tool result
    const history = messages.map(m => {
      const h: any = { role: m.role, content: m.content };
      if (m.role === "tool" && m.toolCallId) h.tool_call_id = m.toolCallId;
      if (m.role === "assistant" && m.toolCalls?.length) {
        h.tool_calls = m.toolCalls.map(tc => ({
          id: tc.id, type: "function",
          function: { name: tc.name, arguments: tc.arguments },
        }));
      }
      return h;
    });
    history.push({ role: "tool" as any, content: output, tool_call_id: pendingTool.id });

    const continueId = randomId();
    setMessages(prev => [...prev, { id: continueId, role: "assistant", content: "", model, isStreaming: true }]);
    setIsLoading(true);

    const cb: StreamCallbacks = {
      onDelta(text) {
        setMessages(prev => prev.map(m => m.id === continueId ? { ...m, content: m.content + text } : m));
      },
      onToolCall(tc) { setPendingTool(tc); setMode("confirm-tool"); },
      onImage(paths, revised) {
        addMessage({ role: "tool", content: "Image generated", imagePaths: paths, revisedPrompt: revised });
        setMessages(prev => prev.map(m =>
          m.id === continueId ? { ...m, isStreaming: false, content: m.content || "Image generated" } : m
        ));
        setIsLoading(false);
        setPendingTool(null);
      },
      onDone(usage) {
        setMessages(prev => {
          const msg = prev.find(m => m.id === continueId);
          const estOut = msg ? Math.ceil(msg.content.length / 4) : 0;
          if (usage) { setTokensIn(n => n + usage.input); setTokensOut(n => n + usage.output); }
          else { setTokensOut(n => n + estOut); }
          return prev.map(m => m.id === continueId ? { ...m, isStreaming: false } : m);
        });
        setIsLoading(false);
        setPendingTool(null);
      },
      onError(err) {
        setError(err);
        setMessages(prev => prev.map(m => m.id === continueId ? { ...m, content: `Error: ${err}`, isStreaming: false } : m));
        setIsLoading(false);
      },
    };

    await streamModel(model, history as any, cb, REGISTRY);
  }, [pendingTool, messages, model]);

  const handleToolDeny = useCallback(() => {
    setPendingTool(null);
    setMode("chat");
    setIsLoading(false);
    const assistantId = streamingIdRef.current;
    if (assistantId) {
      setMessages(prev => prev.map(m =>
        m.id === assistantId ? { ...m, isStreaming: false, content: m.content || "(tool denied)" } : m
      ));
    }
    addMessage({ role: "user", content: "(tool use denied by user)" });
  }, [pendingTool]);

  const handleInputChange = useCallback((val: string) => {
    if (val === "/" && mode === "chat" && !isLoading) {
      setMode("command-palette");
      setInput("");
      return;
    }
    if (val === "$" && mode === "chat" && !isLoading) {
      setMode("tool-palette");
      setInput("$");
      return;
    }
    // Keep palette open while typing filter after $
    if (mode === "tool-palette") {
      setInput(val);
      return;
    }
    setInput(val);
  }, [mode, isLoading]);

  const handleSubmit = useCallback((val: string) => {
    setInput("");
    sendMessage(val);
  }, [sendMessage]);

  if (mode === "splash") {
    return <SplashScreen onEnter={() => setMode("chat")} width={termCols} />;
  }

  return (
    <Box flexDirection="column" width={termCols} height={termRows - 1}>
      <Header model={model} activeMode={activeMode} width={termCols} />

      {/* Messages */}
      <Box flexDirection="column" flexGrow={1}>
        {visibleMessages.map(msg => (
          <MessageBubble key={msg.id} msg={msg} />
        ))}
      </Box>

      {/* Error */}
      {error && (
        <Box marginLeft={2}>
          <StatusMessage variant="error">{error}</StatusMessage>
        </Box>
      )}

      {/* Overlays */}
      {mode === "model-picker" && (
        <ModelPicker
          current={model}
          onSelect={m => { setModel(m); setMode("chat"); }}
          onCancel={() => setMode("chat")}
        />
      )}

      {mode === "confirm-tool" && pendingTool && (
        <ToolConfirm
          toolCall={pendingTool}
          onApprove={handleToolApprove}
          onDeny={handleToolDeny}
        />
      )}

      {mode === "config" && (
        <ConfigMenu
          config={{ planModel, execModel }}
          onSave={(cfg) => {
            saveConfig(cfg);
            if (cfg.planModel) setModel(activeMode === "plan" ? cfg.planModel : model);
            if (cfg.execModel) setModel(activeMode === "exec" ? cfg.execModel : model);
            setMode("chat");
          }}
          onCancel={() => setMode("chat")}
        />
      )}

      {mode === "command-palette" && (
        <CommandPalette
          currentModel={model}
          onSelectModel={m => { setModel(m); setMode("chat"); }}
          onToggleMode={toggleActiveMode}
          onConfig={() => setMode("config")}
          onClear={() => { setMessages(prev => prev.filter(m => m.role === "system")); setMode("chat"); }}
          onExit={() => exit()}
          onCancel={() => setMode("chat")}
        />
      )}

      {mode === "tools-catalog" && (
        <ToolsView
          filter=""
          onCancel={() => setMode("chat")}
          maxRows={termRows}
        />
      )}

      {mode === "tool-palette" && (
        <ToolsView
          filter={input.startsWith("$") ? input.slice(1) : ""}
          onSelect={(slug) => {
            setInput(`$${slug} `);
            setMode("chat");
          }}
          onCancel={() => { setInput(""); setMode("chat"); }}
          maxRows={termRows}
        />
      )}

      {/* Input */}
      {(mode === "chat" || mode === "tool-palette") && (
        <Box flexDirection="column">
          <Text color="gray">{BORDER.repeat(termCols)}</Text>
          <Box>
            <Text color={isLoading ? "gray" : "yellow"}>{isLoading ? "  " : "  › "}</Text>
            {isLoading ? (
              <Spinner label="thinking" />
            ) : (
              <TextInput
                value={input}
                onChange={handleInputChange}
                onSubmit={mode === "tool-palette" ? () => { setInput(""); setMode("chat"); } : handleSubmit}
                placeholder={mode === "tool-palette" ? "filter tools…  esc to close" : "message…  / for commands  $ for tools"}
              />
            )}
          </Box>
          <StatusBar tokensIn={tokensIn} tokensOut={tokensOut} msgCount={visibleMessages.length} />
        </Box>
      )}
    </Box>
  );
}

// ─── Entry ────────────────────────────────────────────────────────────────────

render(<App />);
