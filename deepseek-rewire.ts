#!/usr/bin/env node
import React, { useState, useEffect, useRef, useCallback } from "react";
import { render, Box, Text, useInput, useApp, Static } from "ink";
import TextInput from "ink-text-input";
import { readFileSync, existsSync } from "fs";
import { homedir } from "os";
import { join } from "path";
import { exec } from "child_process";
import { promisify } from "util";

const execAsync = promisify(exec);

// ─── Types ────────────────────────────────────────────────────────────────────

type Role = "user" | "assistant" | "system" | "tool";
type Model = "gpt-5.5" | "gpt-5.4" | "deepseek-v4-pro" | "deepseek-v4-flash";

interface Message {
  id: string;
  role: Role;
  content: string;
  model?: string;
  toolName?: string;
  isStreaming?: boolean;
}

interface ToolCall {
  id: string;
  name: string;
  arguments: string;
}

type AppMode = "chat" | "confirm-tool" | "model-picker";

// ─── Config ───────────────────────────────────────────────────────────────────

const PROXY_BASE = process.env.PROXY_BASE ?? "http://localhost:3100";
const OPENAI_BASE = "https://api.openai.com/v1";
const CHATGPT_BASE = "https://chatgpt.com/backend-api";
const AUTH_PATH = join(homedir(), ".codex", "auth.json");
const DEEPSEEK_KEY = process.env.DEEPSEEK_API_KEY ?? "";

const GPT_MODELS: Model[] = ["gpt-5.5", "gpt-5.4"];
const DS_MODELS: Model[] = ["deepseek-v4-pro", "deepseek-v4-flash"];
const ALL_MODELS: Model[] = [...GPT_MODELS, ...DS_MODELS];

const MODEL_LABELS: Record<Model, string> = {
  "gpt-5.5": "GPT-5.5  (ChatGPT OAuth)",
  "gpt-5.4": "GPT-5.4  (ChatGPT OAuth)",
  "deepseek-v4-pro": "DeepSeek V4 Pro  (proxy)",
  "deepseek-v4-flash": "DeepSeek V4 Flash  (proxy)",
};

function isGPT(m: Model) { return GPT_MODELS.includes(m); }

// ─── Auth ─────────────────────────────────────────────────────────────────────

function loadOAuthToken(): string | null {
  try {
    if (!existsSync(AUTH_PATH)) return null;
    const auth = JSON.parse(readFileSync(AUTH_PATH, "utf8"));
    return auth?.tokens?.access_token ?? null;
  } catch { return null; }
}

// ─── API calls ────────────────────────────────────────────────────────────────

function randomId() { return Math.random().toString(36).slice(2, 10); }

interface StreamCallbacks {
  onDelta: (text: string) => void;
  onToolCall: (tc: ToolCall) => void;
  onDone: (usage?: { input: number; output: number }) => void;
  onError: (err: string) => void;
}

async function streamGPT(
  model: Model,
  messages: Array<{ role: string; content: string }>,
  cb: StreamCallbacks,
) {
  const token = loadOAuthToken();
  if (!token) { cb.onError("No ChatGPT OAuth token — run: codex login"); return; }

  const body = {
    model,
    messages,
    stream: true,
    max_tokens: 8192,
    tools: [
      {
        type: "function",
        function: {
          name: "shell",
          description: "Run a shell command in the current directory",
          parameters: {
            type: "object",
            properties: {
              command: { type: "string", description: "The shell command to run" },
            },
            required: ["command"],
          },
        },
      },
    ],
  };

  let resp: Response;
  try {
    resp = await fetch(`${OPENAI_BASE}/responses`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({ ...body, stream: true }),
    });
    // Fallback to chat completions if responses not available
    if (resp.status === 404) {
      resp = await fetch(`${OPENAI_BASE}/chat/completions`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
        body: JSON.stringify(body),
      });
    }
  } catch (e: any) { cb.onError(e.message); return; }

  if (!resp.ok) {
    const t = await resp.text();
    cb.onError(`GPT ${resp.status}: ${t.slice(0, 120)}`);
    return;
  }

  await parseChatSSE(resp, cb);
}

async function streamDeepSeek(
  model: Model,
  messages: Array<{ role: string; content: string }>,
  cb: StreamCallbacks,
) {
  const body = {
    model,
    messages,
    stream: true,
    max_tokens: 8192,
    thinking: { type: "disabled" },
    tools: [
      {
        type: "function",
        function: {
          name: "shell",
          description: "Run a shell command in the current directory",
          parameters: {
            type: "object",
            properties: {
              command: { type: "string", description: "The shell command to run" },
            },
            required: ["command"],
          },
        },
      },
    ],
  };

  let resp: Response;
  try {
    resp = await fetch(`${PROXY_BASE}/v1/chat/completions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${DEEPSEEK_KEY}`,
      },
      body: JSON.stringify(body),
    });
    // If proxy doesn't have chat endpoint, hit DeepSeek directly
    if (!resp.ok) {
      resp = await fetch("https://api.deepseek.com/v1/chat/completions", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${DEEPSEEK_KEY}` },
        body: JSON.stringify(body),
      });
    }
  } catch (e: any) { cb.onError(e.message); return; }

  if (!resp.ok) {
    const t = await resp.text();
    cb.onError(`DeepSeek ${resp.status}: ${t.slice(0, 120)}`);
    return;
  }

  await parseChatSSE(resp, cb);
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
const WIDTH = 80;

function Header({ model }: { model: Model }) {
  const tag = isGPT(model) ? "oauth" : "proxy";
  const tagColor = isGPT(model) ? "green" : "cyan";
  return (
    <Box flexDirection="column">
      <Text color="gray">{BORDER.repeat(WIDTH)}</Text>
      <Box justifyContent="space-between">
        <Text bold color="white"> ◈ rewire</Text>
        <Text> </Text>
        <Box>
          <Text color="white">{model}  </Text>
          <Text color={tagColor}>[{tag}]</Text>
          <Text color="gray">  /model to change · /exit to quit</Text>
        </Box>
      </Box>
      <Text color="gray">{BORDER.repeat(WIDTH)}</Text>
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
          <Text color="gray">{msg.content}</Text>
        </Box>
      </Box>
    );
  }

  const isUser = msg.role === "user";
  return (
    <Box flexDirection="column" marginBottom={1}>
      <Box marginLeft={isUser ? 2 : 0}>
        <Text color={isUser ? "yellow" : "cyan"} bold>
          {isUser ? "you" : (msg.model ?? "ai")}
        </Text>
        {msg.isStreaming && <Text color="gray"> ●</Text>}
      </Box>
      <Box marginLeft={isUser ? 4 : 2} marginRight={isUser ? 0 : 4}>
        <Text color={isUser ? "white" : "white"} wrap="wrap">
          {msg.content || (msg.isStreaming ? "…" : "")}
        </Text>
      </Box>
    </Box>
  );
}

function ModelPicker({
  current,
  onSelect,
  onCancel,
}: {
  current: Model;
  onSelect: (m: Model) => void;
  onCancel: () => void;
}) {
  const [idx, setIdx] = useState(ALL_MODELS.indexOf(current));

  useInput((input, key) => {
    if (key.upArrow) setIdx(i => Math.max(0, i - 1));
    if (key.downArrow) setIdx(i => Math.min(ALL_MODELS.length - 1, i + 1));
    if (key.return) onSelect(ALL_MODELS[idx]);
    if (key.escape || input === "q") onCancel();
  });

  return (
    <Box flexDirection="column" borderStyle="round" borderColor="cyan" paddingX={2} paddingY={1}>
      <Text bold color="cyan"> Select model</Text>
      <Text color="gray"> ↑↓ navigate · enter select · esc cancel</Text>
      <Text> </Text>
      {ALL_MODELS.map((m, i) => (
        <Box key={m}>
          <Text color={i === idx ? "cyan" : "gray"}>{i === idx ? "▶ " : "  "}</Text>
          <Text color={i === idx ? "white" : "gray"} bold={i === idx}>
            {MODEL_LABELS[m]}
          </Text>
          {m === current && <Text color="gray">  (current)</Text>}
        </Box>
      ))}
    </Box>
  );
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

  useInput((input, key) => {
    if (input === "y" || key.return) onApprove();
    if (input === "n" || key.escape) onDeny();
  });

  return (
    <Box flexDirection="column" borderStyle="round" borderColor="yellow" paddingX={2} paddingY={1}>
      <Text bold color="yellow"> ⚠  Shell command requested</Text>
      <Text> </Text>
      <Text color="gray">  command:</Text>
      <Box marginLeft={4}>
        <Text color="white" bold>{args.command ?? toolCall.arguments}</Text>
      </Box>
      <Text> </Text>
      <Text color="gray">  [y] approve    [n] deny</Text>
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

// ─── Main App ─────────────────────────────────────────────────────────────────

function App() {
  const { exit } = useApp();
  const [model, setModel] = useState<Model>("gpt-5.5");
  const [messages, setMessages] = useState<Message[]>([
    { id: "sys", role: "system", content: "You are a helpful coding assistant. You can run shell commands when needed." },
  ]);
  const [input, setInput] = useState("");
  const [mode, setMode] = useState<AppMode>("chat");
  const [isLoading, setIsLoading] = useState(false);
  const [pendingTool, setPendingTool] = useState<ToolCall | null>(null);
  const [tokensIn, setTokensIn] = useState(0);
  const [tokensOut, setTokensOut] = useState(0);
  const [error, setError] = useState<string | null>(null);

  const streamingIdRef = useRef<string | null>(null);

  const visibleMessages = messages.filter(m => m.role !== "system");

  function addMessage(msg: Omit<Message, "id">) {
    const id = randomId();
    setMessages(prev => [...prev, { ...msg, id }]);
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
      if (cmd === "/exit" || cmd === "/quit") { exit(); return; }
      if (cmd === "/clear") { setMessages(prev => prev.filter(m => m.role === "system")); return; }
      setError(`Unknown command: ${userText}`);
      return;
    }

    addMessage({ role: "user", content: userText });
    setIsLoading(true);

    const history = messages
      .filter(m => m.role !== "tool" || true)
      .map(m => ({ role: m.role === "tool" ? "tool" : m.role, content: m.content }));
    history.push({ role: "user", content: userText });

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
        setMode("confirm-tool");
      },
      onDone(usage) {
        if (usage) { setTokensIn(n => n + usage.input); setTokensOut(n => n + usage.output); }
        setMessages(prev => prev.map(m =>
          m.id === assistantId ? { ...m, isStreaming: false } : m
        ));
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

    if (isGPT(model)) {
      await streamGPT(model, history as any, cb);
    } else {
      await streamDeepSeek(model, history as any, cb);
    }
  }, [messages, model, isLoading, exit]);

  const handleToolApprove = useCallback(async () => {
    if (!pendingTool) return;
    setMode("chat");
    let args: any = {};
    try { args = JSON.parse(pendingTool.arguments); } catch {}
    const command = args.command ?? pendingTool.arguments;

    const toolMsgId = addMessage({ role: "tool", content: `$ ${command}\n…running…`, toolName: pendingTool.name });

    let output = "";
    try {
      const result = await execAsync(command, { timeout: 30000 });
      output = (result.stdout + result.stderr).trim() || "(no output)";
    } catch (e: any) {
      output = e.stderr || e.message || "command failed";
    }

    updateMessage(toolMsgId, { content: `$ ${command}\n${output}` });

    // Continue conversation with tool result
    const history = messages.map(m => ({ role: m.role, content: m.content }));
    history.push({ role: "tool" as any, content: output });

    const continueId = randomId();
    setMessages(prev => [...prev, { id: continueId, role: "assistant", content: "", model, isStreaming: true }]);
    setIsLoading(true);

    const cb: StreamCallbacks = {
      onDelta(text) {
        setMessages(prev => prev.map(m => m.id === continueId ? { ...m, content: m.content + text } : m));
      },
      onToolCall(tc) { setPendingTool(tc); setMode("confirm-tool"); },
      onDone(usage) {
        if (usage) { setTokensIn(n => n + usage.input); setTokensOut(n => n + usage.output); }
        setMessages(prev => prev.map(m => m.id === continueId ? { ...m, isStreaming: false } : m));
        setIsLoading(false);
        setPendingTool(null);
      },
      onError(err) {
        setError(err);
        setMessages(prev => prev.map(m => m.id === continueId ? { ...m, content: `Error: ${err}`, isStreaming: false } : m));
        setIsLoading(false);
      },
    };

    if (isGPT(model)) {
      await streamGPT(model, history as any, cb);
    } else {
      await streamDeepSeek(model, history as any, cb);
    }
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
    addMessage({ role: "tool", content: "(tool use denied by user)" });
  }, [pendingTool]);

  const handleSubmit = useCallback((val: string) => {
    setInput("");
    sendMessage(val);
  }, [sendMessage]);

  return (
    <Box flexDirection="column" width={WIDTH}>
      <Header model={model} />

      {/* Messages */}
      <Box flexDirection="column" flexGrow={1}>
        {visibleMessages.map(msg => (
          <MessageBubble key={msg.id} msg={msg} />
        ))}
      </Box>

      {/* Error */}
      {error && (
        <Box marginLeft={2}>
          <Text color="red">⚠  {error}</Text>
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

      {/* Input */}
      {mode === "chat" && (
        <Box flexDirection="column">
          <Text color="gray">{BORDER.repeat(WIDTH)}</Text>
          <Box>
            <Text color={isLoading ? "gray" : "yellow"}>{isLoading ? "  … " : "  › "}</Text>
            {isLoading ? (
              <Text color="gray">thinking…</Text>
            ) : (
              <TextInput
                value={input}
                onChange={setInput}
                onSubmit={handleSubmit}
                placeholder="message…  or /model  /clear  /exit"
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