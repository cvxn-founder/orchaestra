#!/usr/bin/env bun
// ─── Tool Test Runner ──────────────────────────────────────────────────────────
// Usage: bun tools/test.ts [--verbose] [tool-name]
//
// Tests every tool with sample input to verify they execute without throwing.
// Each tool module can export an optional `testInput` for its happy-path test.

import { TOOL_MODULES, executeTool } from "./index";

const args = (Bun as any).argv as string[];
const verbose = args.includes("--verbose") || args.includes("-v");
const filter = args.find((a: string) => !a.startsWith("-") && !a.includes("test.ts") && !a.includes("bun"));

const testInputs: Record<string, any> = {
  // io
  read_file: { path: "package.json" },
  write_file: { path: "/tmp/orchaestra_test_write.txt", content: "hello from orchaestra test" },
  edit_file: { path: "/tmp/orchaestra_test_edit.txt", find: "hello", replace: "HELLO" },
  list_dir: { path: "." },
  grep: { pattern: "import", path: ".", include: "*.ts" },
  glob: { pattern: "tools/**/*.ts", path: "." },
  // shell
  shell: { command: "echo 'hello from orchaestra test'" },
  // git
  git_diff: {},
  git_log: { count: 3 },
  git_status: {},
  // web
  web_fetch: { url: "https://httpbin.org/get" },
  web_search: { query: "test" },
  // project
  read_json: { path: "package.json", query: "name" },
  run_tests: {},
  typecheck: {},
  package_info: {},
  // meta
  ask_user: { question: "Is this a test?" },
  write_plan: { tasks: JSON.stringify([{ id: "1", subject: "Test task", status: "pending" }]) },
  remember: { key: "test_key", value: "test value from orchaestra test runner" },
  // compute
  stack_detect: { packageJson: JSON.stringify({ dependencies: { react: "^18", next: "^14" }, devDependencies: { tailwindcss: "^3" } }), files: ["src/App.tsx", "tailwind.config.ts"] },
  element_semantic: { tag: "button", attrs: { "aria-label": "Submit form", class: "px-4 py-2 bg-blue-500 rounded" } },
  ngmi_ascii: { text: "TEST" },
  autoschema_discover: { files: [{ path: "test.csv", content: "name,age,city\nAlice,30,NYC\nBob,25,LA" }] },
  factsearch_parse: { llmResponse: "- The sky is blue\n- Water is wet", sourceUrl: "https://example.com" },
};

let passed = 0;
let failed = 0;
let skipped = 0;

console.log("Tool Test Runner\n");

for (const tool of TOOL_MODULES) {
  if (filter && tool.name !== filter && !tool.name.includes(filter)) continue;

  const input = testInputs[tool.name];
  if (!input) {
    console.log(`  SKIP  ${tool.name.padEnd(25)} (no test input)`);
    skipped++;
    continue;
  }

  const args = JSON.stringify(input);
  const label = `${tool.name}(${args.slice(0, 60)}${args.length > 60 ? "..." : ""})`;

  try {
    const result = await executeTool(tool.name, args);
    if (typeof result !== "string") {
      console.log(`  FAIL  ${tool.name.padEnd(25)} returned ${typeof result}`);
      failed++;
      continue;
    }
    if (result.startsWith("Error") || result.startsWith("Unknown tool") || result.startsWith("Tool error")) {
      console.log(`  FAIL  ${tool.name.padEnd(25)} ${result.slice(0, 80)}`);
      failed++;
      continue;
    }
    console.log(`  PASS  ${tool.name.padEnd(25)} ${result.slice(0, 60).replace(/\n/g, " ")}${result.length > 60 ? "..." : ""}`);
    passed++;
  } catch (e: any) {
    console.log(`  FAIL  ${tool.name.padEnd(25)} threw: ${e.message?.slice(0, 60)}`);
    failed++;
  }
}

// Cleanup test files
try {
  const { unlinkSync } = await import("fs");
  unlinkSync("/tmp/orchaestra_test_write.txt");
} catch {}
try {
  const { unlinkSync } = await import("fs");
  unlinkSync("/tmp/orchaestra_test_edit.txt");
} catch {}

console.log(`\n${passed} passed, ${failed} failed, ${skipped} skipped (${TOOL_MODULES.length} total)`);

process.exit(failed > 0 ? 1 : 0);
