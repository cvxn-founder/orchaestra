#!/usr/bin/env bun
// ─── Tool Sync Checker ─────────────────────────────────────────────────────────
// Usage: bun tools/sync.ts
//
// Compares orchaestra tool definitions against webuildfast tool-functions source.
// Reports tools present in one but not the other, schema drift, and content diffs
// for ported tools that have diverged from their webuildfast originals.

import { TOOL_MODULES, TOOL_MAP } from "./index";
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { spawnSync } from "child_process";

const WBF_ROOT = "/Volumes/Extreme SSD/projects/webuildfast/apps/tool-functions";
const ORCH_ROOT = "/Volumes/Extreme SSD/projects/orchaestra";

// ═══════════════════════════════════════════════════════════════════════════════
// Normalization
// ═══════════════════════════════════════════════════════════════════════════════

function norm(name: string): string {
  return name.replace(/[_-]/g, "").toLowerCase();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Parse webuildfast index.ts → tool name → absolute file path
// ═══════════════════════════════════════════════════════════════════════════════

function parseWbfToolPaths(): Map<string, string> {
  const indexPath = join(WBF_ROOT, "src/index.ts");
  if (!existsSync(indexPath)) return new Map();

  const src = readFileSync(indexPath, "utf8");

  // Step 1: import { runXxx } from "./relative/path" → { runFuncName: relativePath }
  const importMap = new Map<string, string>();
  const importRe = /import\s+\{\s*(\w+)\s*\}\s+from\s+"([^"]+)"/g;
  let match;
  while ((match = importRe.exec(src)) !== null) {
    importMap.set(match[1], match[2]); // runStackDetect → ./tools/compute/betterux/stack-detect
  }

  // Step 2: TOOLS array entries: name: "xxx", run: runXxx → { toolName: runFuncName }
  const toolRunMap = new Map<string, string>();
  const entryRe = /\{\s*name:\s*"([^"]+)"[\s\S]*?run:\s*(\w+)\s*,?\s*\}/g;
  while ((match = entryRe.exec(src)) !== null) {
    toolRunMap.set(match[1], match[2]); // element-stack-detect → runStackDetect
  }

  // Step 3: chain → toolName → absolute path
  const result = new Map<string, string>();
  for (const [toolName, runFunc] of toolRunMap) {
    const relPath = importMap.get(runFunc);
    if (relPath) {
      result.set(toolName, join(WBF_ROOT, "src", relPath + ".ts"));
    }
  }

  return result;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Build orchaestra tool → file path map from category + name convention
// ═══════════════════════════════════════════════════════════════════════════════

function buildOrchToolPaths(): Map<string, string> {
  const result = new Map<string, string>();
  for (const t of TOOL_MODULES) {
    result.set(t.name, join(ORCH_ROOT, "tools", t.category, t.name + ".ts"));
  }
  return result;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Read webuildfast tool names + metadata (existing logic, kept for name compare)
// ═══════════════════════════════════════════════════════════════════════════════

function readWbfTools(): { name: string; description: string; category: string }[] {
  const indexPath = join(WBF_ROOT, "src/index.ts");
  if (!existsSync(indexPath)) {
    console.log(`  WARN  webuildfast not found at ${WBF_ROOT}`);
    return [];
  }

  const src = readFileSync(indexPath, "utf8");
  const tools: { name: string; description: string; category: string }[] = [];

  const entryRe = /\{\s*name:\s*"([^"]+)"\s*,\s*description:\s*"([^"]*)"[\s\S]*?\}/g;
  let match;
  while ((match = entryRe.exec(src)) !== null) {
    tools.push({ name: match[1], description: match[2], category: "" });
  }

  // Derive category from known mapping
  const catMap: Record<string, string> = {
    "element-inspect": "compute", "element-semantic": "compute", "element-uilang": "compute",
    "element-componentize": "compute", "element-edit": "compute", "element-stack-detect": "compute",
    "ngmi-ascii": "compute", "polymarket-scorer": "compute", "factsearch-parse": "compute",
    "polymarket-ctf": "external", "autoschema-discover": "compute-io-db",
    "db-search": "db-ch", "db-preview": "db-ch", "db-schema": "db-ch",
    "db-relations": "db-ch", "db-aggregate": "db-ch", "db-query": "db-ch",
  };
  for (const t of tools) {
    t.category = catMap[t.name] ?? "unknown";
  }

  return tools;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════════════════

console.log("Tool Sync Checker\n");
console.log(`  orchaestra: ${TOOL_MODULES.length} tools`);
console.log(`  webuildfast: checking...`);

const wbfTools = readWbfTools();

if (wbfTools.length === 0) {
  console.log(`  webuildfast: NOT FOUND (${WBF_ROOT})`);
  console.log(`\n  Run with WBF_ROOT env to override path.`);
  process.exit(0);
}

console.log(`  webuildfast: ${wbfTools.length} tools\n`);

// ─── Name Comparison ───────────────────────────────────────────────────────

const orchNames = new Set(TOOL_MODULES.map(t => t.name));
const wbfNames = new Set(wbfTools.map(t => t.name));
const wbfNormToName = new Map(wbfTools.map(t => [norm(t.name), t.name]));
const orchNormToName = new Map(TOOL_MODULES.map(t => [norm(t.name), t.name]));

const onlyHere = [...orchNames].filter(n => !wbfNormToName.has(norm(n)));
const onlyWbf = [...wbfNames].filter(n => !orchNormToName.has(norm(n)));
const common = [...orchNames].filter(n => wbfNormToName.has(norm(n)));

// Group by category
const byCat = (names: string[], src: Map<string, string>) => {
  const cats: Record<string, string[]> = {};
  for (const n of names) {
    const cat = src.get(n) ?? "?";
    (cats[cat] ??= []).push(n);
  }
  return cats;
};

const wbfCatMap = new Map(wbfTools.map(t => [t.name, t.category]));
const orchCatMap = new Map(TOOL_MODULES.map(t => [t.name, t.category]));

if (onlyHere.length > 0) {
  console.log("── Only in orchaestra ──");
  for (const [cat, names] of Object.entries(byCat(onlyHere, orchCatMap))) {
    console.log(`  ${cat}/`);
    for (const n of names) console.log(`    ${n}`);
  }
  console.log();
}

if (onlyWbf.length > 0) {
  console.log("── Only in webuildfast (not ported) ──");
  for (const [cat, names] of Object.entries(byCat(onlyWbf, wbfCatMap))) {
    console.log(`  ${cat}/`);
    for (const n of names) console.log(`    ${n}`);
  }
  console.log();
}

// Summary
const totalUnique = new Set([...orchNames, ...wbfNames]).size;
console.log(`Summary: ${orchNames.size} orchaestra + ${wbfNames.size} webuildfast = ${totalUnique} unique tools`);
console.log(`  Shared: ${common.length}  |  Only orchaestra: ${onlyHere.length}  |  Only webuildfast: ${onlyWbf.length}`);

// ═══════════════════════════════════════════════════════════════════════════════
// Content Diff — compare ported tools against webuildfast originals
// ═══════════════════════════════════════════════════════════════════════════════

if (common.length === 0) {
  console.log("\n  No shared tools to diff.");
  process.exit(0);
}

console.log(`\n── Content Diff (${common.length} shared tools) ──`);

const wbfPaths = parseWbfToolPaths();
const orchPaths = buildOrchToolPaths();

let unchanged = 0;
let changed = 0;
let missing = 0;

for (const orchName of common.sort()) {
  // Find the matching webuildfast tool name via normalization
  const wbfName = wbfNormToName.get(norm(orchName));
  if (!wbfName) continue;

  const wbfPath = wbfPaths.get(wbfName);
  const orchPath = orchPaths.get(orchName);

  if (!wbfPath || !existsSync(wbfPath)) {
    console.log(`  ${orchName}: webuildfast source not found at ${wbfPath ?? "?"}`);
    missing++;
    continue;
  }
  if (!orchPath || !existsSync(orchPath)) {
    console.log(`  ${orchName}: orchaestra source not found at ${orchPath ?? "?"}`);
    missing++;
    continue;
  }

  const wbfContent = readFileSync(wbfPath, "utf8");
  const orchContent = readFileSync(orchPath, "utf8");

  if (wbfContent === orchContent) {
    unchanged++;
  } else {
    changed++;
    const wbfLines = wbfContent.split("\n").length;
    const orchLines = orchContent.split("\n").length;
    console.log(`\n  ┌─ ${orchName}  (wbf: ${wbfLines} lines, orch: ${orchLines} lines)`);
    console.log(`  │  wbf: ${wbfPath}`);
    console.log(`  │  orch: ${orchPath}`);

    // Run unified diff
    const diff = spawnSync("diff", ["-u", wbfPath, orchPath], {
      encoding: "utf8",
      maxBuffer: 1024 * 1024,
      timeout: 5000,
    });

    if (diff.status === 1 && diff.stdout) {
      // Show first 40 lines of diff
      const diffLines = diff.stdout.split("\n");
      const preview = diffLines.slice(0, 40);
      for (const line of preview) {
        console.log(`  │ ${line}`);
      }
      if (diffLines.length > 40) {
        console.log(`  │ ... (${diffLines.length - 40} more lines)`);
      }
    }
    console.log(`  └─`);
  }
}

console.log(`\n  Diff summary: ${unchanged} unchanged, ${changed} changed, ${missing} missing`);
