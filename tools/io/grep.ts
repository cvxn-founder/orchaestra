import { readFileSync, existsSync } from "fs";
import { execSync } from "child_process";
import { resolvePath } from "./_resolve";

export const name = "grep";
export const description = "Search for a regex pattern across files in a directory. Uses ripgrep if available, falls back to node.";

export const inputSchema = {
  type: "object",
  properties: {
    pattern: { type: "string", description: "Regex pattern to search for" },
    path: { type: "string", description: "Directory or file to search in (default: cwd)" },
    include: { type: "string", description: "File glob pattern to include (e.g., *.tsx)" },
  },
  required: ["pattern"],
};

export async function run(input: { pattern: string; path?: string; include?: string }): Promise<string> {
  const { pattern, include } = input;
  const target = input.path ? resolvePath(input.path) : process.cwd();
  if (!existsSync(target)) return `Path not found: ${input.path ?? target}`;

  // Try rg first
  try {
    const args = ["--line-number", "--no-heading", "--color=never", "-e", pattern];
    if (include) args.push("--glob", include);
    args.push(target);
    const result = execSync(`rg ${args.map(a => `"${a.replace(/"/g, '\\"')}"`).join(" ")}`, {
      timeout: 15000, maxBuffer: 1024 * 1024, encoding: "utf8", cwd: target,
    });
    return result.trim() || "No matches";
  } catch (e: any) {
    if (e.stdout) return e.stdout.trim() || "No matches";
    // rg not available, use basic grep
  }

  // Fallback: basic node grep (single file or small dir)
  try {
    const st = existsSync(target);
    if (!st) return "No matches";
    const { statSync, readdirSync } = await import("fs");
    const stat = statSync(target);
    const files: string[] = [];
    if (stat.isFile()) {
      files.push(target);
    } else {
      const { readdirSync: rd } = await import("fs");
      walk(target, include ? new RegExp(include.replace(/\*/g, ".*")) : null, files);
    }

    const regex = new RegExp(pattern, "g");
    const results: string[] = [];
    for (const f of files.slice(0, 50)) {
      try {
        const content = readFileSync(f, "utf8");
        const lines = content.split("\n");
        for (let i = 0; i < lines.length; i++) {
          if (regex.test(lines[i])) {
            results.push(`${f}:${i + 1}: ${lines[i].trim().slice(0, 200)}`);
            regex.lastIndex = 0;
          }
        }
      } catch {}
    }
    return results.slice(0, 100).join("\n") || "No matches";
  } catch (e: any) {
    return `Grep error: ${e.message}`;
  }
}

function walk(dir: string, include: RegExp | null, out: string[]) {
  const { readdirSync, statSync } = require("fs") as typeof import("fs");
  const { join } = require("path") as typeof import("path");
  try {
    for (const entry of readdirSync(dir)) {
      const full = join(dir, entry);
      try {
        const st = statSync(full);
        if (st.isDirectory() && !entry.startsWith(".")) walk(full, include, out);
        else if (st.isFile() && (!include || include.test(entry))) out.push(full);
      } catch {}
    }
  } catch {}
}
