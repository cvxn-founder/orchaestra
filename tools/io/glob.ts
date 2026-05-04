import { readdirSync, statSync } from "fs";
import { join, relative } from "path";
import { resolvePath } from "./_resolve";

export const name = "glob";
export const description = "Find files matching a glob pattern (e.g., src/**/*.tsx)";

export const inputSchema = {
  type: "object",
  properties: {
    pattern: { type: "string", description: "Glob pattern (supports **, *, ?)" },
    path: { type: "string", description: "Root directory (default: cwd)" },
  },
  required: ["pattern"],
};

export async function run(input: { pattern: string; path?: string }): Promise<string> {
  const { pattern } = input;
  const base = input.path ? resolvePath(input.path) : process.cwd();

  const regex = globToRegex(pattern);
  const results: string[] = [];
  walkGlob(base, base, regex, results, 200);

  return results.map(r => relative(base, r)).join("\n") || "No matches";
}

function globToRegex(pattern: string): RegExp {
  let p = "";
  let i = 0;
  const specials = ".+^${}()|[]\\";
  while (i < pattern.length) {
    if (pattern.slice(i).startsWith("**")) { p += ".*"; i += 2; }
    else if (pattern[i] === "*") { p += "[^/]*"; i++; }
    else if (pattern[i] === "?") { p += "[^/]"; i++; }
    else if (specials.includes(pattern[i])) { p += "\\" + pattern[i]; i++; }
    else { p += pattern[i]; i++; }
  }
  return new RegExp(`^${p}$`);
}

function walkGlob(base: string, dir: string, regex: RegExp, out: string[], max: number) {
  if (out.length >= max) return;
  try {
    for (const entry of readdirSync(dir)) {
      if (entry.startsWith(".")) continue;
      const full = join(dir, entry);
      try {
        const st = statSync(full);
        const rel = relative(base, full);
        if (st.isDirectory()) {
          walkGlob(base, full, regex, out, max);
        } else if (regex.test(rel) || regex.test(entry)) {
          out.push(full);
        }
      } catch {}
    }
  } catch {}
}
