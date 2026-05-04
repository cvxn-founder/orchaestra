import { existsSync } from "fs";
import { join, isAbsolute, resolve } from "path";

export function resolvePath(raw: string): string {
  // Try exact path first
  if (existsSync(raw)) return raw;

  // Relative: resolve against cwd
  if (!isAbsolute(raw)) {
    const resolved = resolve(process.cwd(), raw);
    if (existsSync(resolved)) return resolved;
    return raw; // return original so error message shows what was asked
  }

  // Absolute but not found: try stripping leading / and joining with cwd
  // Handles cases like /index.tsx when cwd is /Volumes/.../orchaestra
  const stripped = raw.replace(/^\/+/, "");
  const joined = join(process.cwd(), stripped);
  if (existsSync(joined)) return joined;

  // Try resolving as if the leading segment is a project name
  // e.g. /projects/orchaestra/index.tsx → cwd might be /Volumes/.../orchaestra
  const parts = stripped.split("/");
  for (let i = 0; i < parts.length; i++) {
    const suffix = parts.slice(i).join("/");
    const candidate = join(process.cwd(), suffix);
    if (existsSync(candidate)) return candidate;
  }

  return raw; // not found — caller will report error
}
