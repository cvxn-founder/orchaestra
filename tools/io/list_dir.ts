import { readdirSync, existsSync, statSync } from "fs";
import { join } from "path";
import { resolvePath } from "./_resolve";

export const name = "list_dir";
export const description = "List files and directories at the given path";

export const inputSchema = {
  type: "object",
  properties: {
    path: { type: "string", description: "Path to directory (absolute, relative, or omit for cwd)" },
  },
  required: [],
};

export async function run(input: { path?: string }): Promise<string> {
  const dir = input.path ? resolvePath(input.path) : process.cwd();
  if (!existsSync(dir)) return `Directory not found: ${input.path ?? dir}`;
  try {
    const entries = readdirSync(dir);
    const lines: string[] = [];
    for (const name of entries.sort()) {
      const full = join(dir, name);
      try {
        const st = statSync(full);
        const type = st.isDirectory() ? "/" : "@".includes(name[0] ?? "") ? " " : " ";
        const size = st.isFile() ? ` ${String(st.size).padStart(8)}` : "         ";
        lines.push(`${type}${size}  ${name}`);
      } catch {
        lines.push(`?           ${name}`);
      }
    }
    return lines.join("\n") || "(empty directory)";
  } catch (e: any) {
    return `Error listing ${dir}: ${e.message}`;
  }
}
