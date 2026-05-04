import { readFileSync, writeFileSync, existsSync } from "fs";
import { resolvePath } from "./_resolve";

export const name = "edit_file";
export const description = "Find and replace a string in a file. Replace only the first occurrence unless replace_all is true.";

export const inputSchema = {
  type: "object",
  properties: {
    path: { type: "string", description: "Path to the file (absolute or relative to cwd)" },
    find: { type: "string", description: "Exact string to find" },
    replace: { type: "string", description: "String to replace with" },
    replace_all: { type: "boolean", description: "Replace all occurrences (default: false)" },
  },
  required: ["path", "find", "replace"],
};

export async function run(input: { path: string; find: string; replace: string; replace_all?: boolean }): Promise<string> {
  const { find, replace, replace_all } = input;
  const path = resolvePath(input.path);
  if (!existsSync(path)) return `File not found: ${input.path}`;
  try {
    const original = readFileSync(path, "utf8");
    if (!original.includes(find)) return `String not found in ${path}: "${find.slice(0, 80)}"`;
    const updated = replace_all
      ? original.split(find).join(replace)
      : original.replace(find, replace);
    writeFileSync(path, updated, "utf8");
    const count = replace_all ? original.split(find).length - 1 : 1;
    return `Replaced ${count} occurrence(s) in ${path}`;
  } catch (e: any) {
    return `Error editing ${path}: ${e.message}`;
  }
}
