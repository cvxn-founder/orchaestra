import { readFileSync, existsSync } from "fs";
import { resolvePath } from "../io/_resolve";

export const name = "read_json";
export const description = "Read a JSON file and optionally query into it with a dot-separated path (e.g., 'dependencies.react')";

export const inputSchema = {
  type: "object",
  properties: {
    path: { type: "string", description: "Path to the JSON file (absolute or relative to cwd)" },
    query: { type: "string", description: "Optional dot-separated path to extract a specific value" },
  },
  required: ["path"],
};

export async function run(input: { path: string; query?: string }): Promise<string> {
  const { query } = input;
  const path = resolvePath(input.path);
  if (!existsSync(path)) return `File not found: ${input.path}`;
  try {
    const raw = readFileSync(path, "utf8");
    const data = JSON.parse(raw);
    let target: any = data;
    if (query) {
      for (const key of query.split(".")) {
        if (target == null) break;
        target = target[key];
      }
    }
    return JSON.stringify(target, null, 2);
  } catch (e: any) {
    return `Error reading JSON: ${e.message}`;
  }
}
