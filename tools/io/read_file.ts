import { readFileSync, existsSync } from "fs";
import { resolvePath } from "./_resolve";

export const name = "read_file";
export const description = "Read a file at the given path with optional offset and limit";

export const inputSchema = {
  type: "object",
  properties: {
    path: { type: "string", description: "Path to the file (absolute or relative to cwd)" },
    offset: { type: "integer", description: "Line number to start reading from (1-indexed)" },
    limit: { type: "integer", description: "Maximum number of lines to read" },
  },
  required: ["path"],
};

export async function run(input: { path: string; offset?: number; limit?: number }): Promise<string> {
  const { offset, limit } = input;
  const path = resolvePath(input.path);
  if (!existsSync(path)) return `File not found: ${input.path}`;
  try {
    const content = readFileSync(path, "utf8");
    const lines = content.split("\n");
    const start = (offset ?? 1) - 1;
    const end = limit ? start + limit : lines.length;
    const sliced = lines.slice(start, end);
    const numbered = sliced.map((l, i) => `${start + i + 1}\t${l}`).join("\n");
    return numbered || "(empty)";
  } catch (e: any) {
    return `Error reading ${path}: ${e.message}`;
  }
}
