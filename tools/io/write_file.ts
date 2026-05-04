import { writeFileSync, existsSync, mkdirSync } from "fs";
import { dirname } from "path";
import { resolvePath } from "./_resolve";

export const name = "write_file";
export const description = "Create or overwrite a file with the given content";

export const inputSchema = {
  type: "object",
  properties: {
    path: { type: "string", description: "Path to write the file (absolute or relative to cwd)" },
    content: { type: "string", description: "Content to write" },
  },
  required: ["path", "content"],
};

export async function run(input: { path: string; content: string }): Promise<string> {
  const { content } = input;
  const path = resolvePath(input.path);
  try {
    const dir = dirname(path);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    writeFileSync(path, content, "utf8");
    return `Wrote ${content.split("\n").length} lines to ${path}`;
  } catch (e: any) {
    return `Error writing ${path}: ${e.message}`;
  }
}
