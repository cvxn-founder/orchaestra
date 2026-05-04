import { writeFileSync, readFileSync, existsSync, mkdirSync } from "fs";
import { join } from "path";

const MEM_DIR = join(process.cwd(), "user", "memory");

export const name = "remember";
export const description = "Persist a fact or note to memory. Stored in user/memory/ for future reference.";

export const inputSchema = {
  type: "object",
  properties: {
    key: { type: "string", description: "Key/name for this memory" },
    value: { type: "string", description: "The content to remember" },
  },
  required: ["key", "value"],
};

export async function run(input: { key: string; value: string }): Promise<string> {
  const { key, value } = input;
  try {
    if (!existsSync(MEM_DIR)) mkdirSync(MEM_DIR, { recursive: true });
    const safeKey = key.replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 64);
    const filePath = join(MEM_DIR, `${safeKey}.md`);
    writeFileSync(filePath, value, "utf8");
    return `Remembered "${key}" → user/memory/${safeKey}.md`;
  } catch (e: any) {
    return `Error: ${e.message}`;
  }
}
