import { execSync } from "child_process";
import { existsSync } from "fs";

export const name = "typecheck";
export const description = "Run the project's type checker. Auto-detects tsc or bun. Returns compiler errors if any.";

export const inputSchema = {
  type: "object",
  properties: {},
  required: [],
};

export async function run(_input: {}): Promise<string> {
  const cwd = process.cwd();
  try {
    let cmd: string;
    if (existsSync(`${cwd}/tsconfig.json`)) {
      cmd = existsSync(`${cwd}/bun.lock`) || existsSync(`${cwd}/bun.lockb`)
        ? "bun run --bun tsc --noEmit" : "npx tsc --noEmit";
    } else {
      return "No tsconfig.json found";
    }

    const result = execSync(cmd, {
      timeout: 60000, maxBuffer: 1024 * 512, encoding: "utf8", cwd,
    });
    return result.trim() || "No type errors";
  } catch (e: any) {
    const out = (e.stdout ?? "") + (e.stderr ?? "");
    return out.trim() || "Type check failed";
  }
}
