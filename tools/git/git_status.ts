import { execSync } from "child_process";

export const name = "git_status";
export const description = "Show the working tree status (changed, staged, untracked files)";

export const inputSchema = {
  type: "object",
  properties: {},
  required: [],
};

export async function run(_input: {}): Promise<string> {
  try {
    const result = execSync("git status --short", {
      timeout: 5000, maxBuffer: 1024 * 64, encoding: "utf8",
    });
    return result.trim() || "(clean working tree)";
  } catch (e: any) {
    return e.stderr?.trim() || e.message || "git status failed";
  }
}
