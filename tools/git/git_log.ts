import { execSync } from "child_process";

export const name = "git_log";
export const description = "Show recent commit history";

export const inputSchema = {
  type: "object",
  properties: {
    count: { type: "integer", description: "Number of commits to show (default: 10)" },
  },
  required: [],
};

export async function run(input: { count?: number }): Promise<string> {
  try {
    const n = input.count ?? 10;
    const result = execSync(
      `git log --oneline -${n} --decorate`,
      { timeout: 5000, maxBuffer: 1024 * 64, encoding: "utf8" },
    );
    return result.trim() || "(no commits)";
  } catch (e: any) {
    return e.stderr?.trim() || e.message || "git log failed";
  }
}
