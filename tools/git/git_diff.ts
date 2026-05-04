import { execSync } from "child_process";

export const name = "git_diff";
export const description = "Show working-tree changes (unstaged by default, staged if staged=true)";

export const inputSchema = {
  type: "object",
  properties: {
    staged: { type: "boolean", description: "Show staged changes instead of unstaged" },
  },
  required: [],
};

export async function run(input: { staged?: boolean }): Promise<string> {
  try {
    const args = input.staged ? ["diff", "--staged"] : ["diff"];
    const result = execSync(`git ${args.join(" ")}`, { timeout: 10000, maxBuffer: 1024 * 512, encoding: "utf8" });
    return result.trim() || "(no changes)";
  } catch (e: any) {
    return e.stderr?.trim() || e.message || "git diff failed";
  }
}
