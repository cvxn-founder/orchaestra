import { exec } from "child_process";
import { promisify } from "util";

const execAsync = promisify(exec);

export const name = "shell";
export const description = "Run a shell command in the current directory. Use for build commands, git operations, package installs, file operations, and any CLI tool.";

export const inputSchema = {
  type: "object",
  properties: {
    command: { type: "string", description: "The shell command to run" },
  },
  required: ["command"],
};

export async function run(input: { command: string }): Promise<string> {
  const { command } = input;
  try {
    const result = await execAsync(command, { timeout: 30000, maxBuffer: 1024 * 1024 });
    return (result.stdout + result.stderr).trim() || "(no output)";
  } catch (e: any) {
    const out = (e.stdout ?? "") + (e.stderr ?? "");
    return out.trim() || e.message || "command failed";
  }
}
