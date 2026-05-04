import { execSync } from "child_process";
import { existsSync } from "fs";

export const name = "run_tests";
export const description = "Run the project's test suite. Auto-detects bun, npm, or yarn. Use a filter to run specific tests.";

export const inputSchema = {
  type: "object",
  properties: {
    filter: { type: "string", description: "Test name pattern to filter (passed to test runner)" },
  },
  required: [],
};

export async function run(input: { filter?: string }): Promise<string> {
  const cwd = process.cwd();
  try {
    let cmd: string;
    if (existsSync(`${cwd}/bun.lock`) || existsSync(`${cwd}/bun.lockb`)) {
      cmd = input.filter ? `bun test --filter "${input.filter}"` : "bun test";
    } else if (existsSync(`${cwd}/package-lock.json`)) {
      cmd = input.filter ? `npm test -- --filter "${input.filter}"` : "npm test";
    } else {
      cmd = input.filter ? `bun test --filter "${input.filter}"` : "bun test";
    }

    const result = execSync(cmd, {
      timeout: 120000, maxBuffer: 1024 * 1024 * 2, encoding: "utf8", cwd,
    });
    return result.trim() || "Tests passed (no output)";
  } catch (e: any) {
    const out = (e.stdout ?? "") + (e.stderr ?? "");
    return out.trim() || e.message || "Tests failed";
  }
}
