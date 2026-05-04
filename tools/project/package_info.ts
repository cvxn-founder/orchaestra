import { readFileSync, existsSync } from "fs";
import { join } from "path";

export const name = "package_info";
export const description = "Read the project's package.json — shows dependencies, scripts, and metadata";

export const inputSchema = {
  type: "object",
  properties: {},
  required: [],
};

export async function run(_input: {}): Promise<string> {
  const cwd = process.cwd();
  const pkgPath = join(cwd, "package.json");
  if (!existsSync(pkgPath)) return "No package.json found in current directory";
  try {
    const data = JSON.parse(readFileSync(pkgPath, "utf8"));
    return JSON.stringify({
      name: data.name,
      version: data.version,
      scripts: data.scripts,
      dependencies: data.dependencies,
      devDependencies: data.devDependencies,
    }, null, 2);
  } catch (e: any) {
    return `Error: ${e.message}`;
  }
}
