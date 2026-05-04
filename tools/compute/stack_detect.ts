export const name = "stack_detect";
export const description = "Detect tech stack (framework, styling, rendering strategy, build tool, runtime) from package.json contents and file list. Detects Next.js, React, Vue, Nuxt, Svelte, SvelteKit, Astro, Remix, SolidJS, Angular, Tailwind, CSS Modules, Sass, and more.";

export const inputSchema = {
  type: "object",
  properties: {
    packageJson: { type: "string", description: "Contents of package.json" },
    files: { type: "array", items: { type: "string" }, description: "List of file paths in the project" },
    fileContents: { type: "object", description: "Map of file path to content for key config files" },
  },
  required: [],
};

type Framework = "react" | "nextjs" | "vue" | "nuxt" | "svelte" | "sveltekit" | "astro" | "remix" | "solidjs" | "angular" | { unknown: string };
type StylingApproach = "tailwind" | "css_modules" | "styled_components" | "vanilla_css" | "sass" | "unocss" | "pandacss" | { unknown: string };
type RenderingStrategy = "client_side" | "server_side" | "static" | "incremental" | "unknown";

export async function run(input: any): Promise<string> {
  const pkgValue = input.packageJson ? (() => { try { return JSON.parse(input.packageJson); } catch { return undefined; } })() : undefined;
  const files: string[] = input.files ?? [];
  const fileContents: Record<string, string> = input.fileContents ?? {};

  const deps = allDeps(pkgValue);
  const enginesNode = pkgValue?.engines?.node != null ? String(pkgValue.engines.node) : undefined;

  const signals: string[] = [];
  const framework = detectFramework(deps, files, signals);
  const styling = detectStyling(deps, files, signals);
  const rendering = detectRendering(framework, fileContents, signals);
  const buildTool = detectBuildTool(deps, files, signals);
  const runtime = detectRuntime(files, enginesNode, signals);

  return JSON.stringify({ framework, styling, rendering, buildTool, runtime, signals }, null, 2);
}

function allDeps(json: any): Map<string, string> {
  const deps = new Map<string, string>();
  for (const key of ["dependencies", "devDependencies"]) {
    const obj = json?.[key];
    if (obj && typeof obj === "object") for (const [k, v] of Object.entries(obj)) deps.set(k, String(v));
  }
  return deps;
}

function fileExists(files: string[], name: string): boolean {
  return files.some(f => f === name || f.endsWith(`/${name}`));
}

function detectFramework(deps: Map<string, string>, files: string[], signals: string[]): Framework[] {
  const result: Framework[] = [];
  if (deps.has("next")) { result.push("nextjs"); signals.push("Found next dependency"); }
  if ((deps.has("react") || deps.has("react-dom")) && !result.includes("nextjs")) { result.push("react"); signals.push("Found react dependency"); }
  if (deps.has("vue")) { result.push(deps.has("nuxt") ? "nuxt" : "vue"); signals.push(deps.has("nuxt") ? "Found nuxt dependency" : "Found vue dependency"); }
  if (deps.has("svelte")) { result.push(deps.has("@sveltejs/kit") ? "sveltekit" : "svelte"); signals.push(deps.has("@sveltejs/kit") ? "Found SvelteKit dependency" : "Found svelte dependency"); }
  if (deps.has("astro")) { result.push("astro"); signals.push("Found astro dependency"); }
  if (deps.has("@remix-run/react") || deps.has("@remix-run/node")) { result.push("remix"); signals.push("Found Remix dependency"); }
  if (deps.has("solid-js")) { result.push("solidjs"); signals.push("Found solid-js dependency"); }
  if (deps.has("@angular/core")) { result.push("angular"); signals.push("Found Angular dependency"); }
  if (fileExists(files, "svelte.config.js") && !result.some(f => f === "svelte" || f === "sveltekit")) { result.push("svelte"); signals.push("Found svelte.config.js"); }
  if ((fileExists(files, "next.config.js") || fileExists(files, "next.config.mjs")) && !result.includes("nextjs")) { result.push("nextjs"); signals.push("Found next.config"); }
  if (fileExists(files, "astro.config.mjs") && !result.includes("astro")) { result.push("astro"); signals.push("Found astro.config.mjs"); }
  return result;
}

function detectStyling(deps: Map<string, string>, files: string[], signals: string[]): StylingApproach[] {
  const result: StylingApproach[] = [];
  if (fileExists(files, "tailwind.config.js") || fileExists(files, "tailwind.config.ts") || fileExists(files, "tailwind.config.mjs")) { result.push("tailwind"); signals.push("Found tailwind.config"); }
  if (deps.has("tailwindcss") && !result.includes("tailwind")) { result.push("tailwind"); signals.push("Found tailwindcss dep"); }
  if (deps.has("sass") || deps.has("node-sass")) { result.push("sass"); signals.push("Found sass dep"); }
  if (deps.has("styled-components") || deps.has("@emotion/styled")) { result.push("styled_components"); signals.push("Found CSS-in-JS dep"); }
  if (deps.has("unocss")) { result.push("unocss"); signals.push("Found unocss dep"); }
  if (deps.has("@pandacss/dev")) { result.push("pandacss"); signals.push("Found pandacss dep"); }
  if (files.some(f => f.includes(".module.")) && !result.includes("css_modules")) { result.push("css_modules"); signals.push("Found .module.css files"); }
  if (result.length === 0) result.push("vanilla_css");
  return result;
}

function detectRendering(frameworks: Framework[], fc: Record<string, string>, signals: string[]): RenderingStrategy {
  if (frameworks.includes("nextjs")) {
    const config = fc["next.config.js"] ?? fc["next.config.mjs"];
    if (config && (config.includes("output: 'export'") || config.includes('output: "export"'))) { signals.push("Next.js static export detected"); return "static"; }
    signals.push("Next.js: default SSG/ISR"); return "incremental";
  }
  if (frameworks.includes("astro")) { signals.push("Astro: default SSG"); return "static"; }
  if (frameworks.includes("sveltekit")) { signals.push("SvelteKit: hybrid"); return "server_side"; }
  if (frameworks.includes("react")) { signals.push("React: default CSR"); return "client_side"; }
  return "unknown";
}

function detectBuildTool(deps: Map<string, string>, files: string[], signals: string[]): string | null {
  if (fileExists(files, "vite.config.ts") || fileExists(files, "vite.config.js")) { signals.push("Found vite config"); return "Vite"; }
  if (fileExists(files, "webpack.config.js")) { signals.push("Found webpack config"); return "Webpack"; }
  if (fileExists(files, "turbo.json")) { signals.push("Found turbo config"); return "Turbopack/Turborepo"; }
  if (deps.has("vite")) { signals.push("Found vite dep"); return "Vite"; }
  if (deps.has("webpack")) { signals.push("Found webpack dep"); return "Webpack"; }
  return null;
}

function detectRuntime(files: string[], enginesNode: string | undefined, signals: string[]): string | null {
  if (fileExists(files, "bun.lock") || fileExists(files, "bun.lockb")) { signals.push("Found bun.lock"); return "Bun"; }
  if (fileExists(files, "deno.json") || fileExists(files, "deno.lock")) { signals.push("Found deno config"); return "Deno"; }
  if (enginesNode) { signals.push(`Node.js engine: ${enginesNode}`); return `Node.js (${enginesNode})`; }
  return "Node.js";
}
