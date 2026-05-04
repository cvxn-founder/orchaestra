// ─── Tool Registry ─────────────────────────────────────────────────────────────
// Mirroring webuildfast tool-functions pattern:
//   Category directories → each tool exports { name, description, inputSchema, run }
//   Registry aggregates all tools, builds OpenAI-compatible schemas, dispatches execution.

import * as shell from "./shell/shell";
import * as read_file from "./io/read_file";
import * as write_file from "./io/write_file";
import * as edit_file from "./io/edit_file";
import * as list_dir from "./io/list_dir";
import * as grep from "./io/grep";
import * as glob from "./io/glob";
import * as git_diff from "./git/git_diff";
import * as git_log from "./git/git_log";
import * as git_status from "./git/git_status";
import * as web_fetch from "./web/web_fetch";
import * as web_search from "./web/web_search";
import * as read_json from "./project/read_json";
import * as run_tests from "./project/run_tests";
import * as typecheck from "./project/typecheck";
import * as package_info from "./project/package_info";
import * as ask_user from "./meta/ask_user";
import * as write_plan from "./meta/write_plan";
import * as remember from "./meta/remember";
import * as stack_detect from "./compute/stack_detect";
import * as element_semantic from "./compute/element_semantic";
import * as ngmi_ascii from "./compute/ngmi_ascii";
import * as autoschema_discover from "./compute/autoschema_discover";
import * as factsearch_parse from "./compute/factsearch_parse";
import * as element_inspect from "./compute/element_inspect";
import * as element_uilang from "./compute/element_uilang";
import * as element_componentize from "./compute/element_componentize";
import * as element_edit from "./compute/element_edit";
import * as polymarket_scorer from "./compute/polymarket_scorer";
import * as polymarket_ctf from "./compute/polymarket_ctf";
import { ADMIN_USER_TOOLS } from "./admin/users";
import { ADMIN_D1_TOOLS } from "./admin/d1";
import { ADMIN_R2_TOOLS } from "./admin/r2";
import { ADMIN_CH_TOOLS } from "./admin/clickhouse";
import { ADMIN_R2_WRITE_TOOLS } from "./admin/r2_write";
import { ADMIN_MISC_TOOLS } from "./admin/misc";

export interface ToolModule {
  name: string;
  description: string;
  category: string;
  inputSchema: Record<string, any>;
  run: (input: any) => Promise<string>;
}

const RAW: ToolModule[] = [
  // io/
  { ...read_file, category: "io" },
  { ...write_file, category: "io" },
  { ...edit_file, category: "io" },
  { ...list_dir, category: "io" },
  { ...grep, category: "io" },
  { ...glob, category: "io" },
  // shell/
  { ...shell, category: "shell" },
  // git/
  { ...git_diff, category: "git" },
  { ...git_log, category: "git" },
  { ...git_status, category: "git" },
  // web/
  { ...web_fetch, category: "web" },
  { ...web_search, category: "web" },
  // project/
  { ...read_json, category: "project" },
  { ...run_tests, category: "project" },
  { ...typecheck, category: "project" },
  { ...package_info, category: "project" },
  // meta/
  { ...ask_user, category: "meta" },
  { ...write_plan, category: "meta" },
  { ...remember, category: "meta" },
  // compute/
  { ...stack_detect, category: "compute" },
  { ...element_semantic, category: "compute" },
  { ...ngmi_ascii, category: "compute" },
  { ...autoschema_discover, category: "compute" },
  { ...factsearch_parse, category: "compute" },
  { ...polymarket_scorer, category: "compute" },
  { ...polymarket_ctf, category: "compute" },
  // betterux/
  { ...element_inspect, category: "betterux" },
  { ...element_uilang, category: "betterux" },
  { ...element_componentize, category: "betterux" },
  { ...element_edit, category: "betterux" },
  // admin/
  ...ADMIN_USER_TOOLS,
  ...ADMIN_D1_TOOLS,
  ...ADMIN_R2_TOOLS,
  ...ADMIN_R2_WRITE_TOOLS,
  ...ADMIN_CH_TOOLS,
  ...ADMIN_MISC_TOOLS,
];

export const TOOL_MODULES: ToolModule[] = RAW;
export const TOOL_MAP: Map<string, ToolModule> = new Map(RAW.map(t => [t.name, t]));

export function buildToolSchemas() {
  return TOOL_MODULES.map(t => ({
    type: "function" as const,
    function: {
      name: t.name,
      description: t.description,
      parameters: t.inputSchema,
    },
  }));
}

export async function executeTool(name: string, args: string): Promise<string> {
  const tool = TOOL_MAP.get(name);
  if (!tool) return `Unknown tool: ${name}`;
  let parsed: any;
  try { parsed = JSON.parse(args); } catch { parsed = {}; }
  try {
    return await tool.run(parsed);
  } catch (e: any) {
    return `Tool error (${name}): ${e.message}`;
  }
}
