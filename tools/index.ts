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
import * as ds_ch_describe from "./compute/ds_ch_describe";
import * as ds_ch_ttest from "./compute/ds_ch_ttest";
import * as ds_ch_mann_whitney from "./compute/ds_ch_mann_whitney";
import * as ds_ch_lead_lag from "./compute/ds_ch_lead_lag";
import * as ds_ch_event_study from "./compute/ds_ch_event_study";
import * as ds_ch_correlation_matrix from "./compute/ds_ch_correlation_matrix";
import * as ds_ch_rolling_window from "./compute/ds_ch_rolling_window";
import * as ds_ch_anomaly from "./compute/ds_ch_anomaly";
import * as ds_ch_welch_ttest from "./compute/ds_ch_welch_ttest";
import * as ds_ch_local_correlate from "./compute/ds_ch_local_correlate";
import * as ds_ch_local_regression from "./compute/ds_ch_local_regression";
import * as ds_ch_local_logistic from "./compute/ds_ch_local_logistic";
import * as ds_local_chi_square from "./compute/ds_local_chi_square";
import * as ds_local_bootstrap from "./compute/ds_local_bootstrap";
import * as ds_local_anova from "./compute/ds_local_anova";
import * as ds_local_effect_size from "./compute/ds_local_effect_size";
import * as ds_local_permutation_test from "./compute/ds_local_permutation_test";
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
  // compute/ds_ch (Tier 1 — ClickHouse native)
  { ...ds_ch_describe, category: "compute" },
  { ...ds_ch_ttest, category: "compute" },
  { ...ds_ch_mann_whitney, category: "compute" },
  { ...ds_ch_lead_lag, category: "compute" },
  { ...ds_ch_event_study, category: "compute" },
  { ...ds_ch_correlation_matrix, category: "compute" },
  { ...ds_ch_rolling_window, category: "compute" },
  { ...ds_ch_anomaly, category: "compute" },
  { ...ds_ch_welch_ttest, category: "compute" },
  // compute/ds_ch_local (Tier 2 — CH + local TS)
  { ...ds_ch_local_correlate, category: "compute" },
  { ...ds_ch_local_regression, category: "compute" },
  { ...ds_ch_local_logistic, category: "compute" },
  // compute/ds_local (Tier 3 — local TS)
  { ...ds_local_chi_square, category: "compute" },
  { ...ds_local_bootstrap, category: "compute" },
  { ...ds_local_anova, category: "compute" },
  { ...ds_local_effect_size, category: "compute" },
  { ...ds_local_permutation_test, category: "compute" },
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
