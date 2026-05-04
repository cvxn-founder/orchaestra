import { chQuery } from "./_ch_client";
import type { ToolModule } from "../index";

export const name = "ds_ch_welch_ttest";
export const description = "Welch's t-test for unequal variances. Returns t-statistic, p-value, CI. Runs in ClickHouse using welchTTest().";
export const category = "compute";

export const inputSchema = {
  type: "object",
  properties: {
    table: { type: "string", description: "Table name" },
    valueColumn: { type: "string", description: "Numeric column" },
    groupColumn: { type: "string", description: "Group column" },
    groupA: { type: "string", description: "Value for group A" },
    groupB: { type: "string", description: "Value for group B" },
    database: { type: "string", default: "medtech_raw", description: "Database name" },
  },
  required: ["table", "valueColumn", "groupColumn", "groupA", "groupB"],
};

export async function run(input: any): Promise<string> {
  const { table, valueColumn, groupColumn, groupA, groupB, database } = input;
  const db = database ?? "medtech_raw";

  const sql = `SELECT welchTTest(group_a, group_b) AS result FROM (SELECT groupArrayIf(${valueColumn}, ${groupColumn} = '${groupA}') AS group_a, groupArrayIf(${valueColumn}, ${groupColumn} = '${groupB}') AS group_b FROM ${db}.${table})`;
  const res = await chQuery(sql, db);
  const r = res?.data?.[0]?.result ?? res?.[0]?.result ?? {};
  const t = r[0] ?? 0, p = r[1] ?? 1;
  const sig = p < 0.001 ? "***" : p < 0.01 ? "**" : p < 0.05 ? "*" : "ns";

  return [
    `Welch's t-test (unequal variances)`,
    `────────────────────────────────`,
    `  Comparison: ${groupA} vs ${groupB}`,
    `  Column:     ${db}.${table}.${valueColumn}`,
    `────────────────────────────────`,
    `  t-statistic = ${Number(t).toFixed(4)}`,
    `  p-value     = ${Number(p).toFixed(6)} ${sig}`,
    `────────────────────────────────`,
    `  Use Welch's when group variances may differ.`,
    ``,
    `JSON: ${JSON.stringify(r)}`,
  ].join("\n");
}
