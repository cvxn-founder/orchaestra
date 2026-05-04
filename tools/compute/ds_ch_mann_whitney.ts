import { chQuery } from "./_ch_client";
import type { ToolModule } from "../index";

export const name = "ds_ch_mann_whitney";
export const description = "Mann-Whitney U test (non-parametric). Returns U statistic and p-value. Runs entirely in ClickHouse using mannWhitneyUTest().";
export const category = "compute";

export const inputSchema = {
  type: "object",
  properties: {
    table: { type: "string", description: "Table name" },
    valueColumn: { type: "string", description: "Numeric column to compare" },
    groupColumn: { type: "string", description: "Categorical column defining groups" },
    groupA: { type: "string", description: "Value for group A" },
    groupB: { type: "string", description: "Value for group B" },
    database: { type: "string", default: "medtech_raw", description: "Database name" },
  },
  required: ["table", "valueColumn", "groupColumn", "groupA", "groupB"],
};

export async function run(input: any): Promise<string> {
  const { table, valueColumn, groupColumn, groupA, groupB, database } = input;
  const db = database ?? "medtech_raw";

  const sql = `SELECT mannWhitneyUTest(group_a, group_b) AS result FROM (SELECT groupArrayIf(${valueColumn}, ${groupColumn} = '${groupA}') AS group_a, groupArrayIf(${valueColumn}, ${groupColumn} = '${groupB}') AS group_b FROM ${db}.${table})`;
  const res = await chQuery(sql, db);
  const r = res?.data?.[0]?.result ?? res?.[0]?.result ?? {};
  const u = r[0] ?? 0, p = r[1] ?? 1;
  const sig = p < 0.05 ? "significant" : "not significant";

  return [
    `Mann-Whitney U Test`,
    `────────────────────────────────`,
    `  Comparison: ${groupA} vs ${groupB}`,
    `  Column:     ${db}.${table}.${valueColumn}`,
    `────────────────────────────────`,
    `  U statistic = ${Number(u).toFixed(2)}`,
    `  p-value     = ${Number(p).toFixed(6)}`,
    `────────────────────────────────`,
    `  Result: ${p < 0.05 ? "Significant difference detected" : "No significant difference"}`,
    `  Use when data is non-normal or ordinal.`,
    ``,
    `JSON: ${JSON.stringify(r)}`,
  ].join("\n");
}
