import { chQuery } from "./_ch_client";
import type { ToolModule } from "../index";

export const name = "ds_ch_ttest";
export const description = "Student's t-test comparing two groups. Returns t-statistic, p-value, confidence interval. Runs entirely in ClickHouse using studentTTest().";
export const category = "compute";

export const inputSchema = {
  type: "object",
  properties: {
    table: { type: "string", description: "Table name" },
    valueColumn: { type: "string", description: "Numeric column to compare" },
    groupColumn: { type: "string", description: "Categorical column defining groups" },
    groupA: { type: "string", description: "Value for group A in the group column" },
    groupB: { type: "string", description: "Value for group B in the group column" },
    database: { type: "string", default: "medtech_raw", description: "Database name" },
  },
  required: ["table", "valueColumn", "groupColumn", "groupA", "groupB"],
};

export async function run(input: any): Promise<string> {
  const { table, valueColumn, groupColumn, groupA, groupB, database } = input;
  const db = database ?? "medtech_raw";

  const sql = `SELECT studentTTest(group_a, group_b) AS result FROM (SELECT groupArrayIf(${valueColumn}, ${groupColumn} = '${groupA}') AS group_a, groupArrayIf(${valueColumn}, ${groupColumn} = '${groupB}') AS group_b FROM ${db}.${table})`;
  const res = await chQuery(sql, db);
  const r = res?.data?.[0]?.result ?? res?.[0]?.result ?? {};
  const t = r[0] ?? 0, p = r[1] ?? 1, ciLo = r[2], ciHi = r[3];

  const sig = p < 0.001 ? "***" : p < 0.01 ? "**" : p < 0.05 ? "*" : "ns";

  return [
    `Student's t-test`,
    `────────────────────────────────`,
    `  Comparison: ${groupA} vs ${groupB}`,
    `  Column:     ${db}.${table}.${valueColumn}`,
    `────────────────────────────────`,
    `  t-statistic   = ${Number(t).toFixed(4)}`,
    `  p-value       = ${Number(p).toFixed(6)} ${sig}`,
    ciLo != null ? `  95% CI        = [${Number(ciLo).toFixed(4)}, ${Number(ciHi).toFixed(4)}]` : "",
    `────────────────────────────────`,
    `  Interpretation: ${sig === "ns" ? "No significant difference" : `Significant difference (p ${p < 0.001 ? "< 0.001" : "= " + Number(p).toFixed(4)})`}`,
    ``,
    `JSON: ${JSON.stringify(r)}`,
  ].filter(Boolean).join("\n");
}
