import { chQuery } from "./_ch_client";
import { fPValue } from "./_stats";
import type { ToolModule } from "../index";

export const name = "ds_local_anova";
export const description = "One-way ANOVA. CH provides group means, counts, and stddevs; local TS computes between/within SS, F-statistic, p-value, eta-squared effect size.";
export const category = "compute";

export const inputSchema = {
  type: "object",
  properties: {
    table: { type: "string", description: "Table name" },
    valueColumn: { type: "string", description: "Numeric dependent variable" },
    groupColumn: { type: "string", description: "Categorical factor" },
    database: { type: "string", default: "medtech_raw", description: "Database name" },
  },
  required: ["table", "valueColumn", "groupColumn"],
};

export async function run(input: any): Promise<string> {
  const { table, valueColumn, groupColumn, database } = input;
  const db = database ?? "medtech_raw";

  const sql = `SELECT ${groupColumn}, count(*) AS n, avg(${valueColumn}) AS mean, stddevSamp(${valueColumn}) AS std FROM ${db}.${table} GROUP BY ${groupColumn} ORDER BY ${groupColumn}`;
  const res = await chQuery(sql, db);
  const data = res?.data ?? res ?? [];
  if (!Array.isArray(data) || data.length < 2) return "Need at least 2 groups for ANOVA.";

  const groups: { label: string; n: number; mean: number; std: number }[] = [];
  for (const r of data) groups.push({ label: String(r[groupColumn] ?? ""), n: Number(r.n ?? 0), mean: Number(r.mean ?? 0), std: Number(r.std ?? 0) });

  const grandN = groups.reduce((s, g) => s + g.n, 0);
  const grandMean = groups.reduce((s, g) => s + g.n * g.mean, 0) / grandN;
  const ssBetween = groups.reduce((s, g) => s + g.n * (g.mean - grandMean) ** 2, 0);
  const ssWithin = groups.reduce((s, g) => s + (g.n - 1) * g.std ** 2, 0);
  const dfBetween = groups.length - 1;
  const dfWithin = grandN - groups.length;
  const msBetween = ssBetween / dfBetween;
  const msWithin = ssWithin / dfWithin;
  const f = msWithin > 0 ? msBetween / msWithin : 0;
  const p = fPValue(f, dfBetween, dfWithin);
  const etaSq = ssBetween / (ssBetween + ssWithin);
  const sig = p < 0.001 ? "***" : p < 0.01 ? "**" : p < 0.05 ? "*" : "ns";

  const lines = [
    `One-Way ANOVA`,
    `────────────────────────────────`,
    `  DV: ${valueColumn}  |  Factor: ${groupColumn}`,
    `────────────────────────────────`,
    `  Between: SS = ${ssBetween.toFixed(2)}, df = ${dfBetween}, MS = ${msBetween.toFixed(2)}`,
    `  Within:  SS = ${ssWithin.toFixed(2)}, df = ${dfWithin}, MS = ${msWithin.toFixed(2)}`,
    `────────────────────────────────`,
    `  F(${dfBetween}, ${dfWithin}) = ${f.toFixed(2)}`,
    `  p-value = ${p.toFixed(6)} ${sig}`,
    `  η²     = ${etaSq.toFixed(4)} (${(etaSq * 100).toFixed(1)}% variance explained)`,
    `────────────────────────────────`,
  ];
  for (const g of groups) lines.push(`  ${g.label}: n=${g.n}, mean=${g.mean.toFixed(4)}, std=${g.std.toFixed(4)}`);
  lines.push(`  Grand mean: ${grandMean.toFixed(4)} (N=${grandN})`);
  lines.push(p < 0.05 ? `  → Significant differences exist between groups` : `  → No significant differences between groups`);
  return lines.join("\n");
}
