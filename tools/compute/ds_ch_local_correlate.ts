import { chQuery } from "./_ch_client";
import { corrPValue, interpretCorr } from "./_stats";
import type { ToolModule } from "../index";

export const name = "ds_ch_local_correlate";
export const description = "Pearson correlation with full stats: r, R², p-value, 95% CI (via Fisher z-transform), interpretation. CH computes r + n, local TS computes derived stats.";
export const category = "compute";

export const inputSchema = {
  type: "object",
  properties: {
    table: { type: "string", description: "Table name (or join query)" },
    columnX: { type: "string", description: "First numeric column" },
    columnY: { type: "string", description: "Second numeric column" },
    database: { type: "string", default: "medtech_raw", description: "Database name" },
    where: { type: "string", description: "Optional WHERE clause" },
  },
  required: ["table", "columnX", "columnY"],
};

export async function run(input: any): Promise<string> {
  const { table, columnX, columnY, database } = input;
  const db = database ?? "medtech_raw";
  const where = input.where ? `WHERE ${input.where}` : "";

  const sql = `SELECT corrStable(${columnX}, ${columnY}) AS r, count(*) AS n FROM ${db}.${table} ${where}`;
  const res = await chQuery(sql, db);
  const row = res?.data?.[0] ?? res?.[0] ?? {};
  const r = Number(row.r ?? 0), n = Number(row.n ?? 0);
  const r2 = r * r;
  const p = corrPValue(r, n);

  // Fisher z-transform 95% CI
  const z = 0.5 * Math.log((1 + r) / (1 - r));
  const se = 1 / Math.sqrt(n - 3);
  const ciLo = Math.tanh(z - 1.96 * se);
  const ciHi = Math.tanh(z + 1.96 * se);

  return [
    `Pearson Correlation`,
    `────────────────────────────────`,
    `  ${columnX} vs ${columnY}`,
    `  Table: ${db}.${table}`,
    `────────────────────────────────`,
    `  r  = ${r.toFixed(4)}`,
    `  R² = ${r2.toFixed(4)} (${(r2 * 100).toFixed(1)}% shared variance)`,
    `  n  = ${n}`,
    `  p  = ${p.toFixed(6)} ${p < 0.001 ? "***" : p < 0.01 ? "**" : p < 0.05 ? "*" : ""}`,
    `  95% CI = [${ciLo.toFixed(4)}, ${ciHi.toFixed(4)}]`,
    `────────────────────────────────`,
    `  Interpretation: ${interpretCorr(r)} ${r > 0 ? "positive" : "negative"} correlation`,
    p > 0.05 ? `  Not statistically significant at α = 0.05` : "",
  ].join("\n");
}
