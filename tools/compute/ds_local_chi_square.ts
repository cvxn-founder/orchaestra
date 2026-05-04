import { chQuery } from "./_ch_client";
import { chiSqPValue } from "./_stats";
import type { ToolModule } from "../index";

export const name = "ds_local_chi_square";
export const description = "Chi-square test of independence for two categorical columns. CH fetches the contingency table; local TS computes χ² statistic, df, p-value, and Cramér's V effect size.";
export const category = "compute";

export const inputSchema = {
  type: "object",
  properties: {
    table: { type: "string", description: "Table name" },
    columnA: { type: "string", description: "First categorical column" },
    columnB: { type: "string", description: "Second categorical column" },
    database: { type: "string", default: "medtech_raw", description: "Database name" },
  },
  required: ["table", "columnA", "columnB"],
};

export async function run(input: any): Promise<string> {
  const { table, columnA, columnB, database } = input;
  const db = database ?? "medtech_raw";

  const sql = `SELECT ${columnA}, ${columnB}, count(*) AS cnt FROM ${db}.${table} GROUP BY ${columnA}, ${columnB} ORDER BY ${columnA}, ${columnB}`;
  const res = await chQuery(sql, db);
  const data = res?.data ?? res ?? [];
  if (!Array.isArray(data) || data.length < 2) return "Need at least 2 cells for chi-square test.";

  // Build contingency table
  const rowMap = new Map<string, Map<string, number>>();
  const allCols = new Set<string>();
  for (const r of data) {
    const a = String(r[columnA] ?? ""), b = String(r[columnB] ?? "");
    const cnt = Number(r.cnt ?? 0);
    if (!rowMap.has(a)) rowMap.set(a, new Map());
    rowMap.get(a)!.set(b, cnt);
    allCols.add(b);
  }
  const rows = [...rowMap.keys()], cols = [...allCols];
  const R = rows.length, C = cols.length;
  if (R < 2 || C < 2) return `Need at least 2×2 table. Got ${R}×${C}.`;

  // Observed and expected
  const obs: number[][] = [], rowSums: number[] = new Array(R).fill(0), colSums: number[] = new Array(C).fill(0);
  let total = 0;
  for (let i = 0; i < R; i++) {
    obs[i] = [];
    for (let j = 0; j < C; j++) {
      const v = rowMap.get(rows[i])?.get(cols[j]) ?? 0;
      obs[i][j] = v;
      rowSums[i] += v;
      colSums[j] += v;
      total += v;
    }
  }

  let chi = 0;
  const tableLines: string[] = [];
  for (let i = 0; i < R; i++) {
    for (let j = 0; j < C; j++) {
      const exp = (rowSums[i] * colSums[j]) / total;
      if (exp > 0) chi += (obs[i][j] - exp) ** 2 / exp;
    }
  }
  const df = (R - 1) * (C - 1);
  const p = chiSqPValue(chi, df);
  const cramersV = Math.sqrt(chi / (total * Math.min(R - 1, C - 1)));
  const sig = p < 0.001 ? "***" : p < 0.01 ? "**" : p < 0.05 ? "*" : "ns";

  return [
    `Chi-Square Test of Independence`,
    `────────────────────────────────`,
    `  ${columnA} × ${columnB}`,
    `  Table: ${db}.${table} (${R}×${C})`,
    `────────────────────────────────`,
    `  χ²(${df})  = ${chi.toFixed(4)}`,
    `  p-value = ${p.toFixed(6)} ${sig}`,
    `  Cramér's V = ${cramersV.toFixed(4)}`,
    `────────────────────────────────`,
    `  Total observations: ${total}`,
    `  ${p < 0.05 ? "Significant association detected" : "No significant association"}`,
  ].join("\n");
}
