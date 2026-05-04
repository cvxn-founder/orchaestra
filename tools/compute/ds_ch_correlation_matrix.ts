import { chQuery } from "./_ch_client";
import { corrPValue, interpretCorr } from "./_stats";
import type { ToolModule } from "../index";

export const name = "ds_ch_correlation_matrix";
export const description = "Pairwise Pearson correlation matrix for multiple columns. Returns matrix with r values and significance stars. Runs in ClickHouse via corrStable().";
export const category = "compute";

export const inputSchema = {
  type: "object",
  properties: {
    table: { type: "string", description: "Table name" },
    columns: { type: "array", items: { type: "string" }, description: "Numeric column names" },
    database: { type: "string", default: "medtech_raw", description: "Database name" },
    where: { type: "string", description: "Optional WHERE clause" },
  },
  required: ["table", "columns"],
};

export async function run(input: any): Promise<string> {
  const { table, columns, database } = input;
  const db = database ?? "medtech_raw";
  const where = input.where ? `WHERE ${input.where}` : "";
  const cols = columns as string[];
  if (cols.length < 2) return "Need at least 2 columns";

  // Get n
  const nSql = `SELECT count(*) AS n FROM ${db}.${table} ${where}`;
  const nRes = await chQuery(nSql, db);
  const n = nRes?.data?.[0]?.n ?? nRes?.[0]?.n ?? 0;

  const lines = [`Correlation Matrix — ${db}.${table}`, `n = ${n}   * p<0.05  ** p<0.01  *** p<0.001`, `──────────────────────────────────────────`];
  const matrix: number[][] = [];

  for (let i = 0; i < cols.length; i++) {
    matrix[i] = [];
    for (let j = 0; j < cols.length; j++) {
      if (i === j) { matrix[i][j] = 1; continue; }
      if (j < i) { matrix[i][j] = matrix[j][i]; continue; }
      const sql = `SELECT corrStable(${cols[i]}, ${cols[j]}) AS r FROM ${db}.${table} ${where}`;
      const res = await chQuery(sql, db);
      const r = Number(res?.data?.[0]?.r ?? res?.[0]?.r ?? 0);
      matrix[i][j] = r;
    }
  }

  // Header
  const maxLen = Math.max(...cols.map(c => c.length));
  lines.push("  " + " ".repeat(maxLen) + "  " + cols.map(c => c.padStart(8)).join(""));
  for (let i = 0; i < cols.length; i++) {
    const vals = cols.map((_, j) => {
      const r = matrix[i][j];
      const p = corrPValue(r, n);
      const star = p < 0.001 ? "***" : p < 0.01 ? "**" : p < 0.05 ? "*" : " ";
      return `${r.toFixed(3)}${star}`.padStart(9);
    }).join("");
    lines.push(`  ${cols[i].padEnd(maxLen)} ${vals}`);
  }

  return lines.join("\n");
}
