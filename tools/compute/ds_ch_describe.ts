import { chQuery, buildChDescribeSQL } from "./_ch_client";
import type { ToolModule } from "../index";

export const name = "ds_ch_describe";
export const description = "Descriptive statistics for any ClickHouse column: count, mean, median, Q1, Q3, IQR, stddev, variance, skew, kurtosis, min, max. Runs entirely in ClickHouse.";
export const category = "compute";

export const inputSchema = {
  type: "object",
  properties: {
    table: { type: "string", description: "Table name" },
    column: { type: "string", description: "Numeric column to describe" },
    database: { type: "string", default: "medtech_raw", description: "Database name" },
    where: { type: "string", description: "Optional WHERE clause to filter rows" },
  },
  required: ["table", "column"],
};

export async function run(input: any): Promise<string> {
  const sql = buildChDescribeSQL(input.table, input.column, input.database ?? "medtech_raw", input.where);
  const result = await chQuery(sql, input.database ?? "medtech_raw");
  if (!result?.ok && result?.data?.[0] == null) return `No data: ${JSON.stringify(result)}`;

  const row = result?.data?.[0] ?? result?.[0] ?? {};
  const n = row.n ?? 0;
  const iqr = (row.q3 ?? 0) - (row.q1 ?? 0);
  const cv = row.mean ? (row.std / row.mean * 100) : 0;

  return [
    `Descriptive Statistics — ${input.database}.${input.table}.${input.column}`,
    `─────────────────────────────────────────────`,
    `  n        = ${n}`,
    `  mean     = ${Number(row.mean).toFixed(4)}`,
    `  median   = ${Number(row.median).toFixed(4)}`,
    `  Q1       = ${Number(row.q1).toFixed(4)}`,
    `  Q3       = ${Number(row.q3).toFixed(4)}`,
    `  IQR      = ${iqr.toFixed(4)}`,
    `  std      = ${Number(row.std).toFixed(4)}`,
    `  var      = ${Number(row.var).toFixed(4)}`,
    `  skew     = ${Number(row.skew).toFixed(4)} ${row.skew > 1 ? "(right-tailed)" : row.skew < -1 ? "(left-tailed)" : "(symmetric)"}`,
    `  kurtosis = ${Number(row.kurt).toFixed(4)} ${row.kurt > 3 ? "(leptokurtic)" : "(platykurtic)"}`,
    `  min      = ${Number(row.min).toFixed(4)}`,
    `  max      = ${Number(row.max).toFixed(4)}`,
    `  CV       = ${cv.toFixed(2)}%`,
    `─────────────────────────────────────────────`,
    ``,
    `JSON: ${JSON.stringify(row)}`,
  ].join("\n");
}
