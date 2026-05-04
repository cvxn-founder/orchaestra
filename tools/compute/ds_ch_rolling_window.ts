import { chQuery } from "./_ch_client";
import type { ToolModule } from "../index";

export const name = "ds_ch_rolling_window";
export const description = "Rolling window statistics: moving average, moving sum, Z-scores. For time series data ordered by a date column. Runs in ClickHouse.";
export const category = "compute";

export const inputSchema = {
  type: "object",
  properties: {
    table: { type: "string", description: "Table name" },
    valueColumn: { type: "string", description: "Numeric column" },
    timeColumn: { type: "string", description: "Date/time column for ordering" },
    windowSize: { type: "integer", default: 7, description: "Window size in rows" },
    mode: { type: "string", enum: ["avg", "sum", "zscore"], default: "avg", description: "Window type" },
    database: { type: "string", default: "medtech_raw", description: "Database name" },
    limit: { type: "integer", default: 50, description: "Max rows returned" },
  },
  required: ["table", "valueColumn", "timeColumn"],
};

export async function run(input: any): Promise<string> {
  const { table, valueColumn, timeColumn, database } = input;
  const db = database ?? "medtech_raw";
  const ws = input.windowSize ?? 7;
  const mode = input.mode ?? "avg";
  const limit = input.limit ?? 50;

  let selectExpr: string;
  if (mode === "zscore") {
    selectExpr = `(${valueColumn} - avg(${valueColumn}) OVER (ORDER BY ${timeColumn} ROWS BETWEEN ${ws} PRECEDING AND ${ws} FOLLOWING)) / nullIf(stddevSamp(${valueColumn}) OVER (ORDER BY ${timeColumn} ROWS BETWEEN ${ws} PRECEDING AND ${ws} FOLLOWING), 0) AS zscore`;
  } else if (mode === "sum") {
    selectExpr = `sum(${valueColumn}) OVER (ORDER BY ${timeColumn} ROWS BETWEEN ${ws - 1} PRECEDING AND CURRENT ROW) AS rolling_sum`;
  } else {
    selectExpr = `avg(${valueColumn}) OVER (ORDER BY ${timeColumn} ROWS BETWEEN ${ws - 1} PRECEDING AND CURRENT ROW) AS rolling_avg`;
  }

  const sql = `SELECT ${timeColumn}, ${valueColumn}, ${selectExpr} FROM ${db}.${table} ORDER BY ${timeColumn} DESC LIMIT ${limit}`;
  const res = await chQuery(sql, db);
  const data = res?.data ?? res ?? [];

  if (!Array.isArray(data) || data.length === 0) return `No data returned. SQL: ${sql}`;

  const lines = [`Rolling ${mode.toUpperCase()} — ${db}.${table}.${valueColumn}`, `Window: ${ws} rows, last ${data.length} observations`, `────────────────────────────────────────`];
  for (const row of data) {
    const val = row[valueColumn] ?? row[0];
    const stat = row.rolling_avg ?? row.rolling_sum ?? row.zscore ?? row[1];
    const time = row[timeColumn] ?? "";
    lines.push(`  ${String(time).slice(0, 16)}  raw=${Number(val).toFixed(4).padStart(10)}  ${mode}=${Number(stat).toFixed(4)}`);
  }
  return lines.join("\n");
}
