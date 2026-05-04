import { chQuery } from "./_ch_client";
import type { ToolModule } from "../index";

export const name = "ds_ch_anomaly";
export const description = "Z-score based anomaly detection. Returns rows more than N standard deviations from the mean. Runs in ClickHouse.";
export const category = "compute";

export const inputSchema = {
  type: "object",
  properties: {
    table: { type: "string", description: "Table name" },
    valueColumn: { type: "string", description: "Numeric column to scan for anomalies" },
    timeColumn: { type: "string", description: "Optional: date column for context" },
    threshold: { type: "number", default: 3, description: "Z-score threshold (default 3)" },
    database: { type: "string", default: "medtech_raw", description: "Database name" },
    limit: { type: "integer", default: 100, description: "Max anomalies to return" },
  },
  required: ["table", "valueColumn"],
};

export async function run(input: any): Promise<string> {
  const { table, valueColumn, database } = input;
  const db = database ?? "medtech_raw";
  const threshold = input.threshold ?? 3;
  const limit = input.limit ?? 100;
  const timeCol = input.timeColumn ? `, ${input.timeColumn}` : "";

  const sql = `SELECT *, (${valueColumn} - avg_val) / nullIf(std_val, 0) AS zscore FROM (SELECT *, avg(${valueColumn}) OVER () AS avg_val, stddevSamp(${valueColumn}) OVER () AS std_val FROM ${db}.${table}) WHERE abs(zscore) > ${threshold} ORDER BY abs(zscore) DESC LIMIT ${limit}`;
  const res = await chQuery(sql, db);
  const data = res?.data ?? res ?? [];

  if (!Array.isArray(data) || data.length === 0) return `No anomalies found at Z > ${threshold} in ${db}.${table}.${valueColumn}`;

  const lines = [`Anomaly Detection — ${db}.${table}.${valueColumn}`, `Threshold: Z > ${threshold}  |  Found: ${data.length} outliers`, `────────────────────────────────────────`];
  for (const row of data) {
    const z = Number(row.zscore).toFixed(2);
    const val = Number(row[valueColumn]).toFixed(4);
    const time = timeCol ? String(row[input.timeColumn] ?? "").slice(0, 16) : "";
    lines.push(`  Z=${z.padStart(6)}  value=${val.padStart(12)}  ${time}`);
  }
  return lines.join("\n");
}
