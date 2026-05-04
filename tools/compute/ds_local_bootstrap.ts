import { chQuery } from "./_ch_client";
import type { ToolModule } from "../index";

export const name = "ds_local_bootstrap";
export const description = "Bootstrap confidence intervals for any statistic (mean, median, correlation). CH provides the data; local TS does N resamples with replacement and reports CI at configurable confidence level.";
export const category = "compute";

export const inputSchema = {
  type: "object",
  properties: {
    table: { type: "string", description: "Table name" },
    column: { type: "string", description: "Numeric column" },
    columnY: { type: "string", description: "Second column (optional, for correlation bootstrap)" },
    statistic: { type: "string", enum: ["mean", "median", "correlation", "std"], default: "mean" },
    samples: { type: "integer", default: 1000, description: "Number of bootstrap resamples (100-10000)" },
    confidence: { type: "number", default: 95, description: "Confidence level (90, 95, 99)" },
    database: { type: "string", default: "medtech_raw", description: "Database name" },
  },
  required: ["table", "column"],
};

function computeStat(data: number[], dataY: number[] | null, stat: string): number {
  if (stat === "correlation" && dataY) {
    return pearson(data, dataY);
  }
  if (stat === "median") { const s = [...data].sort((a, b) => a - b); return s[Math.floor(s.length / 2)]; }
  if (stat === "std") { const m = data.reduce((a, b) => a + b, 0) / data.length; return Math.sqrt(data.reduce((a, b) => a + (b - m) ** 2, 0) / (data.length - 1)); }
  return data.reduce((a, b) => a + b, 0) / data.length; // mean
}

function pearson(xs: number[], ys: number[]): number {
  const n = xs.length;
  let sx = 0, sy = 0, sxy = 0, sx2 = 0, sy2 = 0;
  for (let i = 0; i < n; i++) { sx += xs[i]; sy += ys[i]; sxy += xs[i] * ys[i]; sx2 += xs[i] ** 2; sy2 += ys[i] ** 2; }
  const num = n * sxy - sx * sy;
  const den = Math.sqrt((n * sx2 - sx * sx) * (n * sy2 - sy * sy));
  return den === 0 ? 0 : num / den;
}

export async function run(input: any): Promise<string> {
  const { table, column, columnY, database } = input;
  const db = database ?? "medtech_raw";
  const stat = input.statistic ?? "mean";
  const samples = Math.min(input.samples ?? 1000, 10000);
  const conf = input.confidence ?? 95;

  // Fetch data
  const cols = columnY ? `${column}, ${columnY}` : column;
  const sql = `SELECT ${cols} FROM ${db}.${table} LIMIT 5000`;
  const res = await chQuery(sql, db);
  const data = res?.data ?? res ?? [];
  if (!Array.isArray(data) || data.length < 5) return "Need at least 5 observations for bootstrap.";

  const xs: number[] = [], ys: number[] = [];
  for (const r of data) { xs.push(Number(r[column] ?? 0)); if (columnY) ys.push(Number(r[columnY] ?? 0)); }

  const observed = computeStat(xs, columnY ? ys : null, stat);

  // Bootstrap
  const bootStats: number[] = [];
  for (let b = 0; b < samples; b++) {
    const bx: number[] = [], by: number[] = [];
    for (let i = 0; i < xs.length; i++) {
      const idx = Math.floor(Math.random() * xs.length);
      bx.push(xs[idx]);
      if (columnY) by.push(ys[idx]);
    }
    bootStats.push(computeStat(bx, columnY ? by : null, stat));
  }
  bootStats.sort((a, b) => a - b);

  const alpha = (100 - conf) / 100;
  const loIdx = Math.floor(samples * alpha / 2);
  const hiIdx = Math.floor(samples * (1 - alpha / 2));
  const ciLo = bootStats[loIdx], ciHi = bootStats[hiIdx];
  const bootMean = bootStats.reduce((a, b) => a + b, 0) / samples;
  const bootSe = Math.sqrt(bootStats.reduce((a, b) => a + (b - bootMean) ** 2, 0) / (samples - 1));

  return [
    `Bootstrap Analysis`,
    `────────────────────────────────`,
    `  Statistic: ${stat}`,
    `  Observed:  ${observed.toFixed(4)}`,
    `  Bootstrap: ${bootMean.toFixed(4)} (mean of ${samples} resamples)`,
    `  Bootstrap SE: ${bootSe.toFixed(4)}`,
    `  ${conf}% CI:   [${ciLo.toFixed(4)}, ${ciHi.toFixed(4)}]`,
    `────────────────────────────────`,
    `  n = ${xs.length} observations, ${samples} bootstrap samples`,
  ].join("\n");
}
