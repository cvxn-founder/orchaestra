import { chQuery } from "./_ch_client";
import { corrPValue, interpretCorr } from "./_stats";
import type { ToolModule } from "../index";

export const name = "ds_ch_lead_lag";
export const description = "Cross-correlation between two time series at multiple lags (-5 to +5). Finds whether one series leads or lags the other. Runs in ClickHouse via window functions.";
export const category = "compute";

export const inputSchema = {
  type: "object",
  properties: {
    table: { type: "string", description: "Table name" },
    series1: { type: "string", description: "First numeric column" },
    series2: { type: "string", description: "Second numeric column" },
    timeColumn: { type: "string", description: "Date/time column for ordering" },
    maxLag: { type: "integer", default: 5, description: "Maximum lag to check (1-10)" },
    database: { type: "string", default: "medtech_raw", description: "Database name" },
  },
  required: ["table", "series1", "series2", "timeColumn"],
};

export async function run(input: any): Promise<string> {
  const { table, series1, series2, timeColumn, database } = input;
  const db = database ?? "medtech_raw";
  const maxLag = Math.min(input.maxLag ?? 5, 10);

  const sql = `SELECT groupArray(${series1}) AS s1, groupArray(${series2}) AS s2 FROM (SELECT ${series1}, ${series2} FROM ${db}.${table} ORDER BY ${timeColumn})`;
  const res = await chQuery(sql, db);
  const data = res?.data?.[0] ?? res?.[0] ?? {};
  const s1: number[] = data.s1 ?? [], s2: number[] = data.s2 ?? [];
  const n = Math.min(s1.length, s2.length);

  const lines = [`Lead/Lag Cross-Correlation`, `────────────────────────────────`, `  Series 1: ${series1}`, `  Series 2: ${series2}`, `  n = ${n} observations`, `────────────────────────────────`];

  for (let lag = -maxLag; lag <= maxLag; lag++) {
    const xs: number[] = [], ys: number[] = [];
    for (let i = 0; i < n; i++) {
      const j = i + lag;
      if (j < 0 || j >= n) continue;
      xs.push(s1[i]); ys.push(s2[j]);
    }
    if (xs.length < 3) continue;
    const r = pearson(xs, ys);
    const p = corrPValue(r, xs.length);
    const label = lag === 0 ? "  lag  0 (simultaneous)" : lag > 0 ? `  lag +${lag} (s1 leads s2)` : `  lag ${lag} (s2 leads s1)`;
    lines.push(`${label}: r = ${r.toFixed(4)}, p = ${p.toFixed(4)} ${p < 0.05 ? "*" : ""} ${interpretCorr(r)}`);
  }
  return lines.join("\n");
}

function pearson(xs: number[], ys: number[]): number {
  const n = xs.length;
  let sx = 0, sy = 0, sxy = 0, sx2 = 0, sy2 = 0;
  for (let i = 0; i < n; i++) { sx += xs[i]; sy += ys[i]; sxy += xs[i] * ys[i]; sx2 += xs[i] ** 2; sy2 += ys[i] ** 2; }
  const num = n * sxy - sx * sy;
  const den = Math.sqrt((n * sx2 - sx * sx) * (n * sy2 - sy * sy));
  return den === 0 ? 0 : num / den;
}
