import { chQuery } from "./_ch_client";
import { cohensD, interpretD } from "./_stats";
import type { ToolModule } from "../index";

export const name = "ds_local_effect_size";
export const description = "Effect size calculator: Cohen's d, Hedges' g, Glass's Δ with interpretation thresholds. CH provides group stats; local TS computes effect sizes.";
export const category = "compute";

export const inputSchema = {
  type: "object",
  properties: {
    table: { type: "string", description: "Table name" },
    valueColumn: { type: "string", description: "Numeric column" },
    groupColumn: { type: "string", description: "Group column" },
    groupA: { type: "string", description: "Group A value" },
    groupB: { type: "string", description: "Group B value" },
    database: { type: "string", default: "medtech_raw", description: "Database name" },
  },
  required: ["table", "valueColumn", "groupColumn", "groupA", "groupB"],
};

export async function run(input: any): Promise<string> {
  const { table, valueColumn, groupColumn, groupA, groupB, database } = input;
  const db = database ?? "medtech_raw";

  const sql = `SELECT ${groupColumn}, count(*) AS n, avg(${valueColumn}) AS mean, stddevSamp(${valueColumn}) AS std FROM ${db}.${table} WHERE ${groupColumn} IN ('${groupA}', '${groupB}') GROUP BY ${groupColumn}`;
  const res = await chQuery(sql, db);
  const data = res?.data ?? res ?? [];

  let aN = 0, aMean = 0, aStd = 0, bN = 0, bMean = 0, bStd = 0;
  for (const r of data) {
    if (String(r[groupColumn]) === groupA) { aN = Number(r.n); aMean = Number(r.mean); aStd = Number(r.std); }
    else { bN = Number(r.n); bMean = Number(r.mean); bStd = Number(r.std); }
  }
  if (aN < 2 || bN < 2) return "Need at least 2 observations per group.";

  const d = cohensD(aMean, bMean, aStd, bStd, aN, bN);
  // Hedges' g correction
  const g = d * (1 - 3 / (4 * (aN + bN) - 9));
  // Glass's Δ
  const glassDelta = (aMean - bMean) / aStd;

  return [
    `Effect Size Analysis`,
    `────────────────────────────────`,
    `  ${groupA} (n=${aN}): mean=${aMean.toFixed(4)}, std=${aStd.toFixed(4)}`,
    `  ${groupB} (n=${bN}): mean=${bMean.toFixed(4)}, std=${bStd.toFixed(4)}`,
    `────────────────────────────────`,
    `  Cohen's d   = ${d.toFixed(4)}  (${interpretD(d)})`,
    `  Hedges' g   = ${g.toFixed(4)}  (bias-corrected)`,
    `  Glass's Δ   = ${glassDelta.toFixed(4)}  (standardized by ${groupA} SD)`,
    `────────────────────────────────`,
    `  Thresholds: |d| < 0.2 = negligible, < 0.5 = small, < 0.8 = medium, ≥ 0.8 = large`,
  ].join("\n");
}
