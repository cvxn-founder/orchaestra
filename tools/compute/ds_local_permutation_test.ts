import { chQuery } from "./_ch_client";
import type { ToolModule } from "../index";

export const name = "ds_local_permutation_test";
export const description = "Permutation test for difference in means. CH provides group data; local TS randomly shuffles group labels N times and computes empirical p-value. Non-parametric, no distribution assumptions.";
export const category = "compute";

export const inputSchema = {
  type: "object",
  properties: {
    table: { type: "string", description: "Table name" },
    valueColumn: { type: "string", description: "Numeric column" },
    groupColumn: { type: "string", description: "Group column" },
    groupA: { type: "string", description: "Group A value" },
    groupB: { type: "string", description: "Group B value" },
    permutations: { type: "integer", default: 5000, description: "Number of permutations (1000-50000)" },
    database: { type: "string", default: "medtech_raw", description: "Database name" },
  },
  required: ["table", "valueColumn", "groupColumn", "groupA", "groupB"],
};

export async function run(input: any): Promise<string> {
  const { table, valueColumn, groupColumn, groupA, groupB, database } = input;
  const db = database ?? "medtech_raw";
  const perms = Math.min(input.permutations ?? 5000, 50000);

  const sql = `SELECT ${groupColumn}, ${valueColumn} FROM ${db}.${table} WHERE ${groupColumn} IN ('${groupA}', '${groupB}')`;
  const res = await chQuery(sql, db);
  const data = res?.data ?? res ?? [];
  if (!Array.isArray(data) || data.length < 6) return "Need at least 6 observations.";

  const values: number[] = [];
  const labels: number[] = []; // 0 = A, 1 = B
  let aCount = 0, bCount = 0;
  for (const r of data) {
    const isA = String(r[groupColumn]) === groupA;
    values.push(Number(r[valueColumn] ?? 0));
    labels.push(isA ? 0 : 1);
    if (isA) aCount++; else bCount++;
  }
  if (aCount < 3 || bCount < 3) return `Need at least 3 observations per group. Got A=${aCount}, B=${bCount}.`;

  // Observed difference
  const obsDiff = meanDiff(values, labels);

  // Permutation test
  let extreme = 0;
  const permLabels = [...labels];
  for (let p = 0; p < perms; p++) {
    // Fisher-Yates shuffle
    for (let i = permLabels.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [permLabels[i], permLabels[j]] = [permLabels[j], permLabels[i]];
    }
    const pd = meanDiff(values, permLabels);
    if (Math.abs(pd) >= Math.abs(obsDiff)) extreme++;
  }
  const p = extreme / perms;

  return [
    `Permutation Test`,
    `────────────────────────────────`,
    `  ${groupA} (n=${aCount}) vs ${groupB} (n=${bCount})`,
    `  Column: ${db}.${table}.${valueColumn}`,
    `────────────────────────────────`,
    `  Observed difference = ${obsDiff.toFixed(4)}`,
    `  Permutations        = ${perms}`,
    `  More extreme       = ${extreme}`,
    `  Empirical p-value   = ${p.toFixed(6)} ${p < 0.001 ? "***" : p < 0.01 ? "**" : p < 0.05 ? "*" : "ns"}`,
    `────────────────────────────────`,
    `  ${p < 0.05 ? "Significant — group difference unlikely by chance" : "Not significant — difference could be random"}`,
    `  No distribution assumptions required.`,
  ].join("\n");
}

function meanDiff(vals: number[], labs: number[]): number {
  let sa = 0, sb = 0, ca = 0, cb = 0;
  for (let i = 0; i < vals.length; i++) {
    if (labs[i] === 0) { sa += vals[i]; ca++; }
    else { sb += vals[i]; cb++; }
  }
  return (sa / ca) - (sb / cb);
}
