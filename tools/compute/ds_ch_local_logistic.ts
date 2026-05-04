import { chQuery } from "./_ch_client";
import { chiSqPValue } from "./_stats";
import type { ToolModule } from "../index";

export const name = "ds_ch_local_logistic";
export const description = "Logistic regression via SGD. ClickHouse runs stochasticLogisticRegression for coefficients; local TS computes odds ratios, pseudo-R², and significance. For binary outcomes.";
export const category = "compute";

export const inputSchema = {
  type: "object",
  properties: {
    table: { type: "string", description: "Table name" },
    columnX: { type: "string", description: "Predictor column (numeric)" },
    columnY: { type: "string", description: "Binary response column (0/1)" },
    database: { type: "string", default: "medtech_raw", description: "Database name" },
  },
  required: ["table", "columnX", "columnY"],
};

export async function run(input: any): Promise<string> {
  const { table, columnX, columnY, database } = input;
  const db = database ?? "medtech_raw";

  const sql = `SELECT stochasticLogisticRegression(${columnX}, ${columnY}) AS coef FROM ${db}.${table}`;
  const res = await chQuery(sql, db);
  const coef = res?.data?.[0]?.coef ?? res?.[0]?.coef ?? [0, 0];
  const slope = Number(coef[0]), intercept = Number(coef[1]);
  const oddsRatio = Math.exp(slope);

  // Pseudo R² via likelihood ratio against null model
  const nullSql = `SELECT count(*) AS n, sum(${columnY}) AS s1 FROM ${db}.${table}`;
  const nullRes = await chQuery(nullSql, db);
  const n = Number(nullRes?.data?.[0]?.n ?? nullRes?.[0]?.n ?? 0);
  const s1 = Number(nullRes?.data?.[0]?.s1 ?? nullRes?.[0]?.s1 ?? 0);
  const pBar = s1 / n;
  const nullLL = s1 * Math.log(pBar) + (n - s1) * Math.log(1 - pBar);

  // Get log-likelihood from predictions
  const llSql = `SELECT sum(${columnY} * log(px) + (1 - ${columnY}) * log(1 - px)) AS ll FROM (SELECT ${columnY}, 1 / (1 + exp(-(${slope} * ${columnX} + ${intercept}))) AS px FROM ${db}.${table})`;
  const llRes = await chQuery(llSql, db);
  const modelLL = Number(llRes?.data?.[0]?.ll ?? llRes?.[0]?.ll ?? 0);

  const pseudoR2 = 1 - modelLL / nullLL;
  const lrChi = 2 * (modelLL - nullLL);
  const lrP = chiSqPValue(lrChi, 1);

  return [
    `Logistic Regression (SGD)`,
    `────────────────────────────────`,
    `  Model: logit(p) = ${slope.toFixed(4)} × ${columnX} + ${intercept.toFixed(4)}`,
    `  n = ${n}`,
    `────────────────────────────────`,
    `  Coefficient (slope) = ${slope.toFixed(4)}`,
    `  Odds Ratio           = ${oddsRatio.toFixed(4)}`,
    `    (1-unit increase in ${columnX} multiplies odds by ${oddsRatio.toFixed(2)})`,
    `  Intercept            = ${intercept.toFixed(4)}`,
    `────────────────────────────────`,
    `  Pseudo R² (McFadden) = ${pseudoR2.toFixed(4)}`,
    `  LR χ²(1) = ${lrChi.toFixed(2)}, p = ${lrP.toFixed(6)} ${lrP < 0.05 ? "*" : ""}`,
  ].join("\n");
}
