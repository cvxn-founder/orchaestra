import { chQuery } from "./_ch_client";
import { tPValue, fPValue } from "./_stats";
import type { ToolModule } from "../index";

export const name = "ds_ch_local_regression";
export const description = "Simple linear regression with full stats: coefficients, R², F-statistic, p-values on slope/intercept, standard errors. CH computes k,b via simpleLinearRegression; local TS derives full diagnostics.";
export const category = "compute";

export const inputSchema = {
  type: "object",
  properties: {
    table: { type: "string", description: "Table name" },
    columnX: { type: "string", description: "Predictor (independent) column" },
    columnY: { type: "string", description: "Response (dependent) column" },
    database: { type: "string", default: "medtech_raw", description: "Database name" },
    where: { type: "string", description: "Optional WHERE clause" },
  },
  required: ["table", "columnX", "columnY"],
};

export async function run(input: any): Promise<string> {
  const { table, columnX, columnY, database } = input;
  const db = database ?? "medtech_raw";
  const where = input.where ? `WHERE ${input.where}` : "";

  // Get regression coefficients from CH
  const regSql = `SELECT simpleLinearRegression(${columnX}, ${columnY}) AS coef FROM ${db}.${table} ${where}`;
  const regRes = await chQuery(regSql, db);
  const coef = regRes?.data?.[0]?.coef ?? regRes?.[0]?.coef ?? [0, 0];
  const slope = Number(coef[0]), intercept = Number(coef[1]);

  // Get summary stats for R² and diagnostics
  const statsSql = `SELECT count(*) AS n, avg(${columnX}) AS mx, avg(${columnY}) AS my, sum(${columnX}*${columnY}) AS sxy, sum(${columnX}*${columnX}) AS sx2, sum(${columnY}*${columnY}) AS sy2 FROM ${db}.${table} ${where}`;
  const statsRes = await chQuery(statsSql, db);
  const s = statsRes?.data?.[0] ?? statsRes?.[0] ?? {};
  const n = Number(s.n ?? 0), mx = Number(s.mx ?? 0), my = Number(s.my ?? 0);
  const sxy = Number(s.sxy ?? 0), sx2 = Number(s.sx2 ?? 0), sy2 = Number(s.sy2 ?? 0);

  // R² computation
  const ssTot = sy2 - n * my * my;
  const ssReg = slope * slope * (sx2 - n * mx * mx);
  const ssRes = ssTot - ssReg;
  const r2 = ssTot > 0 ? ssReg / ssTot : 0;

  // Standard errors
  const mse = n > 2 ? ssRes / (n - 2) : 0;
  const seSlope = sx2 > 0 ? Math.sqrt(mse / (sx2 - n * mx * mx)) : 0;
  const seInt = Math.sqrt(mse * (1 / n + mx * mx / (sx2 - n * mx * mx)));

  // t-statistics and p-values
  const tSlope = seSlope > 0 ? slope / seSlope : 0;
  const tInt = seInt > 0 ? intercept / seInt : 0;
  const pSlope = tPValue(tSlope, n - 2);
  const pInt = tPValue(tInt, n - 2);

  // F-statistic
  const fStat = r2 < 1 && mse > 0 ? (ssReg / 1) / mse : 0;
  const pF = fPValue(fStat, 1, n - 2);

  return [
    `Simple Linear Regression`,
    `────────────────────────────────`,
    `  Model: ${columnY} = ${slope.toFixed(4)} × ${columnX} + ${intercept.toFixed(4)}`,
    `  n = ${n}`,
    `────────────────────────────────`,
    `  R²        = ${r2.toFixed(4)} (${(r2 * 100).toFixed(1)}% variance explained)`,
    `  F(1,${n - 2}) = ${fStat.toFixed(2)}, p = ${pF.toFixed(6)} ${pF < 0.05 ? "*" : ""}`,
    `────────────────────────────────`,
    `  Slope:`,
    `    coeff = ${slope.toFixed(4)}`,
    `    SE    = ${seSlope.toFixed(4)}`,
    `    t     = ${tSlope.toFixed(2)}`,
    `    p     = ${pSlope.toFixed(6)} ${pSlope < 0.001 ? "***" : pSlope < 0.01 ? "**" : pSlope < 0.05 ? "*" : ""}`,
    `  Intercept:`,
    `    coeff = ${intercept.toFixed(4)}`,
    `    SE    = ${seInt.toFixed(4)}`,
    `    t     = ${tInt.toFixed(2)}`,
    `    p     = ${pInt.toFixed(6)}`,
    `────────────────────────────────`,
    n < 10 ? `  ⚠  Small sample size (n < 10) — results may be unreliable` : "",
  ].join("\n");
}
