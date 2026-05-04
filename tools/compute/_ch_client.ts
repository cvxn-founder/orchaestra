// Shared ClickHouse query helper for data science tools
import { adminFetch } from "../admin/_client";

export async function chQuery(sql: string, database: string = "medtech_raw", limit: number = 200): Promise<any> {
  const raw = await adminFetch("POST", "/admin/clickhouse/query", {
    database,
    sql,
    limit,
  });
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

export function buildChDescribeSQL(table: string, column: string, database: string = "medtech_raw", where?: string): string {
  const filter = where ? `WHERE ${where}` : "";
  return `SELECT count(${column}) AS n, avg(${column}) AS mean, median(${column}) AS median, quantile(0.25)(${column}) AS q1, quantile(0.75)(${column}) AS q3, stddevSamp(${column}) AS std, varSamp(${column}) AS var, skewSamp(${column}) AS skew, kurtSamp(${column}) AS kurt, min(${column}) AS min, max(${column}) AS max FROM ${database}.${table} ${filter}`;
}

export function buildChCorrSQL(xTable: string, xCol: string, yTable: string, yCol: string, joinOn: string, database: string = "medtech_raw"): string {
  return `SELECT corrStable(x.${xCol}, y.${yCol}) AS r, count(*) AS n FROM ${database}.${xTable} AS x INNER JOIN ${database}.${yTable} AS y ON ${joinOn}`;
}

export function buildChGroupStatsSQL(table: string, valueCol: string, groupCol: string, database: string = "medtech_raw", where?: string): string {
  const filter = where ? `WHERE ${where}` : "";
  return `SELECT ${groupCol}, count(${valueCol}) AS n, avg(${valueCol}) AS mean, stddevSamp(${valueCol}) AS std FROM ${database}.${table} ${filter} GROUP BY ${groupCol} ORDER BY ${groupCol}`;
}

export function buildChTTestSQL(table: string, valueCol: string, groupCol: string, groupA: string, groupB: string, database: string = "medtech_raw"): string {
  return `SELECT studentTTest(group_a, group_b) AS result FROM (SELECT groupArrayIf(${valueCol}, ${groupCol} = '${groupA}') AS group_a, groupArrayIf(${valueCol}, ${groupCol} = '${groupB}') AS group_b FROM ${database}.${table})`;
}
