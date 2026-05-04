import { chQuery } from "./_ch_client";
import { tPValue, cohensD, interpretD } from "./_stats";
import type { ToolModule } from "../index";

export const name = "ds_ch_event_study";
export const description = "Pre/post event analysis. Aggregates a metric in configurable windows before and after event dates, then runs t-test on pre vs post. Runs in ClickHouse with local stats.";
export const category = "compute";

export const inputSchema = {
  type: "object",
  properties: {
    eventsTable: { type: "string", description: "Table with event dates" },
    eventsDateCol: { type: "string", description: "Date column in events table" },
    eventsIdCol: { type: "string", description: "ID column in events table" },
    metricTable: { type: "string", description: "Table with the metric to measure" },
    metricDateCol: { type: "string", description: "Date column in metric table" },
    metricValueCol: { type: "string", description: "Numeric value column to aggregate" },
    joinCondition: { type: "string", description: "JOIN condition between tables (e.g., x.device_id = y.device_id)" },
    windows: { type: "array", items: { type: "integer" }, description: "Window sizes in days (e.g., [30, 60, 90])" },
    database: { type: "string", default: "medtech_raw", description: "Database name" },
  },
  required: ["eventsTable", "eventsDateCol", "eventsIdCol", "metricTable", "metricDateCol", "metricValueCol", "joinCondition"],
};

export async function run(input: any): Promise<string> {
  const { eventsTable, eventsDateCol, eventsIdCol, metricTable, metricDateCol, metricValueCol, joinCondition, database } = input;
  const db = database ?? "medtech_raw";
  const windows = input.windows ?? [30, 60, 90];

  const lines = [`Event Study Analysis`, `────────────────────────────────`, `  Events: ${db}.${eventsTable}.${eventsDateCol}`, `  Metric: ${db}.${metricTable}.${metricValueCol}`, `────────────────────────────────`];

  for (const w of windows) {
    const sql = `SELECT avgIf(${metricValueCol}, dateDiff('day', e.${eventsDateCol}, m.${metricDateCol}) BETWEEN -${w} AND -1) AS pre_mean, avgIf(${metricValueCol}, dateDiff('day', e.${eventsDateCol}, m.${metricDateCol}) BETWEEN 0 AND ${w}) AS post_mean, stddevSampIf(${metricValueCol}, dateDiff('day', e.${eventsDateCol}, m.${metricDateCol}) BETWEEN -${w} AND -1) AS pre_std, stddevSampIf(${metricValueCol}, dateDiff('day', e.${eventsDateCol}, m.${metricDateCol}) BETWEEN 0 AND ${w}) AS post_std, countIf(${metricValueCol}, dateDiff('day', e.${eventsDateCol}, m.${metricDateCol}) BETWEEN -${w} AND -1) AS pre_n, countIf(${metricValueCol}, dateDiff('day', e.${eventsDateCol}, m.${metricDateCol}) BETWEEN 0 AND ${w}) AS post_n FROM ${db}.${eventsTable} AS e LEFT JOIN ${db}.${metricTable} AS m ON ${joinCondition}`;
    const res = await chQuery(sql, db);
    const r = res?.data?.[0] ?? res?.[0] ?? {};

    const preMean = Number(r.pre_mean ?? 0), postMean = Number(r.post_mean ?? 0);
    const preStd = Number(r.pre_std ?? 0), postStd = Number(r.post_std ?? 0);
    const preN = Number(r.pre_n ?? 0), postN = Number(r.post_n ?? 0);
    const delta = postMean - preMean;
    const d = cohensD(postMean, preMean, postStd, preStd, postN, preN);

    // Welch's t-test approximation
    const se = Math.sqrt(preStd ** 2 / preN + postStd ** 2 / postN);
    const t = se > 0 ? delta / se : 0;
    const df = Math.floor((preStd ** 2 / preN + postStd ** 2 / postN) ** 2 / ((preStd ** 2 / preN) ** 2 / (preN - 1) + (postStd ** 2 / postN) ** 2 / (postN - 1)));
    const p = tPValue(t, Math.max(1, df));

    lines.push(`  Window: ±${w} days`);
    lines.push(`    Pre  (n=${preN}):  mean=${preMean.toFixed(4)}, std=${preStd.toFixed(4)}`);
    lines.push(`    Post (n=${postN}):  mean=${postMean.toFixed(4)}, std=${postStd.toFixed(4)}`);
    lines.push(`    Δ = ${delta.toFixed(4)}, d = ${d.toFixed(3)} (${interpretD(d)}), p = ${p.toFixed(4)} ${p < 0.05 ? "*" : ""}`);
  }
  return lines.join("\n");
}
