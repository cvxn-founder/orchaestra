import { adminFetch } from "./_client";
import type { ToolModule } from "../index";

export const adminChSchema: ToolModule = {
  name: "clickhouse_schema",
  description: "FIRST STEP before querying: list all tables in a ClickHouse database, or describe a specific table's columns and types. Always use this before search/query to check which columns are searchable strings vs arrays.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      database: { type: "string", default: "medtech_raw", description: "Database name" },
      table: { type: "string", description: "Optional: table to describe columns for" },
    },
    required: [],
  },
  run: async (input: any) => {
    return adminFetch("POST", "/admin/clickhouse/schema", input);
  },
};

export const adminChSearch: ToolModule = {
  name: "clickhouse_search",
  description: "Search a ClickHouse table with ILIKE on TEXT columns only. IMPORTANT: only search STRING columns, not Array columns — check schema first with clickhouse_schema. Returns up to 50 rows.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      database: { type: "string", default: "medtech_raw", description: "Database name" },
      table: { type: "string", description: "Table name" },
      searchColumns: { type: "array", items: { type: "string" }, description: "STRING columns to search with ILIKE (not array columns!)" },
      query: { type: "string", description: "Search query text" },
      synonyms: { type: "array", items: { type: "string" }, description: "Synonym terms" },
      selectColumns: { type: "array", items: { type: "string" }, description: "Columns to return" },
      where: { type: "string", description: "Additional WHERE clause" },
      orderBy: { type: "string", description: "ORDER BY clause" },
      limit: { type: "integer", default: 50, description: "Max results" },
    },
    required: ["table", "searchColumns"],
  },
  run: async (input: any) => {
    return adminFetch("POST", "/admin/clickhouse/search", input);
  },
};

export const adminChPreview: ToolModule = {
  name: "clickhouse_preview",
  description: "Preview the most recent rows from a ClickHouse table. Use after checking schema. Returns up to 50 rows.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      database: { type: "string", default: "medtech_raw", description: "Database name" },
      table: { type: "string", description: "Table name" },
      columns: { type: "array", items: { type: "string" }, description: "Columns to return" },
      orderBy: { type: "string", description: "ORDER BY clause" },
      limit: { type: "integer", default: 50, description: "Max rows" },
      offset: { type: "integer", default: 0, description: "Row offset" },
    },
    required: ["table"],
  },
  run: async (input: any) => {
    return adminFetch("POST", "/admin/clickhouse/preview", input);
  },
};

export const adminChRelations: ToolModule = {
  name: "clickhouse_relations",
  description: "Find possible join keys across ClickHouse tables by matching column names. Use to discover cross-table relationships.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      database: { type: "string", default: "medtech_raw", description: "Database name" },
      tables: { type: "array", items: { type: "string" }, description: "Tables to compare" },
      limit: { type: "integer", default: 200, description: "Max matches" },
    },
    required: ["tables"],
  },
  run: async (input: any) => {
    return adminFetch("POST", "/admin/clickhouse/relations", input);
  },
};

export const adminChAggregate: ToolModule = {
  name: "clickhouse_aggregate",
  description: "Run GROUP BY aggregations (COUNT, SUM, AVG) on a ClickHouse table. Use for stats, counts, distributions. Returns up to 200 groups.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      database: { type: "string", default: "medtech_raw", description: "Database name" },
      table: { type: "string", description: "Table name" },
      groupBy: { type: "array", items: { type: "string" }, description: "Group-by columns or expressions" },
      aggregations: { type: "array", items: { type: "object" }, description: "Array of { fn, alias } objects" },
      where: { type: "string", description: "WHERE clause" },
      orderBy: { type: "string", description: "ORDER BY clause" },
      limit: { type: "integer", default: 200, description: "Max groups" },
    },
    required: ["table", "groupBy", "aggregations"],
  },
  run: async (input: any) => {
    return adminFetch("POST", "/admin/clickhouse/aggregate", input);
  },
};

export const adminChQuery: ToolModule = {
  name: "clickhouse_query",
  description: "Parameterized ClickHouse query with safe {param:String} placeholders. Use when search/preview aren't flexible enough. Returns up to 200 rows. Always check schema first to know column names and types.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      database: { type: "string", default: "medtech_raw", description: "Database name" },
      table: { type: "string", description: "Table name" },
      selectColumns: { type: "array", items: { type: "string" }, description: "Columns to return" },
      where: { type: "string", description: "WHERE clause with {param:String} placeholders" },
      params: { type: "object", description: "Param key → value map" },
      orderBy: { type: "string", description: "ORDER BY clause" },
      limit: { type: "integer", default: 200, description: "Max rows" },
      offset: { type: "integer", default: 0, description: "Row offset" },
    },
    required: ["table"],
  },
  run: async (input: any) => {
    return adminFetch("POST", "/admin/clickhouse/query", input);
  },
};

export const ADMIN_CH_TOOLS: ToolModule[] = [
  adminChSchema, adminChSearch, adminChPreview, adminChRelations, adminChAggregate, adminChQuery,
];
