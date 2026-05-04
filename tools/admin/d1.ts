import { adminFetch } from "./_client";
import type { ToolModule } from "../index";

export const adminD1Tables: ToolModule = {
  name: "admin_d1_tables",
  description: "List all D1 database tables.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {},
    required: [],
  },
  run: async () => {
    return adminFetch("GET", "/admin/d1/tables");
  },
};

export const adminD1Schema: ToolModule = {
  name: "admin_d1_schema",
  description: "Get the schema (columns, types) for a D1 table.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      table: { type: "string", description: "Table name" },
    },
    required: ["table"],
  },
  run: async (input: any) => {
    return adminFetch("GET", `/admin/d1/tables/${input.table}/schema`);
  },
};

export const adminD1Rows: ToolModule = {
  name: "admin_d1_rows",
  description: "Browse rows in a D1 table with optional ordering.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      table: { type: "string", description: "Table name" },
      limit: { type: "integer", description: "Max rows (default 50)" },
      offset: { type: "integer", description: "Row offset" },
      order_by: { type: "string", description: "Column to order by" },
      direction: { type: "string", enum: ["asc", "desc"], description: "Sort direction" },
    },
    required: ["table"],
  },
  run: async (input: any) => {
    const q = new URLSearchParams();
    if (input.limit) q.set("limit", String(input.limit));
    if (input.offset) q.set("offset", String(input.offset));
    if (input.order_by) q.set("order_by", input.order_by);
    if (input.direction) q.set("direction", input.direction);
    const qs = q.toString();
    return adminFetch("GET", `/admin/d1/tables/${input.table}/rows${qs ? "?" + qs : ""}`);
  },
};

export const adminD1Select: ToolModule = {
  name: "admin_d1_select",
  description: "Run a SELECT query against WBF's D1 database.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      sql: { type: "string", description: "SQL SELECT statement" },
      limit: { type: "integer", description: "Max results (default 50)" },
    },
    required: ["sql"],
  },
  run: async (input: any) => {
    return adminFetch("POST", "/admin/d1/select", { sql: input.sql, limit: input.limit ?? 50 });
  },
};

export const ADMIN_D1_TOOLS: ToolModule[] = [
  adminD1Tables, adminD1Schema, adminD1Rows, adminD1Select,
];
