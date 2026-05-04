import { adminFetch } from "./_client";
import type { ToolModule } from "../index";

export const adminR2List: ToolModule = {
  name: "admin_r2_list",
  description: "List R2 objects with optional prefix filter.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      prefix: { type: "string", description: "Prefix filter (e.g. users/)" },
      limit: { type: "integer", description: "Max results (default 25)" },
    },
    required: [],
  },
  run: async (input: any) => {
    const q = new URLSearchParams();
    if (input.prefix) q.set("prefix", input.prefix);
    if (input.limit) q.set("limit", String(input.limit));
    const qs = q.toString();
    return adminFetch("GET", `/admin/r2/objects${qs ? "?" + qs : ""}`);
  },
};

export const adminR2Metadata: ToolModule = {
  name: "admin_r2_metadata",
  description: "Get metadata for an R2 object by key.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      key: { type: "string", description: "R2 object key" },
    },
    required: ["key"],
  },
  run: async (input: any) => {
    return adminFetch("GET", `/admin/r2/objects/${encodeURIComponent(input.key)}/metadata`);
  },
};

export const adminR2Content: ToolModule = {
  name: "admin_r2_content",
  description: "Get the content of an R2 object (base64 or raw).",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      key: { type: "string", description: "R2 object key" },
      format: { type: "string", enum: ["base64", "raw"], description: "Response format" },
    },
    required: ["key"],
  },
  run: async (input: any) => {
    const fmt = input.format === "raw" ? "" : "?format=base64";
    return adminFetch("GET", `/admin/r2/objects/${encodeURIComponent(input.key)}/content${fmt}`);
  },
};

export const ADMIN_R2_TOOLS: ToolModule[] = [
  adminR2List, adminR2Metadata, adminR2Content,
];
