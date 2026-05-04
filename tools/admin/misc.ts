import { adminFetch } from "./_client";
import type { ToolModule } from "../index";

export const adminAuditLog: ToolModule = {
  name: "admin_audit_log",
  description: "View the WBF admin audit log.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      limit: { type: "integer", description: "Max entries (default 50)" },
    },
    required: [],
  },
  run: async (input: any) => {
    const q = input.limit ? `?limit=${input.limit}` : "";
    return adminFetch("GET", `/admin/audit-log${q}`);
  },
};

export const ADMIN_MISC_TOOLS: ToolModule[] = [adminAuditLog];
