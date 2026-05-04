import { adminFetch } from "./_client";
import type { ToolModule } from "../index";

export const adminR2Upload: ToolModule = {
  name: "admin_r2_upload",
  description: "Upload a file to R2 storage via multipart form. Key is the R2 object path.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      key: { type: "string", description: "R2 object key (e.g., raw/data.csv)" },
      content: { type: "string", description: "File content as text" },
    },
    required: ["key", "content"],
  },
  run: async (input: any) => {
    return adminFetch("POST", `/admin/r2/upload?key=${encodeURIComponent(input.key)}`, { content: input.content });
  },
};

export const adminR2Put: ToolModule = {
  name: "admin_r2_put",
  description: "Write raw content to an R2 object at the given key.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      key: { type: "string", description: "R2 object key" },
      content: { type: "string", description: "Content to write" },
    },
    required: ["key", "content"],
  },
  run: async (input: any) => {
    return adminFetch("PUT", `/admin/r2/objects/${encodeURIComponent(input.key)}/content`, { content: input.content });
  },
};

export const adminR2Delete: ToolModule = {
  name: "admin_r2_delete",
  description: "Delete an R2 object. Requires key confirmation.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      key: { type: "string", description: "R2 object key to delete" },
      confirm: { type: "string", description: "Must match the key to confirm deletion" },
    },
    required: ["key", "confirm"],
  },
  run: async (input: any) => {
    return adminFetch("DELETE", `/admin/r2/objects/${encodeURIComponent(input.key)}`, { confirm: input.confirm });
  },
};

export const ADMIN_R2_WRITE_TOOLS: ToolModule[] = [adminR2Upload, adminR2Put, adminR2Delete];
