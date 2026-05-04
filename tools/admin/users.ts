import { adminFetch } from "./_client";
import type { ToolModule } from "../index";

export const adminUsersList: ToolModule = {
  name: "admin_users_list",
  description: "List WBF users. Optional: query (search), limit (default 50).",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      query: { type: "string", description: "Search by email or name" },
      limit: { type: "integer", description: "Max results (default 50)" },
    },
    required: [],
  },
  run: async (input: any) => {
    const q = new URLSearchParams();
    if (input.query) q.set("query", input.query);
    if (input.limit) q.set("limit", String(input.limit));
    const qs = q.toString();
    return adminFetch("GET", `/admin/users${qs ? "?" + qs : ""}`);
  },
};

export const adminUserCreate: ToolModule = {
  name: "admin_user_create",
  description: "Create a new WBF user account.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      email: { type: "string", description: "User email" },
      password: { type: "string", description: "Password" },
      role: { type: "string", enum: ["user", "admin"], description: "Role (default: user)" },
      display_name: { type: "string", description: "Display name" },
    },
    required: ["email", "password"],
  },
  run: async (input: any) => {
    return adminFetch("POST", "/admin/users", {
      email: input.email,
      password: input.password,
      role: input.role ?? "user",
      display_name: input.display_name,
    });
  },
};

export const adminUserUpdate: ToolModule = {
  name: "admin_user_update",
  description: "Update a WBF user's details.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      id: { type: "string", description: "User ID" },
      email: { type: "string" },
      role: { type: "string", enum: ["user", "admin"] },
      display_name: { type: "string" },
    },
    required: ["id"],
  },
  run: async (input: any) => {
    const { id, ...fields } = input;
    return adminFetch("PATCH", `/admin/users/${id}`, fields);
  },
};

export const adminUserDisable: ToolModule = {
  name: "admin_user_disable",
  description: "Disable a WBF user account.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      id: { type: "string", description: "User ID" },
    },
    required: ["id"],
  },
  run: async (input: any) => {
    return adminFetch("POST", `/admin/users/${input.id}/disable`);
  },
};

export const adminUserDeletePlan: ToolModule = {
  name: "admin_user_delete_plan",
  description: "Preview what will be deleted for a user before hard-deleting.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      id: { type: "string", description: "User ID" },
    },
    required: ["id"],
  },
  run: async (input: any) => {
    return adminFetch("POST", `/admin/users/${input.id}/delete-plan`);
  },
};

export const adminUserHardDelete: ToolModule = {
  name: "admin_user_hard_delete",
  description: "Permanently delete a WBF user. Requires email confirmation.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      id: { type: "string", description: "User ID" },
      confirm: { type: "string", description: "User email or ID to confirm" },
    },
    required: ["id", "confirm"],
  },
  run: async (input: any) => {
    return adminFetch("POST", `/admin/users/${input.id}/hard-delete`, { confirm: input.confirm });
  },
};

export const adminUserFiles: ToolModule = {
  name: "admin_user_files",
  description: "List files owned by a WBF user.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      id: { type: "string", description: "User ID" },
    },
    required: ["id"],
  },
  run: async (input: any) => {
    return adminFetch("GET", `/admin/users/${input.id}/files`);
  },
};

export const adminUserFilesDeletePlan: ToolModule = {
  name: "admin_user_files_delete_plan",
  description: "Preview file deletion plan for a user.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      id: { type: "string", description: "User ID" },
    },
    required: ["id"],
  },
  run: async (input: any) => {
    return adminFetch("POST", `/admin/users/${input.id}/files/delete-plan`);
  },
};

export const adminUserFilesHardDelete: ToolModule = {
  name: "admin_user_files_hard_delete",
  description: "Permanently delete a user's files. Requires user ID confirmation.",
  category: "admin",
  inputSchema: {
    type: "object",
    properties: {
      id: { type: "string", description: "User ID" },
      confirm: { type: "string", description: "User ID to confirm" },
    },
    required: ["id", "confirm"],
  },
  run: async (input: any) => {
    return adminFetch("POST", `/admin/users/${input.id}/files/hard-delete`, { confirm: input.confirm });
  },
};

export const ADMIN_USER_TOOLS: ToolModule[] = [
  adminUsersList, adminUserCreate, adminUserUpdate, adminUserDisable,
  adminUserDeletePlan, adminUserHardDelete, adminUserFiles,
  adminUserFilesDeletePlan, adminUserFilesHardDelete,
];
