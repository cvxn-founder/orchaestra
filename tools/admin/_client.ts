import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { homedir } from "os";

const BASE_URL = "https://admin-api.webuildfast.ai";

interface AdminConfig {
  admin_tui_key?: string;
  cf_client_id?: string;
  cf_client_secret?: string;
}

let _cached: AdminConfig | null = null;

function loadAdminConfig(): AdminConfig {
  if (_cached) return _cached;
  const paths = [
    join(homedir(), ".orchaestra", "models.toml"),
    join(process.cwd(), "user", "models.toml"),
  ];
  for (const p of paths) {
    try {
      if (!existsSync(p)) continue;
      const raw = readFileSync(p, "utf8");
      // Simple TOML section parser for [admin]
      const adminMatch = raw.match(/\[admin\]([\s\S]*?)(?=\[|$)/);
      if (!adminMatch) continue;
      const section = adminMatch[1];
      const get = (key: string) => {
        const m = section.match(new RegExp(`${key}\\s*=\\s*"([^"]*)"`));
        return m ? m[1] : undefined;
      };
      _cached = {
        admin_tui_key: get("admin_tui_key"),
        cf_client_id: get("cf_client_id"),
        cf_client_secret: get("cf_client_secret"),
      };
      break;
    } catch {}
  }
  return _cached ?? {};
}

export async function adminFetch(
  method: string,
  path: string,
  body?: Record<string, unknown>,
): Promise<string> {
  const cfg = loadAdminConfig();
  const headers: Record<string, string> = { "Content-Type": "application/json" };

  if (cfg.cf_client_id) headers["CF-Access-Client-Id"] = cfg.cf_client_id;
  if (cfg.cf_client_secret) headers["CF-Access-Client-Secret"] = cfg.cf_client_secret;
  if (cfg.admin_tui_key) headers["Authorization"] = `Bearer ${cfg.admin_tui_key}`;

  const url = `${BASE_URL}${path}`;
  const init: RequestInit = { method, headers };
  if (body && method !== "GET") init.body = JSON.stringify(body);

  let resp: Response;
  try {
    resp = await fetch(url, init);
  } catch (e: any) {
    return `Admin API error: ${e.message}`;
  }

  const text = await resp.text();
  if (!resp.ok) {
    try {
      const j = JSON.parse(text);
      let msg = j.error ?? text.slice(0, 200);
      // Add hints for common ClickHouse errors
      if (typeof msg === "string") {
        if (msg.includes("Illegal type") && msg.includes("Array")) {
          msg += "\nHINT: You searched an Array column with ILIKE. Use admin_ch_schema first to find string columns, then retry with only string columns in searchColumns.";
        } else if (msg.includes("column") && msg.includes("not found")) {
          msg += "\nHINT: Column not found. Use admin_ch_schema to list available columns.";
        }
      }
      return `Admin ${resp.status}: ${msg}`;
    } catch {
      return `Admin ${resp.status}: ${text.slice(0, 200)}`;
    }
  }

  // Pretty-print JSON responses
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return text;
  }
}
