/**
 * Parse operator-pasted Snowflake URLs the same way the engine does.
 * snowflake.connector.connect is keyword-only — never send a raw URL.
 */

export interface SnowflakeUrlParts {
  account?: string;
  user?: string;
  password?: string;
  database?: string;
  schema?: string;
  warehouse?: string;
  role?: string;
}

export const SNOWFLAKE_HOST_ONLY_URL_MSG =
  "That is a Snowflake account host, not a login. Use snowflake://user:password@account/DATABASE/SCHEMA?warehouse=COMPUTE_WH or switch to Username & password. If the password contains @, encode it as %40.";

const HOST_SUFFIXES = [".privatelink.snowflakecomputing.com", ".snowflakecomputing.com"];

export function normalizeSnowflakeAccount(host: string): string {
  let raw = (host || "").trim();
  if (!raw) return "";
  raw = raw.replace(/^https?:\/\//i, "");
  raw = raw.split("/")[0].split("?")[0];
  if (raw.includes("@")) raw = raw.slice(raw.lastIndexOf("@") + 1);
  if ((raw.match(/:/g) || []).length === 1 && !raw.startsWith("[")) {
    const [hostPart, port] = raw.split(":");
    if (/^\d+$/.test(port)) raw = hostPart;
  }
  raw = raw.replace(/\.+$/, "").trim();
  const lower = raw.toLowerCase();
  for (const suffix of HOST_SUFFIXES) {
    if (lower.endsWith(suffix)) return raw.slice(0, -suffix.length);
  }
  return raw;
}

function q(params: URLSearchParams, ...names: string[]): string {
  for (const name of names) {
    const value = params.get(name) || params.get(name.toLowerCase()) || params.get(name.toUpperCase());
    if (value && value.trim()) {
      try {
        return decodeURIComponent(value);
      } catch {
        return value;
      }
    }
  }
  return "";
}

function decodePart(value: string): string {
  if (!value) return "";
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

export function parseSnowflakeUrl(raw: string): SnowflakeUrlParts {
  let text = (raw || "").trim();
  if (!text) return {};
  if (text.toLowerCase().startsWith("jdbc:")) text = text.slice(5).trim();

  let query = "";
  const qIdx = text.indexOf("?");
  if (qIdx >= 0) {
    query = text.slice(qIdx + 1);
    text = text.slice(0, qIdx);
  }

  const rest = text.includes("://") ? text.split("://").slice(1).join("://") : text;

  let userinfo = "";
  let afterAt = rest;
  const at = rest.lastIndexOf("@");
  if (at >= 0) {
    userinfo = rest.slice(0, at);
    afterAt = rest.slice(at + 1);
  }

  const pathParts = afterAt.split("/").filter(Boolean);
  const accountRaw = decodePart(pathParts[0] || "");
  let database = decodePart(pathParts[1] || "");
  let schema = decodePart(pathParts[2] || "");

  let user = "";
  let password = "";
  if (userinfo) {
    const colon = userinfo.indexOf(":");
    if (colon >= 0) {
      user = decodePart(userinfo.slice(0, colon));
      password = decodePart(userinfo.slice(colon + 1));
    } else {
      user = decodePart(userinfo);
    }
  }

  const params = new URLSearchParams(query);
  const account = normalizeSnowflakeAccount(accountRaw || q(params, "account"));
  user = user || q(params, "user", "username");
  password = password || q(params, "password", "passwd", "pwd");
  database = database || q(params, "db", "database");
  schema = schema || q(params, "schema");
  const warehouse = q(params, "warehouse", "wh");
  const role = q(params, "role");

  const out: SnowflakeUrlParts = {};
  if (account) out.account = account;
  if (user) out.user = user;
  if (password) out.password = password;
  if (database) out.database = database;
  if (schema) out.schema = schema;
  if (warehouse) out.warehouse = warehouse;
  if (role) out.role = role;
  return out;
}

export function isSnowflakeAccountHostOnly(parsed: SnowflakeUrlParts): boolean {
  return Boolean(parsed.account && !parsed.user && !parsed.password);
}

export function validateSnowflakeConnectionString(raw: string): string | null {
  const text = (raw || "").trim();
  if (!text) return "Connection string is required.";
  const parsed = parseSnowflakeUrl(text);
  if (isSnowflakeAccountHostOnly(parsed)) return SNOWFLAKE_HOST_ONLY_URL_MSG;
  if (!parsed.account) {
    return "Could not read a Snowflake account from this URL. Expected snowflake://user:password@account/DATABASE/SCHEMA?warehouse=COMPUTE_WH.";
  }
  if (!parsed.user || !parsed.password) {
    return "Connection string must include username and password (snowflake://user:password@account/...).";
  }
  return null;
}
