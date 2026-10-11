/**
 * Absolute MCP client config.
 *
 * Cursor and Claude dial a host. A snippet that says `/api/v1/mcp` or
 * `https://api.datawrap.io` does not reach this workspace. The URL is the
 * origin the operator is signed into, plus `/api/v1/mcp`, and the workspace
 * API key travels in the Authorization header.
 */

export function absoluteApiRoot(apiBase: string, origin: string): string {
  const trimmed = String(apiBase || "").trim().replace(/\/+$/, "");
  const absolute = /^https?:\/\//i.test(trimmed)
    ? trimmed
    : `${String(origin || "").replace(/\/+$/, "")}${trimmed.startsWith("/") ? trimmed : `/${trimmed}`}`;
  return absolute.replace(/\/api\/v1$/i, "");
}

export function mcpHttpUrl(apiBase: string, origin: string): string {
  return `${absoluteApiRoot(apiBase, origin)}/api/v1/mcp`;
}

export function cursorMcpSnippet(url: string): string {
  return JSON.stringify(
    {
      mcpServers: {
        dataflow: {
          url,
          headers: { Authorization: "Bearer <workspace-api-key>" },
        },
      },
    },
    null,
    2,
  );
}

export function claudeMcpSnippet(url: string): string {
  return JSON.stringify(
    {
      mcpServers: {
        dataflow: {
          type: "http",
          url,
          headers: { Authorization: "Bearer <workspace-api-key>" },
        },
      },
    },
    null,
    2,
  );
}

export function vscodeMcpSnippet(url: string): string {
  return JSON.stringify(
    {
      servers: {
        dataflow: {
          type: "http",
          url,
          headers: { Authorization: "Bearer <workspace-api-key>" },
        },
      },
    },
    null,
    2,
  );
}

export function customGptMcpSnippet(url: string): string {
  return `POST ${url}/tools/call\nAuthorization: Bearer <workspace-api-key>`;
}

/** Any client that speaks MCP Streamable HTTP (Grok, agent frameworks, custom code). */
export function remoteMcpSnippet(url: string): string {
  return JSON.stringify(
    {
      transport: "streamable-http",
      url,
      headers: { Authorization: "Bearer <workspace-api-key>" },
    },
    null,
    2,
  );
}

export type McpLogKind =
  | "ok"
  | "permission_denied"
  | "rate_limited"
  | "auth"
  | "policy_denied"
  | "tool_error";

const LOG_KIND_LABELS: Record<McpLogKind, string> = {
  ok: "OK",
  permission_denied: "Denied",
  rate_limited: "Rate limited",
  auth: "Not signed in",
  policy_denied: "Blocked by policy",
  tool_error: "Tool error",
};

/** Why a call ended, from the invocation log. Older rows have no kind. */
export function mcpLogStatusLabel(status: string, errorKind?: string | null): string {
  if (errorKind && errorKind in LOG_KIND_LABELS) return LOG_KIND_LABELS[errorKind as McpLogKind];
  return status === "ok" ? "OK" : "Error";
}

/** Denials, rate limits and policy blocks are the caller's to fix, not tool failures. */
export function mcpLogTone(status: string, errorKind?: string | null): "ok" | "warn" | "err" {
  if (status === "ok" || errorKind === "ok") return "ok";
  if (errorKind === "permission_denied" || errorKind === "rate_limited" || errorKind === "auth" || errorKind === "policy_denied") {
    return "warn";
  }
  return "err";
}
