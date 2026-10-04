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
