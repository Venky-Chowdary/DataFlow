/**
 * Run: npx --yes tsx --test apps/web/src/lib/mcpClientConfig.test.ts
 */
import assert from "node:assert/strict";
import test from "node:test";

import {
  absoluteApiRoot,
  claudeMcpSnippet,
  cursorMcpSnippet,
  mcpHttpUrl,
  vscodeMcpSnippet,
} from "./mcpClientConfig.ts";

test("a same-origin API base becomes an absolute MCP URL", () => {
  const url = mcpHttpUrl("/api/v1", "https://www.datawrap.io");
  assert.equal(url, "https://www.datawrap.io/api/v1/mcp");
  assert.equal(absoluteApiRoot("/api/v1", "https://www.datawrap.io"), "https://www.datawrap.io");
});

test("an absolute API base keeps its own host", () => {
  assert.equal(
    mcpHttpUrl("https://tenant.example/api/v1", "https://ignored.example"),
    "https://tenant.example/api/v1/mcp",
  );
});

test("client snippets name the real endpoint and require a bearer key", () => {
  const url = "https://www.datawrap.io/api/v1/mcp";
  for (const snippet of [cursorMcpSnippet(url), claudeMcpSnippet(url), vscodeMcpSnippet(url)]) {
    assert.match(snippet, /https:\/\/www\.datawrap\.io\/api\/v1\/mcp/);
    assert.match(snippet, /Bearer <workspace-api-key>/);
    assert.doesNotMatch(snippet, /api\.datawrap\.io/);
    assert.doesNotMatch(snippet, /mcp-bridge/);
  }
});
