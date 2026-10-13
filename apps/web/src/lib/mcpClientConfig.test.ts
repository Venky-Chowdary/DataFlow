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
  mcpLogStatusLabel,
  mcpLogTone,
  remoteMcpSnippet,
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

test("the generic remote snippet carries the absolute URL and a Bearer header", () => {
  const cfg = JSON.parse(remoteMcpSnippet("https://www.datawrap.io/api/v1/mcp"));
  assert.equal(cfg.transport, "streamable-http");
  assert.equal(cfg.url, "https://www.datawrap.io/api/v1/mcp");
  assert.equal(cfg.headers.Authorization, "Bearer <workspace-api-key>");
});

test("log status names the reason a call ended", () => {
  assert.equal(mcpLogStatusLabel("error", "permission_denied"), "Denied");
  assert.equal(mcpLogStatusLabel("error", "rate_limited"), "Rate limited");
  assert.equal(mcpLogStatusLabel("ok", null), "OK");
  assert.equal(mcpLogStatusLabel("error", undefined), "Error");
  assert.equal(mcpLogStatusLabel("error", "something_new"), "Error");
  assert.equal(mcpLogTone("error", "permission_denied"), "warn");
  assert.equal(mcpLogTone("error", "tool_error"), "err");
  assert.equal(mcpLogTone("ok", "ok"), "ok");
});
