/**
 * QA (557f556c): the MCP page's config snippet registered the server under
 * the key "dataflow". Every client snippet names it "datawrap".
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { claudeMcpSnippet, cursorMcpSnippet, vscodeMcpSnippet } from "./mcpClientConfig.js";

describe("MCP config snippets", () => {
  const url = "https://www.datawrap.io/api/v1/mcp";
  for (const [name, make] of [["cursor", cursorMcpSnippet], ["claude", claudeMcpSnippet], ["vscode", vscodeMcpSnippet]] as const) {
    it(`${name} snippet registers the server as "datawrap"`, () => {
      const text = make(url);
      const cfg = JSON.parse(text);
      const servers = cfg.mcpServers ?? cfg.servers;
      assert.deepEqual(Object.keys(servers), ["datawrap"]);
      assert.doesNotMatch(text, /dataflow/i);
    });
  }
});
