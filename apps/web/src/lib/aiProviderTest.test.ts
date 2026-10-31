/**
 * QA enterprise (557f556c) #42: Ollama read "ready" while offline and its
 * test said "no cloud key to test". The card now offers a connection test
 * and reports the server's answer.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { providerHasTest, providerTestLabel, providerTestToast } from "./aiProviderTest.js";

describe("Ollama connection test", () => {
  it("is offered for Ollama even when it is not configured", () => {
    assert.equal(providerHasTest({ provider: "ollama", tier: "local", configured: false }), true);
    assert.equal(providerHasTest({ provider: "openai", tier: "cloud", configured: false }), false);
    assert.equal(providerTestLabel("ollama"), "Test connection");
    assert.equal(providerTestLabel("openai"), "Test key");
  });

  it("reports an offline server in connection terms", () => {
    const t = providerTestToast("ollama", { ok: false, error: "Ollama is not reachable at http://x:11434 (ConnectError)." });
    assert.equal(t.title, "Ollama is not ready");
    assert.match(t.message, /not reachable/);
    assert.doesNotMatch(t.title, /key/);
  });
});
