/**
 * CLM-15, CLM-17: product copy says what the product does.
 * Run: npx --yes tsx --test apps/web/src/lib/claimsWording.test.ts
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";

const SRC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const CATALOG = path.resolve(SRC, "..", "..", "api", "data", "connector_catalog.json");

describe("claims wording", () => {
  it("CLM-15: the Query page does not promise every connector family", () => {
    const page = readFileSync(path.join(SRC, "pages", "QueryPage.tsx"), "utf8");
    assert.doesNotMatch(page, /every connector family/);
    assert.match(page, /Kafka topics and vector stores are not queried here/);
  });

  it("CLM-17: a connector with a reader is not described as destination-only", () => {
    const catalog = JSON.parse(readFileSync(CATALOG, "utf8")) as {
      connectors: { id: string; description?: string }[];
    };
    const readable = new Set(["qdrant", "pgvector"]);
    for (const entry of catalog.connectors) {
      if (readable.has(entry.id)) {
        assert.doesNotMatch(entry.description ?? "", /destination[- ]only/i, entry.id);
      }
    }
  });
});
