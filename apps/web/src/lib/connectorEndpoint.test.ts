/**
 * Run: npx --yes tsx --test apps/web/src/lib/connectorEndpoint.test.ts
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import { connectorEndpointLabel } from "./connectorEndpoint.js";
import { preserveSourceDecimalPrecision } from "./typeDisplay.js";

describe("Open Validate lands on the recovered route", () => {
  it("does not force Source when the caller asked for Validate", () => {
    const page = readFileSync(new URL("../pages/TransferPage.tsx", import.meta.url), "utf8");
    assert.match(page, /const askedValidate = seedStudioIntent\.step === "validate"/);
    assert.match(page, /else if \(askedValidate\)/);
    assert.match(page, /setAnalysis\(null\)/);
    assert.match(page, /setActiveData\(null\)/);
  });
});

describe("connector endpoint honesty", () => {
  it("shows an Atlas host instead of a leftover localhost field", () => {
    assert.equal(
      connectorEndpointLabel({
        host: "localhost",
        port: 27017,
        dial_host: "cluster0.ab.mongodb.net",
        dial_host_loopback: false,
      }),
      "cluster0.ab.mongodb.net",
    );
  });

  it("says loopback when the stored target really is localhost", () => {
    assert.equal(
      connectorEndpointLabel({ host: "localhost", port: 27017, dial_host_loopback: true }),
      "localhost (loopback — not a cloud target)",
    );
  });
});

describe("declared decimal precision", () => {
  it("keeps DECIMAL(18,6) when the mapper offered the MySQL floor", () => {
    assert.equal(
      preserveSourceDecimalPrecision("DECIMAL(18,6)", "DECIMAL(38,15)"),
      "DECIMAL(18,6)",
    );
    assert.equal(
      preserveSourceDecimalPrecision("NUMERIC(18,6)", "DECIMAL(38,15)"),
      "NUMERIC(18,6)",
    );
  });

  it("does not rewrite an existing physical column or an operator type", () => {
    assert.equal(
      preserveSourceDecimalPrecision("DECIMAL(18,6)", "DECIMAL(10,2)", true),
      "DECIMAL(10,2)",
    );
    assert.equal(
      preserveSourceDecimalPrecision("DECIMAL(18,6)", "DECIMAL(12,4)"),
      "DECIMAL(12,4)",
    );
  });
});
