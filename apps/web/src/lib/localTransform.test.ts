import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { applyLocalTransform, UntrustedLocalTransform } from "./localTransform.js";
import { runLocalFileExport } from "./localFileExport.js";
import type { EditableMapping } from "./mapping.js";

describe("local preview fidelity", () => {
  it("refuses to invent a hash, an IBAN, a time, or a boolean", () => {
    for (const transform of ["hash_pii", "iban", "time", "boolean"]) {
      assert.throws(
        () => applyLocalTransform("yes", transform),
        (err: unknown) => err instanceof UntrustedLocalTransform && !String(err.message).includes("sha256:"),
      );
    }
  });

  it("still trims locally", () => {
    assert.equal(applyLocalTransform("  ada ", "trim"), "ada");
  });

  it("does not download a file that contains a toy digest", () => {
    const mapping = {
      source: "email",
      target: "email",
      confidence: 1,
      transform: "hash_pii",
      approved: true,
      requiresReview: false,
      isPii: true,
    } as EditableMapping;
    const result = runLocalFileExport({
      sourceFilename: "people.csv",
      rows: [{ email: "a@b.c" }],
      mappings: [mapping],
      format: "csv",
    });
    assert.equal(result.success, false);
    assert.match(result.error || "", /server/);
    assert.equal(JSON.stringify(result).includes("sha256:"), false);
  });
});
