import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { ambiguousDateColumns, isAmbiguousMdyDmy } from "./dateLocale.ts";

describe("dateLocale Auto contract", () => {
  it("fails closed on 01/02/2024 and accepts 31/12 and 12/31 as unambiguous", () => {
    assert.equal(isAmbiguousMdyDmy("01/02/2024"), true);
    assert.equal(isAmbiguousMdyDmy("03/04/2024"), true);
    assert.equal(isAmbiguousMdyDmy("31/12/2024"), false);
    assert.equal(isAmbiguousMdyDmy("12/31/2024"), false);
    assert.equal(isAmbiguousMdyDmy("05/05/2024"), false);
    assert.equal(isAmbiguousMdyDmy("2024-01-02"), false);
    assert.equal(isAmbiguousMdyDmy("01/02/2024", "MDY"), false);
  });

  it("names columns whose samples are Auto-ambiguous", () => {
    const findings = ambiguousDateColumns(
      [{ event_date: "01/02/2024" }, { note: "ok" }],
      ["event_date", "note"],
    );
    assert.equal(findings.length, 1);
    assert.equal(findings[0].column, "event_date");
    assert.ok(findings[0].samples.includes("01/02/2024"));
  });
});
