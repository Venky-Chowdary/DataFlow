import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  parseLocaleDecimalText,
  parseLocaleNumber,
} from "./numberLocale.js";

describe("number locale contract (browser = write path)", () => {
  it("parses unambiguous money and both-separator forms under Auto", () => {
    const cases: Array<[string, string]> = [
      ["$1,000.00", "1000.00"],
      ["€2.000,50", "2000.50"],
      ["USD 1000000.89", "1000000.89"],
      ["1,234,567.89", "1234567.89"],
      ["1.234.567,89", "1234567.89"],
      ["12.34", "12.34"],
      ["12,34", "12.34"],
      ["1000", "1000"],
      ["1.2345", "1.2345"],
      ["52.310500000000000", "52.310500000000000"],
    ];
    for (const [raw, expected] of cases) {
      assert.equal(parseLocaleDecimalText(raw), expected, raw);
    }
  });

  it("refuses a lone three-digit group under Auto", () => {
    for (const raw of ["1,234", "1.234", "01,234"]) {
      assert.equal(parseLocaleNumber(raw), null, raw);
    }
  });

  it("honors US locale", () => {
    assert.equal(parseLocaleDecimalText("1,234", "US"), "1234");
    assert.equal(parseLocaleDecimalText("1.234", "US"), "1.234");
    assert.equal(parseLocaleDecimalText("$1,234", "US"), "1234");
  });

  it("honors EU locale", () => {
    assert.equal(parseLocaleDecimalText("1,234", "EU"), "1.234");
    assert.equal(parseLocaleDecimalText("1.234", "EU"), "1234");
    assert.equal(parseLocaleDecimalText("€1.234", "EU"), "1234");
  });

  it("lets $ / € disambiguate a lone group without an operator locale", () => {
    assert.equal(parseLocaleDecimalText("$1,234"), "1234");
    assert.equal(parseLocaleDecimalText("€1.234"), "1234");
  });

  it("parses accounting negatives the write path accepts", () => {
    assert.equal(parseLocaleDecimalText("(1,234.56)"), "-1234.56");
  });
});
