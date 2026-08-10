import { describe, expect, it } from "vitest";
import { MISSING_SENTINEL, SQL_NULL_SENTINEL, nullWireLabel } from "./nullWire";

describe("nullWireLabel", () => {
  it("labels wire sentinels an operator should never read raw", () => {
    expect(nullWireLabel(SQL_NULL_SENTINEL)).toBe("NULL");
    expect(nullWireLabel(` ${MISSING_SENTINEL} `)).toBe("absent");
  });

  it("leaves real values alone", () => {
    expect(nullWireLabel("")).toBeNull();
    expect(nullWireLabel("0")).toBeNull();
    expect(nullWireLabel("__DF_SQL_NULL__x")).toBeNull();
    expect(nullWireLabel(null)).toBeNull();
    expect(nullWireLabel(12)).toBeNull();
  });
});
