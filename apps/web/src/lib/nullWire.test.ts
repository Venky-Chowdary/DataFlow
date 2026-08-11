import assert from "node:assert/strict";
import test from "node:test";

import { MISSING_SENTINEL, SQL_NULL_SENTINEL, nullWireLabel } from "./nullWire";

test("labels wire sentinels an operator should never read raw", () => {
  assert.equal(nullWireLabel(SQL_NULL_SENTINEL), "NULL");
  assert.equal(nullWireLabel(` ${MISSING_SENTINEL} `), "absent");
});

test("leaves real values alone", () => {
  assert.equal(nullWireLabel(""), null);
  assert.equal(nullWireLabel("0"), null);
  assert.equal(nullWireLabel("__DF_SQL_NULL__x"), null);
  assert.equal(nullWireLabel(null), null);
  assert.equal(nullWireLabel(12), null);
});
