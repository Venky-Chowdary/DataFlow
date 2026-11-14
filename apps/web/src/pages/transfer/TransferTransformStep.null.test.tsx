/**
 * UX-10: the Transform sample printed the engine's NULL wire token
 * (`__DF_SQL_NULL__`) in the grid. Cells now go through lib/nullWire.ts and
 * render the Query grid's NULL marker.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { SampleCell } from "./TransferTransformStep.js";
import { MISSING_SENTINEL, SQL_NULL_SENTINEL } from "../../lib/nullWire.js";

const cell = (value: unknown) => renderToStaticMarkup(createElement(SampleCell, { value }));

describe("Transform sample cells", () => {
  it("renders SQL NULL as the NULL marker, never the wire token", () => {
    assert.equal(cell(SQL_NULL_SENTINEL), '<span data-cell="null">NULL</span>');
    assert.equal(cell(` ${SQL_NULL_SENTINEL} `), '<span data-cell="null">NULL</span>');
    assert.equal(cell(null), '<span data-cell="null">NULL</span>');
  });

  it("renders an absent document field as absent", () => {
    assert.equal(cell(MISSING_SENTINEL), '<span data-cell="missing">(absent)</span>');
  });

  it("keeps real values, including an empty string, as they are", () => {
    assert.equal(cell("c1@example.com"), "c1@example.com");
    assert.equal(cell(""), "");
    assert.equal(cell(0), "0");
    assert.equal(cell(false), "false");
    assert.equal(cell({ a: 1 }), "{&quot;a&quot;:1}");
  });
});
