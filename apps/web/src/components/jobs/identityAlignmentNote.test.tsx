import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { IdentityAlignmentNote } from "./IdentityAlignmentNote.tsx";

describe("IdentityAlignmentNote", () => {
  it("shows the last-stream re-read and does not call it a mismatch", () => {
    const html = renderToStaticMarkup(
      createElement(IdentityAlignmentNote, {
        job: {
          destination_summary: {
            multi_stream: true,
            table: "orders",
            streams: [{ name: "customers" }, { name: "orders" }],
            identity_alignment: {
              identity_hash_aligned: null,
              write_pass_rows: 0,
              reread_rows: 2,
              reason: "write_pass_not_fingerprinted",
            },
          },
        },
      }),
    );
    assert.match(html, /Identity alignment/);
    assert.match(html, /Last stream orders/);
    assert.match(html, /2 rows/);
    assert.match(html, /not a hash mismatch/);
    assert.doesNotMatch(html, /did not align/);
  });

  it("stays quiet when the engine did not stamp alignment", () => {
    const html = renderToStaticMarkup(
      createElement(IdentityAlignmentNote, { job: { destination_summary: { table: "orders" } } }),
    );
    assert.equal(html, "");
  });
});
