/**
 * Run: npx --yes tsx --test apps/web/src/lib/streamHealthTable.test.tsx
 *
 * Proves a summary-only stream list is what the operator table renders.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { StreamHealthTable } from "../components/jobs/StreamHealthTable.tsx";
import { readJobStreams } from "./jobEvidence.ts";

describe("StreamHealthTable", () => {
  it("renders each stream when health exists only on the destination summary", () => {
    const streams = readJobStreams({
      destination_summary: {
        streams: [
          {
            name: "orders",
            status: "completed",
            records_processed: 50,
            watermark: "phase=snapshot|table=orders",
            row_accounting: {
              conservation_kind: "overwrite",
              dest_count: 50,
              balanced: true,
              rows_written_source: "dest_readback",
            },
          },
          { name: "users", status: "failed", records_processed: 0, error: "count refused" },
        ],
      },
    });
    const html = renderToStaticMarkup(<StreamHealthTable streams={streams} />);
    assert.match(html, /orders/);
    assert.match(html, /users/);
    assert.match(html, /count refused/);
    assert.match(html, /50/);
    assert.equal(streams.length, 2);
  });
});
