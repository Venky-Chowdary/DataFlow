/**
 * Run: npx --yes tsx --test apps/web/src/lib/runCarryNotes.test.tsx
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { RunCarryNotes } from "../components/jobs/RunCarryNotes.tsx";

describe("RunCarryNotes", () => {
  it("lists writer warnings and unsupported foreign keys from the summary", () => {
    const html = renderToStaticMarkup(
      <RunCarryNotes
        job={{
          destination_summary: {
            warnings: ["batch checksum skipped"],
            foreign_keys: {
              verdict: "partial",
              carried: 1,
              decisions: [
                {
                  status: "unsupported",
                  dest_table: "orders",
                  referenced_table: "customers",
                  reason: "parent type differs",
                },
              ],
            },
          },
        }}
      />,
    );
    assert.match(html, /batch checksum skipped/);
    assert.match(html, /Foreign keys partially carried/);
    assert.match(html, /orders → customers: unsupported — parent type differs/);
  });

  it("states a clean recreate without listing each carried key", () => {
    const html = renderToStaticMarkup(
      <RunCarryNotes
        job={{
          destination_summary: {
            foreign_keys: {
              verdict: "carried",
              carried: 2,
              decisions: [
                { status: "carried", dest_table: "orders", referenced_table: "customers" },
                { status: "carried", dest_table: "lines", referenced_table: "orders" },
              ],
            },
          },
        }}
      />,
    );
    assert.match(html, /2 foreign keys recreated on the destination/);
    assert.doesNotMatch(html, /orders → customers/);
  });
});
