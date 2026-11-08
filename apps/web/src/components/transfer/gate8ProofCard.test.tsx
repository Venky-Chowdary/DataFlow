/**
 * The last-stream digest must not render as a green whole-job match.
 */
import assert from "node:assert/strict";
import { afterEach, before, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import type { Root } from "react-dom/client";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { Gate8ProofCard } from "./Gate8ProofCard.tsx";

describe("Gate8ProofCard literal proof verdict", () => {
  for (const verdict of ["false", "true", 1, undefined]) {
    it(`does not claim full verification for ${String(verdict)}`, () => {
      const html = renderToStaticMarkup(<Gate8ProofCard report={{
        passed: verdict as boolean, phase: "post_write", coverage: "full_checksum",
        source_checksum_provenance: "independent_source_reread",
        source_checksum: "abc", target_checksum: "abc", source_rows: 5, target_rows: 5,
      }} />);
      assert.doesNotMatch(html, /Source and destination match/);
      assert.doesNotMatch(html, />Verified</);
    });
  }
  for (const [name, overrides] of [
    ["different checksums", { target_checksum: "different" }],
    ["explicit checksum veto", { checksum_match: false }],
    ["missing source provenance", { source_checksum_provenance: undefined }],
  ] as const) {
    it(`does not claim full verification for ${name}`, () => {
      const html = renderToStaticMarkup(<Gate8ProofCard report={{
        passed: true, phase: "post_write", coverage: "full_checksum",
        source_checksum_provenance: "independent_source_reread",
        source_checksum: "abc", target_checksum: "abc", source_rows: 5, target_rows: 5,
        ...overrides,
      }} />);
      assert.doesNotMatch(html, /Source and destination match/);
      assert.doesNotMatch(html, />Verified</);
    });
  }
});

describe("Gate8ProofCard last stream", () => {
  it("labels the 2-row digest and the job dest sum separately", () => {
    const html = renderToStaticMarkup(
      <Gate8ProofCard
        report={{
          passed: true,
          assurance_level: "per_stream_checksum",
          coverage: "per_stream_checksum",
          checksum_scope: "last_stream",
          source_checksum_provenance: "independent_source_reread",
          source_checksum: "abc",
          target_checksum: "abc",
          source_rows: 2,
          target_rows: 2,
          job_dest_count: 4,
          migration_proven: false,
          message: "Checksum matches orders (2 rows). Job destination population is 4 across 2 streams (each COUNT(*)). This digest is not the whole job.",
          match_summary: {
            source_rows: 2,
            dest_rows: 2,
            sample_match_percent: 100,
            denominator: "8 cell(s) across 2 key-aligned row(s) of the read-back sample — sample evidence, not population proof",
          },
        }}
      />,
    );
    assert.match(html, /Last stream source/);
    assert.match(html, /Last stream dest/);
    assert.match(html, /Job dest COUNT/);
    assert.match(html, /Match \(last stream\)/);
    assert.match(html, /last stream source/);
    assert.match(html, /job dest/);
    assert.match(html, /migration_proven stays false/);
    assert.doesNotMatch(html, />Source rows</);
    assert.doesNotMatch(html, />Match</);
  });

  it("keeps a single-table match labeled as the job", () => {
    const html = renderToStaticMarkup(
      <Gate8ProofCard
        report={{
          passed: true,
          assurance_level: "full_checksum",
          coverage: "full_checksum",
          source_checksum_provenance: "independent_source_reread",
          source_checksum: "abc",
          target_checksum: "abc",
          source_rows: 4,
          target_rows: 4,
        }}
      />,
    );
    assert.match(html, /Source rows/);
    assert.match(html, />Match</);
    assert.doesNotMatch(html, /Last stream source/);
    assert.doesNotMatch(html, /Match \(last stream\)/);
  });
});

describe("Gate8ProofCard business rule checks", () => {
  it("renders failed and unverified rule findings", () => {
    const html = renderToStaticMarkup(
      <Gate8ProofCard
        report={{
          passed: false,
          message: "Reconciliation did not verify",
          rule_checks: [
            {
              rule_id: "uniq-email",
              type: "unique",
              column: "email",
              status: "failed",
              duplicates: 1,
            },
          ],
          unverified_rules: [
            { rule_id: "uniq-order", reason: "unsupported_destination" },
          ],
        }}
      />,
    );
    assert.match(html, /Business rule checks/);
    assert.match(html, /Business rule uniq-email: 1 duplicate value\(s\) in email after load/);
    assert.match(html, /Business rule uniq-order not verified: unsupported_destination/);
  });
});

describe("Gate8ProofCard file export", () => {
  it("a file export with no cell read-back is never labeled Verified / Source and destination match", () => {
    const html = renderToStaticMarkup(
      <Gate8ProofCard
        report={{
          passed: true,
          unproven: true,
          skipped_readback: true,
          migration_proven: false,
          source_rows: 3,
          target_rows: 3,
          message: "File/object export wrote successfully — Gate-8 cell fidelity unproven (no destination cell read-back). Independent artifact record count is 3. Writer acknowledgement is diagnostic.",
        }}
      />,
    );
    assert.match(html, /Rows counted — cell values not read back/);
    assert.match(html, /Count only/);
    assert.doesNotMatch(html, /Source and destination match/);
    assert.doesNotMatch(html, />Verified</);
  });
});

describe("Gate8ProofCard signature isolation", () => {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
  const pending: Array<(response: Response) => void> = [];
  let root: Root | null = null;
  let host: HTMLElement | null = null;
  before(() => {
    Object.assign(globalThis, {
      window: dom.window, document: dom.window.document, HTMLElement: dom.window.HTMLElement,
      localStorage: dom.window.localStorage, sessionStorage: dom.window.sessionStorage,
      IS_REACT_ACT_ENVIRONMENT: true,
    });
    globalThis.fetch = ((input: RequestInfo | URL, options?: RequestInit) => {
      const method = options?.method || (input instanceof Request ? input.method : "GET");
      if (method === "POST") return new Promise<Response>((resolve) => pending.push(resolve));
      return Promise.resolve(new Response("{}", { status: 200 }));
    }) as typeof fetch;
  });
  async function render(jobId: string) {
    await import("../../lib/api");
    const { createRoot } = await import("react-dom/client");
    if (!root) {
      host = document.createElement("div");
      document.body.appendChild(host);
      root = createRoot(host);
    }
    await React.act(async () => root!.render(<Gate8ProofCard jobId={jobId} report={{
      passed: false, phase: "post_write_failed", source_rows: 5, target_rows: 4,
    }} />));
  }
  async function startVerification() {
    const input = document.querySelector<HTMLInputElement>('input[type="file"]')!;
    Object.defineProperty(input, "files", { configurable: true, value: [{ text: async () => '{"job_id":"job-a"}' }] });
    await React.act(async () => {
      input.dispatchEvent(new dom.window.Event("change", { bubbles: true }));
      await new Promise((resolve) => setTimeout(resolve, 0));
    });
    assert.equal(pending.length, 1, host?.textContent || "No mounted card");
  }
  async function verified() {
    await React.act(async () => {
      pending.shift()!(new Response(JSON.stringify({ ok: true, errors: [], content_sha256: "old-file-hash" }), { status: 200 }));
      await new Promise((resolve) => setTimeout(resolve, 0));
    });
  }
  afterEach(async () => {
    if (root) await React.act(async () => root!.unmount());
    root = null;
    host?.remove();
    host = null;
    pending.length = 0;
  });
  it("does not upgrade failed data after signature verification", async () => {
    await render("job-a");
    await startVerification();
    await verified();
    assert.match(host!.textContent || "", /Signature valid/);
    assert.match(host!.textContent || "", /Reconciliation did not verify/);
    assert.doesNotMatch(host!.textContent || "", /Source and destination match/);
  });
  it("ignores an old file-verification response after the job changes", async () => {
    await render("job-a");
    await startVerification();
    await render("job-b");
    await verified();
    assert.doesNotMatch(host!.textContent || "", /Signature valid|old-file-hash/);
  });
});
