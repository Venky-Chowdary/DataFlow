/**
 * The last-stream digest must not render as a green whole-job match.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { Gate8ProofCard } from "./Gate8ProofCard.tsx";

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
