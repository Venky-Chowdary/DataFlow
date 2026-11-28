/**
 * One verdict vocabulary (UI pass 2, item 1).
 * Run: npx tsx --test apps/web/src/lib/verdict.test.ts
 */
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { VerdictChip } from "../components/ui/VerdictChip.tsx";
import { VERDICT_WORDS } from "./runResult.ts";
import type { TransferJob } from "./types.ts";
import {
  VERDICT_CHIP_WORDS,
  VERDICT_RANK,
  jobVerdictState,
  jobsNeedingVerdicts,
  mergeListVerdicts,
  verdictIdsToFetch,
  verdictPresentation,
  listVerdictStamp,
  type VerdictState,
} from "./verdict.ts";

const SRC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

describe("verdictPresentation", () => {
  it("uses the Trust Report's five words, one colour and one glyph per verdict", () => {
    const five = ["proven", "explained", "no_difference_found", "unexplained", "unverifiable"] as const;
    for (const v of five) assert.equal(verdictPresentation(v).word, VERDICT_WORDS[v]);
    assert.deepEqual(five.map((v) => verdictPresentation(v).tone), ["success", "success", "info", "danger", "neutral"]);
    const states: VerdictState[] = [...five, "not_checked", "checking", "not_loaded"];
    const icons = new Set(states.filter((s) => s !== "not_loaded").map((s) => verdictPresentation(s).icon));
    assert.equal(icons.size, 7, "each verdict and waiting state has its own glyph");
    assert.equal(verdictPresentation("checking").isVerdict, false);
    assert.equal(verdictPresentation("not_checked").word, "Not checked yet");
  });

  it("renders icon + word (never colour alone) and the unexplained row count", () => {
    const html = renderToStaticMarkup(React.createElement(VerdictChip, { state: "unexplained", unexplainedRows: 4 }));
    assert.match(html, /data-tone="danger" data-verdict="unexplained"/);
    assert.match(html, /<svg[^>]*aria-hidden="true"/);
    assert.match(html, /Unexplained<\/span><span class="df2-verdict-count">· 4 rows</);
    assert.doesNotMatch(renderToStaticMarkup(React.createElement(VerdictChip, { state: "proven", unexplainedRows: 4 })), /rows/);
  });
});

describe("Parity at Scale's verifying state (train 2)", () => {
  it("a listed 'verifying' is a waiting state with its own word, never a verdict or 'Checking'", () => {
    const job = { status: "completed", verification_summary: { passed: true } } as unknown as Pick<TransferJob, "status" | "verification_summary">;
    const state = jobVerdictState(job, { verdict: "verifying", unexplained: 0 }, "ready");
    assert.equal(state, "verifying");
    const shown = verdictPresentation(state);
    assert.equal(shown.word, "Verification running");
    assert.equal(shown.isVerdict, false);
    assert.equal(shown.tone, "neutral");
    assert.ok(VERDICT_RANK.verifying > VERDICT_RANK.checking && VERDICT_RANK.verifying < VERDICT_RANK.explained);
  });
});

describe("guard: no second verdict vocabulary", () => {
  it("the old grid labels are gone from the app source", () => {
    // Help-article figures describe screenshots taken before this change; their
    // alt text follows the images when the docs captures are regenerated.
    const banned = [/["'>]Verified \(sample\)/, /["'>]Verified \(partial\)/, /label: "Verified"/];
    const skip = new Set(["lib/helpDocs.ts"]);
    const offenders: string[] = [];
    const walk = (dir: string) => {
      for (const entry of readdirSync(dir, { withFileTypes: true })) {
        const full = path.join(dir, entry.name);
        if (entry.isDirectory()) {
          if (entry.name !== "marketing" && entry.name !== "__fixtures__") walk(full);
        } else if (/\.tsx?$/.test(entry.name) && !/\.test\.tsx?$/.test(entry.name)) {
          const text = readFileSync(full, "utf8");
          const rel = path.relative(SRC, full);
          if (!skip.has(rel) && banned.some((re) => re.test(text))) offenders.push(rel);
        }
      }
    };
    walk(SRC);
    assert.deepEqual(offenders, []);
    assert.ok(VERDICT_CHIP_WORDS.includes("No difference found"));
  });
});

describe("list verdict cache (review UI2 #2)", () => {
  const job = (id: string, patch: Partial<TransferJob> = {}) =>
    ({ _id: id, status: "completed", updated_at: "t1", verification_summary: { passed: true }, ...patch }) as TransferJob;
  const proven = { verdict: "proven" as const, unexplained: 0 };
  const bad = { verdict: "unexplained" as const, unexplained: 4 };

  it("asks only for finished, reconciled rows it has no current verdict for", () => {
    const rows = [job("a"), job("b", { status: "running" }), job("c", { verification_summary: undefined }), job("a")];
    assert.deepEqual(jobsNeedingVerdicts(rows), ["a"]);
    const cache = new Map([["a", { stamp: listVerdictStamp(rows[0]), verdict: proven, at: 1 }]]);
    assert.deepEqual(verdictIdsToFetch(rows, cache), []);
    assert.deepEqual(verdictIdsToFetch([job("a", { updated_at: "t2" })], cache), ["a"], "a re-run re-reads");
    const merged = mergeListVerdicts(rows, cache);
    assert.equal(merged[0].parity_verdict?.verdict, "proven");
    assert.equal(merged[1], rows[1], "rows without a verdict are untouched");
    const untouched = [job("z")];
    assert.equal(mergeListVerdicts(untouched, cache), untouched, "no current verdict: the same array, no re-render");
  });

  it("re-reads when Parity Studio changes the verdict without touching updated_at", () => {
    const before = job("a", { parity_stamp: "1:2026-11-21T10:00:00Z" });
    const cache = new Map([["a", { stamp: listVerdictStamp(before), verdict: proven, at: 1 }]]);
    assert.deepEqual(verdictIdsToFetch([job("a", { parity_stamp: "2:2026-11-21T10:05:00Z" })], cache), ["a"]);
  });

  it("a newer Trust Report read wins over the list; an older one does not", () => {
    const rows = [job("a")];
    const cache = new Map([["a", { stamp: listVerdictStamp(rows[0]), verdict: proven, at: 10 }]]);
    assert.equal(mergeListVerdicts(rows, cache, new Map([["a", { verdict: bad, at: 20 }]]))[0].parity_verdict?.verdict, "unexplained");
    assert.equal(mergeListVerdicts(rows, cache, new Map([["a", { verdict: bad, at: 5 }]]))[0].parity_verdict?.verdict, "proven");
  });

  it("a failed read shows Not loaded but is never cached", () => {
    const rows = [job("a")];
    const merged = mergeListVerdicts(rows, new Map(), new Map(), new Set(["a"]));
    assert.equal(merged[0].parity_verdict, null);
    assert.deepEqual(verdictIdsToFetch(rows, new Map()), ["a"], "still wanted: retried on the next read");
  });
});
