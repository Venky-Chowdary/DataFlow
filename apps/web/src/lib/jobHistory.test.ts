import assert from "node:assert/strict";
import test from "node:test";

import { jobFilterCounts, jobHistoryFromResponse, jobWindowNote } from "./jobHistory";
import type { TransferJob } from "./types";

function job(id: string, status: string): TransferJob {
  return { _id: id, status, created_at: "2026-01-01T00:00:00Z" } as TransferJob;
}

test("filter counts come from the counted history, not the page of rows", () => {
  const history = jobHistoryFromResponse({
    jobs: [job("a", "completed"), job("b", "failed")],
    total: 90,
    status_counts: {
      completed: 42,
      completed_with_quarantine: 0,
      running: 17,
      pending: 2,
      failed: 29,
    },
  });
  const counts = jobFilterCounts(history);
  assert.equal(counts.all, 90);
  assert.equal(counts.running, 19);
  assert.equal(counts.completed, 42);
  assert.equal(counts.failed, 29);
});

test("a quarantined success counts as completed and as quarantine", () => {
  const counts = jobFilterCounts(
    jobHistoryFromResponse({
      jobs: [],
      total: 5,
      status_counts: { completed: 3, completed_with_quarantine: 2 },
    }),
  );
  assert.equal(counts.completed, 5);
  assert.equal(counts.quarantine, 2);
  assert.equal(counts.all, 5);
});

test("an uncounted response falls back to the rows it returned", () => {
  const history = jobHistoryFromResponse({
    jobs: [job("a", "failed"), job("b", "failed"), job("c", "running")],
  });
  assert.equal(history.total, 3);
  const counts = jobFilterCounts(history);
  assert.equal(counts.failed, 2);
  assert.equal(counts.running, 1);
  assert.equal(counts.all, 3);
});

test("an empty history counts zero everywhere", () => {
  const counts = jobFilterCounts(jobHistoryFromResponse({ jobs: [], total: 0, status_counts: {} }));
  assert.deepEqual(counts, { all: 0, running: 0, completed: 0, quarantine: 0, failed: 0 });
});

test("statuses the chips do not name still count in the total", () => {
  const counts = jobFilterCounts(
    jobHistoryFromResponse({ jobs: [], total: 4, status_counts: { cancelled: 3, completed: 1 } }),
  );
  assert.equal(counts.all, 4);
  assert.equal(counts.completed, 1);
  assert.equal(counts.failed, 0);
});

const NINETY = jobFilterCounts(
  jobHistoryFromResponse({
    jobs: [],
    total: 90,
    status_counts: { completed: 42, running: 17, pending: 2, failed: 29 },
  }),
);

test("a filtered window says how much of that status it holds", () => {
  assert.equal(
    jobWindowNote({ counts: NINETY, filter: "failed", loaded: 50, shown: 10 }),
    "Showing 10 of 29 failed job(s) — this list holds the 50 most recent of 90 jobs.",
  );
});

test("the unfiltered window names only the page it holds", () => {
  assert.equal(
    jobWindowNote({ counts: NINETY, filter: "all", loaded: 50, shown: 50 }),
    "Showing the 50 most recent of 90 jobs.",
  );
});

test("a search-narrowed list does not claim its rows are the status slice", () => {
  assert.equal(
    jobWindowNote({ counts: NINETY, filter: "failed", loaded: 50 }),
    "This list holds the 50 most recent of 90 jobs.",
  );
});

test("no window note when the page holds the whole history", () => {
  const counts = jobFilterCounts(
    jobHistoryFromResponse({ jobs: [], total: 12, status_counts: { failed: 12 } }),
  );
  assert.equal(jobWindowNote({ counts, filter: "failed", loaded: 12, shown: 12 }), undefined);
});
