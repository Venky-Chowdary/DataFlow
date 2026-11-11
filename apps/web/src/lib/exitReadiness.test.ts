/**
 * Reversibility Proof helpers: pills, measured durations, the window ratio,
 * overdue wording, coverage honesty, the anchor, sign-off, the declare form and
 * deep links. Each wording rule from the plan is pinned here: nothing measured
 * reads "Not measured", sampled never reads as proof, an anchor that is not
 * write-once says so.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { ApiError } from "./api.js";
import {
  anchorPresentation,
  boardFilterCounts,
  boardFilterMatches,
  coverageLines,
  criticalityPresentation,
  draftFromSystem,
  dueText,
  emptyDraft,
  engineLabel,
  exitErrorMessage,
  exitFocusFromHash,
  exitHash,
  fitsText,
  formatBytes,
  formatDuration,
  formatWindow,
  growthText,
  isOverdue,
  measuredVsWindowText,
  newerRehearsal,
  normalizeRehearsal,
  pillClass,
  projectionText,
  readinessPresentation,
  rehearsalSignoff,
  rehearsalSignoffBlockedReason,
  rehearsalVerdictPresentation,
  shortHash,
  shouldPollRehearsal,
  sortBoardRows,
  stageStatusPresentation,
  tableStatusPresentation,
  validateDraft,
  valueMethodPresentation,
  windowRatio,
  type ExitRehearsal,
} from "./exitReadiness.js";
import {
  ANCHORED,
  ANCHOR_FAILED,
  NOT_ANCHORED,
  boardFixture,
  provenRehearsal,
  runningRehearsalRaw,
  systemFixture,
} from "../components/exit/exitFixtures.js";
import { realRunningRaw, realSigned } from "../components/exit/realExitPayloads.js";

const H = 3600;
const D = 86400;

describe("status pills", () => {
  it("maps every readiness state to words and a tone; overdue outranks an old proof", () => {
    assert.deepEqual(readinessPresentation("proven"), { label: "Proven", tone: "ok" });
    assert.deepEqual(readinessPresentation("not_proven"), { label: "Not proven", tone: "warn" });
    assert.deepEqual(readinessPresentation("incomplete"), { label: "Incomplete", tone: "danger" });
    assert.deepEqual(readinessPresentation("failed"), { label: "Failed", tone: "danger" });
    assert.deepEqual(readinessPresentation("running"), { label: "Running", tone: "run" });
    assert.deepEqual(readinessPresentation("never_rehearsed"), { label: "Never rehearsed", tone: "muted" });
    assert.deepEqual(readinessPresentation("proven", true), { label: "Overdue", tone: "warn" });
    // A running rehearsal is the fix for overdue: it shows as running.
    assert.equal(readinessPresentation("running", true).label, "Running");
  });

  it("never colours an unknown state green", () => {
    assert.deepEqual(readinessPresentation("brand_new_state"), { label: "Brand new state", tone: "muted" });
    assert.equal(rehearsalVerdictPresentation("weird").tone, "muted");
    assert.equal(tableStatusPresentation("weird").tone, "muted");
  });

  it("uses the job list's badges, including the running one", () => {
    assert.equal(pillClass("ok"), "df2-badge df2-badge-live");
    assert.equal(pillClass("danger"), "df2-badge df2-badge-error");
    assert.equal(pillClass("run"), "df2-badge df2-badge-run");
    assert.equal(pillClass("muted"), "df2-badge df2-badge-muted");
  });

  it("words criticality, stages and table statuses", () => {
    assert.equal(criticalityPresentation("critical").label, "Critical");
    assert.equal(stageStatusPresentation("running").label, "Running");
    assert.equal(tableStatusPresentation("not_proven").label, "Not proven");
    assert.equal(engineLabel("sqlserver"), "SQL Server");
    assert.equal(engineLabel(null), "Unknown engine");
  });
});

describe("measured durations", () => {
  it("formats a measurement, and null as Not measured (never zero)", () => {
    assert.equal(formatDuration(null), "Not measured");
    assert.equal(formatDuration(undefined), "Not measured");
    assert.equal(formatDuration(0), "0 s");
    assert.equal(formatDuration(42), "42 s");
    assert.equal(formatDuration(18 * 60), "18 min");
    assert.equal(formatDuration(2 * H + 5 * 60), "2 h 5 min");
    assert.equal(formatDuration(3 * H), "3 h");
    assert.equal(formatDuration(61 * H), "61 h");
    assert.equal(formatDuration(4.5 * D), "4.5 d");
  });

  it("reads a window the way it was declared", () => {
    assert.equal(formatWindow(30 * D), "30 d");
    assert.equal(formatWindow(36 * H), "36 h");
    assert.equal(formatWindow(0), "No window");
    assert.equal(measuredVsWindowText(61 * H, 30 * D), "61 h / 30 d");
    assert.equal(measuredVsWindowText(null, 30 * D), "Not measured");
  });

  it("computes the window ratio to scale, and none when nothing was measured", () => {
    const fit = windowRatio(61 * H, 30 * D)!;
    assert.ok(Math.abs(fit.ratio - 61 / 720) < 1e-9);
    assert.equal(fit.over, false);
    assert.equal(fit.label, "8.5% of the window");
    const over = windowRatio(40 * H, 36 * H)!;
    assert.equal(over.over, true);
    assert.equal(over.fraction, 1);
    assert.equal(over.label, "111% of the window");
    assert.equal(windowRatio(30, 30 * D)!.label, "<1% of the window");
    assert.equal(windowRatio(null, 30 * D), null);
    assert.equal(windowRatio(10, 0), null);
    assert.equal(fitsText(null), "Not measured");
    assert.equal(fitsText(false), "Does not fit the window");
  });

  it("labels a projection as one, with its basis", () => {
    const text = projectionText({ kind: "projection", seconds: 63 * H, basis: "rehearsal 6 vs 5" });
    assert.match(text, /^Projection, not measured: about 63 h/);
    assert.match(text, /Basis: rehearsal 6 vs 5/);
    assert.equal(projectionText(null), "");
    // The API's basis opens with its own label: "projection" is said once.
    const api = projectionText({ kind: "projection", seconds: 0, basis: "Projection, not a measurement: rehearsal 2 measured 0 s for 63 rows." });
    assert.equal(api, "Projection, not measured: about 0 s at the latest size. Basis: rehearsal 2 measured 0 s for 63 rows.");
    assert.equal(growthText(3), "Rows grew 3% since the previous rehearsal");
    assert.equal(growthText(-2.5), "Rows shrank 2.5% since the previous rehearsal");
    assert.equal(growthText(0), "Rows unchanged since the previous rehearsal");
    assert.equal(growthText(null), "");
  });

  it("formats bytes and hashes compactly", () => {
    assert.equal(formatBytes(2_400_000_000_000), "2.4 TB");
    assert.equal(formatBytes(812_000_000), "812 MB");
    assert.equal(formatBytes(null), "Unknown");
    assert.equal(shortHash("sha256:4be1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa09c2"), "sha256:4be1…09c2");
    assert.equal(shortHash("abc"), "abc");
  });
});

describe("overdue", () => {
  const now = new Date("2026-11-11T12:00:00Z");
  it("is overdue once the next rehearsal date has passed", () => {
    assert.equal(isOverdue("2026-10-18T09:00:00Z", now), true);
    assert.equal(isOverdue("2026-12-01T09:00:00Z", now), false);
    assert.equal(isOverdue(null, now), false);
  });

  it("words the due date from the API's flag", () => {
    assert.equal(dueText("2026-10-18T09:00:00Z", true, now), "Overdue by 24 days");
    assert.equal(dueText("2026-11-16T12:00:00Z", false, now), "Due in 5 days");
    assert.equal(dueText("2026-11-11T18:00:00Z", false, now), "Due today");
    assert.equal(dueText(null, true, now), "Overdue");
    assert.equal(dueText(null, false, now), "");
  });
});

describe("coverage honesty", () => {
  it("lists every method used, and sampled / count-only are not proof", () => {
    const lines = coverageLines({ full_checksum: 398, sampled: 14, count_only: 0, none: 2 });
    assert.deepEqual(lines.map((l) => [l.method, l.count, l.proves]), [
      ["full_checksum", 398, true],
      ["sampled", 14, false],
      ["none", 2, false],
    ]);
    assert.equal(valueMethodPresentation("population_digest").proves, true);
    assert.equal(valueMethodPresentation("sampled").proves, false);
    assert.equal(valueMethodPresentation("count_only").proves, false);
    assert.equal(valueMethodPresentation("something_new").proves, false);
    assert.deepEqual(coverageLines({}), []);
  });
});

describe("write-once anchor", () => {
  it("is green only when confirmed in S3 Object Lock", () => {
    const anchored = anchorPresentation(ANCHORED);
    assert.equal(anchored.label, "Anchored");
    assert.equal(anchored.tone, "ok");
    assert.match(anchored.detail, /S3 Object Lock \(compliance mode\) until/);
  });

  it("says the stub is not anchored and why", () => {
    const stub = anchorPresentation(NOT_ANCHORED);
    assert.equal(stub.label, "Not anchored");
    assert.equal(stub.tone, "muted");
    assert.equal(stub.detail, "the local stub is not a write-once store");
    assert.equal(anchorPresentation(null).label, "Not anchored");
  });

  it("fails closed: a receipt that could not be confirmed is a danger with its reason", () => {
    const failed = anchorPresentation(ANCHOR_FAILED);
    assert.equal(failed.label, "Anchor failed");
    assert.equal(failed.tone, "danger");
    assert.match(failed.detail, /could not be confirmed: the bucket .* Object Lock/);
  });
});

describe("sign-off", () => {
  it("adapts the rehearsal's sign-off for Parity's panel", () => {
    const s = rehearsalSignoff(realSigned);
    assert.equal(s.status, "signed");
    assert.deepEqual(s.required_roles, ["engineering", "data_owner", "compliance"]);
    assert.equal(s.approvals.length, 3);
  });

  it("opens only for a proven rehearsal, and keeps the API's reason", () => {
    assert.equal(rehearsalSignoffBlockedReason(provenRehearsal()), "");
    const blocked = rehearsalSignoffBlockedReason({
      verdict: "not_proven",
      signoff: { ...provenRehearsal().signoff, status: "blocked", blocked_reason: "Only a proven rehearsal can be signed." },
    });
    assert.equal(blocked, "Only a proven rehearsal can be signed.");
    const fallback = rehearsalSignoffBlockedReason({ verdict: "running", signoff: { ...provenRehearsal().signoff, status: "blocked", blocked_reason: null } });
    assert.match(fallback, /when the rehearsal has finished/);
  });

  it("never rolls back to an older version", () => {
    const v7 = provenRehearsal();
    const v6 = { ...v7, version: 6, verdict: "running" as const };
    assert.equal(newerRehearsal(v7, v6), v7);
    assert.equal(newerRehearsal(v6, v7), v7);
    assert.equal(newerRehearsal(null, v6), v6);
  });

  it("polls only a running rehearsal, bounded", () => {
    assert.equal(shouldPollRehearsal({ verdict: "running" }, 3), true);
    assert.equal(shouldPollRehearsal({ verdict: "running" }, 10_000), false);
    assert.equal(shouldPollRehearsal({ verdict: "proven" }, 1), false);
    assert.equal(shouldPollRehearsal(null, 1), false);
  });
});

describe("a running rehearsal as the API sends it", () => {
  it("fills every table field the views read with 'not measured', never a match", () => {
    for (const raw of [realRunningRaw as ExitRehearsal, runningRehearsalRaw()]) {
      const r = normalizeRehearsal(raw);
      assert.equal(r.verdict, "running");
      for (const t of r.tables) {
        assert.equal(t.key_set.match, null);
        assert.equal(t.row_set.match, null);
        assert.deepEqual(t.control_totals, []);
        assert.deepEqual(t.differences, []);
        assert.equal(t.value_parity.method, "none");
        assert.equal(t.rows_source, null);
        assert.equal(t.part, null);
      }
      assert.equal(r.seal.merkle_root, null);
      assert.equal(r.duration.measured_seconds, null);
    }
  });
});

describe("the board", () => {
  const board = boardFixture();
  it("filters and counts systems by readiness", () => {
    const counts = boardFilterCounts(board.systems);
    assert.equal(counts.all, 9);
    assert.equal(counts.overdue, 2);
    assert.equal(counts.never, 1);
    assert.equal(counts.attention, 3); // incomplete, not proven, failed
    assert.equal(counts.proven, 3); // proven and not overdue
    assert.equal(boardFilterMatches({ state: "proven", overdue: true }, "proven"), false);
  });

  it("puts overdue systems first, then by criticality", () => {
    const order = sortBoardRows(board.systems).map((s) => s.name);
    assert.deepEqual(order.slice(0, 2), ["Payments", "Loyalty"]);
    assert.equal(order[2], "Billing"); // first critical, alphabetically
  });
});

describe("the declare form", () => {
  it("refuses what the API would 422, field by field", () => {
    const { errors, input } = validateDraft({ ...emptyDraft(), tables: [{ name: "orders", schema: "", keys: "" }, { name: "ORDERS", schema: "", keys: "" }], windowValue: "0" });
    assert.equal(input, null);
    assert.match(errors.name, /Name the system/);
    assert.match(errors.connector, /saved connection/);
    assert.match(errors.tables, /declared twice/);
    assert.match(errors.window, /1 hour to 10 years/);
  });

  it("builds the contract body: days become hours, keys split, empty dates dropped", () => {
    const draft = {
      ...emptyDraft(),
      name: " Core ledger ",
      connectorId: "ora-fin",
      tables: [{ name: "gl_entries", schema: "FIN", keys: "entry_id, line" }, { name: "", schema: "", keys: "" }],
      windowValue: "30",
      windowUnit: "days" as const,
      regulatoryDates: [{ date: "2027-01-12", label: "Data Act" }, { date: "", label: "" }],
      scheduleCron: "0 2 1 * *",
    };
    const { errors, input } = validateDraft(draft);
    assert.deepEqual(errors, {});
    assert.deepEqual(input, {
      name: "Core ledger",
      description: "",
      criticality: "important",
      source: { connector_id: "ora-fin" },
      tables: [{ name: "gl_entries", schema: "FIN", key_columns: ["entry_id", "line"] }],
      exit_window_hours: 720,
      window_basis: "",
      regulatory_dates: [{ date: "2027-01-12", label: "Data Act" }],
      rehearsal_every_days: 90,
      rehearsal_schedule: { cron: "0 2 1 * *", timezone: "UTC" },
      extract_format: "jsonl",
      required_roles: ["engineering", "data_owner", "compliance"],
    });
  });

  it("refuses a bad cron, a dateless label and no sign-off roles", () => {
    const { errors } = validateDraft({ ...emptyDraft(), name: "x", connectorId: "c", tables: [{ name: "t", schema: "", keys: "" }], scheduleCron: "daily", regulatoryDates: [{ date: "", label: "x" }], requiredRoles: [] });
    assert.match(errors.schedule, /five-field cron/);
    assert.match(errors.regulatoryDates, /valid date/);
    assert.match(errors.roles, /at least one/);
  });

  it("round-trips a system into an edit draft", () => {
    const draft = draftFromSystem(systemFixture());
    assert.equal(draft.windowValue, "30");
    assert.equal(draft.windowUnit, "days");
    assert.equal(draft.tables[2].keys, "invoice_id, line_no");
    assert.equal(validateDraft(draft).input?.exit_window_hours, 720);
  });
});

describe("deep links and errors", () => {
  it("reads the copilot's link and writes it back", () => {
    assert.deepEqual(exitFocusFromHash("#/exit?system=exs_1&rehearsal=exr_2"), { system: "exs_1", rehearsal: "exr_2" });
    assert.deepEqual(exitFocusFromHash("#/exit"), {});
    assert.equal(exitFocusFromHash("#/jobs?jobId=1"), null);
    assert.equal(exitHash({ rehearsal: "exr_2" }), "#/exit?rehearsal=exr_2");
    assert.equal(exitHash(), "#/exit");
  });

  it("keeps the API's reason and advises by status", () => {
    assert.match(exitErrorMessage(new ApiError("A rehearsal of this system is running", 409), "x"), /running\. The page has been refreshed/);
    assert.match(exitErrorMessage(new ApiError("criticality must be one of", 422), "x"), /^The request was refused: criticality/);
    assert.match(exitErrorMessage(new ApiError("Exit system not found", 404), "x"), /Not found in this workspace/);
    assert.match(exitErrorMessage(new ApiError("boom", 503), "Could not load."), /HTTP 503/);
  });
});
