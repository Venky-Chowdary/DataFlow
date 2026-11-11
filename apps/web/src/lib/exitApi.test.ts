/**
 * Reversibility Proof API client against a mocked fetch: contract paths,
 * methods and JSON bodies; refusals keep their status and reason; a running
 * rehearsal comes back normalised.
 */
import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";
import { ApiError } from "./api.js";
import {
  declareExitSystem,
  deleteExitSystem,
  downloadExitEvidenceMarkdown,
  fetchExitEvidencePack,
  fetchExitReadiness,
  fetchExitRehearsal,
  fetchExitSystem,
  fetchExitSystems,
  fetchJobReversibility,
  fetchPlanReversibility,
  fetchSystemRehearsals,
  setPlanRequireReversibility,
  signExitRehearsal,
  startExitRehearsal,
  updateExitSystem,
  verifyExitEvidence,
} from "./exitApi.js";
import { realBoard, realPack, realPlanReversibility, realRunningRaw, realSigned, realVerify } from "../components/exit/realExitPayloads.js";

const realFetch = globalThis.fetch;
const realWindow = Object.getOwnPropertyDescriptor(globalThis, "window");
type Call = { url: string; method: string; body: string; headers: Headers };

function mockFetch(respond: (call: Call) => Response): Call[] {
  Object.defineProperty(globalThis, "window", {
    configurable: true,
    value: { setTimeout: globalThis.setTimeout, clearTimeout: globalThis.clearTimeout, dispatchEvent: () => true },
  });
  const calls: Call[] = [];
  globalThis.fetch = (async (url: string, init: RequestInit = {}) => {
    const call = { url: String(url), method: String(init.method || "GET"), body: String(init.body ?? ""), headers: new Headers(init.headers) };
    calls.push(call);
    return respond(call);
  }) as typeof fetch;
  return calls;
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), { status, headers: { "Content-Type": "application/json" } });
}

const path = (url: string) => url.replace(/^.*\/api\/v1/, "");

afterEach(() => {
  globalThis.fetch = realFetch;
  if (realWindow) Object.defineProperty(globalThis, "window", realWindow);
  else Reflect.deleteProperty(globalThis, "window");
});

describe("exit inventory client", () => {
  it("reads the board, the systems and one system at the contract paths", async () => {
    const calls = mockFetch((c) => (c.url.endsWith("/exit/systems") ? json({ systems: [] }) : json(realBoard)));
    const board = await fetchExitReadiness();
    assert.equal(board.kpis.systems, realBoard.kpis.systems);
    assert.deepEqual(await fetchExitSystems(), []);
    await fetchExitSystem("exs/1");
    assert.deepEqual(calls.map((c) => [c.method, path(c.url)]), [
      ["GET", "/exit/readiness"],
      ["GET", "/exit/systems"],
      ["GET", "/exit/systems/exs%2F1"],
    ]);
  });

  it("declares, updates with the expected version, and deletes", async () => {
    const calls = mockFetch((c) => (c.method === "DELETE" ? new Response(null, { status: 204 }) : json({ id: "exs_1", name: "Core ledger" }, c.method === "POST" ? 201 : 200)));
    const input = {
      name: "Core ledger", description: "", criticality: "critical" as const, source: { connector_id: "c1" },
      tables: [{ name: "orders", schema: null, key_columns: [] }], exit_window_hours: 720, window_basis: "",
      regulatory_dates: [], rehearsal_every_days: 90, rehearsal_schedule: null, extract_format: "jsonl" as const,
      required_roles: ["engineering"],
    };
    await declareExitSystem(input);
    await updateExitSystem("exs_1", { name: "Ledger" }, 3);
    await updateExitSystem("exs_1", { name: "Ledger" });
    await deleteExitSystem("exs_1");
    assert.deepEqual(calls.map((c) => [c.method, path(c.url)]), [
      ["POST", "/exit/systems"],
      ["PATCH", "/exit/systems/exs_1"],
      ["PATCH", "/exit/systems/exs_1"],
      ["DELETE", "/exit/systems/exs_1"],
    ]);
    assert.deepEqual(JSON.parse(calls[0].body), input);
    assert.equal(calls[1].body, '{"name":"Ledger","expected_version":3}');
    assert.equal(calls[2].body, '{"name":"Ledger"}');
    for (const c of calls.slice(0, 3)) assert.equal(c.headers.get("Content-Type"), "application/json");
  });

  it("keeps the status and the API's reason on a refusal", async () => {
    mockFetch(() => json({ detail: "A rehearsal of this system is running; wait for it to finish." }, 409));
    await assert.rejects(deleteExitSystem("exs_1"), (err: unknown) => {
      assert.ok(err instanceof ApiError);
      assert.equal(err.status, 409);
      assert.match(err.message, /rehearsal of this system is running/);
      return true;
    });
    mockFetch(() => json({ detail: "criticality must be one of critical, important, standard" }, 422));
    await assert.rejects(fetchExitSystem("exs_1"), (err: unknown) => err instanceof ApiError && err.status === 422);
  });
});

describe("rehearsal client", () => {
  it("starts a rehearsal and normalises the running answer", async () => {
    const calls = mockFetch(() => json(realRunningRaw, 202));
    const r = await startExitRehearsal("exs_1");
    assert.equal(calls[0].method, "POST");
    assert.equal(path(calls[0].url), "/exit/systems/exs_1/rehearsals");
    assert.equal(r.verdict, "running");
    assert.equal(r.tables[0].key_set.match, null);
    assert.equal(r.tables[0].value_parity.method, "none");
  });

  it("lists rehearsals with a clamped limit and reads one", async () => {
    const calls = mockFetch((c) => (c.url.includes("?limit=") ? json({ rehearsals: [], trend: { points: [], growth_pct_rows: null, projection: null } }) : json(realSigned)));
    await fetchSystemRehearsals("exs_1", 500);
    await fetchSystemRehearsals("exs_1", 0);
    const r = await fetchExitRehearsal("exr_1");
    assert.equal(r.signoff.status, "signed");
    assert.deepEqual(calls.map((c) => path(c.url)), [
      "/exit/systems/exs_1/rehearsals?limit=100",
      "/exit/systems/exs_1/rehearsals?limit=20",
      "/exit/rehearsals/exr_1",
    ]);
  });

  it("signs with the role, the trimmed note and the version decided on", async () => {
    const calls = mockFetch(() => json(realSigned));
    await signExitRehearsal("exr_1", "compliance", "  checked  ", 6);
    await signExitRehearsal("exr_1", "engineering");
    assert.equal(path(calls[0].url), "/exit/rehearsals/exr_1/signoff");
    assert.equal(calls[0].body, '{"role":"compliance","note":"checked","expected_version":6}');
    assert.equal(calls[1].body, '{"role":"engineering"}');
  });

  it("exports the Markdown report and the signed pack, which is 409 until signed", async () => {
    const calls = mockFetch((c) => (c.url.includes("format=markdown") ? new Response("# Exit Evidence", { status: 200 }) : json(realPack)));
    assert.equal(await (await downloadExitEvidenceMarkdown("exr_1")).text(), "# Exit Evidence");
    const pack = await fetchExitEvidencePack("exr_1");
    assert.equal(pack.kind, "exit_evidence");
    assert.deepEqual(calls.map((c) => path(c.url)), [
      "/exit/rehearsals/exr_1/evidence?format=markdown",
      "/exit/rehearsals/exr_1/evidence?format=json",
    ]);
    mockFetch(() => json({ detail: "The rehearsal is not signed yet." }, 409));
    await assert.rejects(fetchExitEvidencePack("exr_1"), (err: unknown) => err instanceof ApiError && err.status === 409);
  });

  it("verifies a pack, wrapping a bare pack and passing a {pack} file as it is", async () => {
    const calls = mockFetch(() => json(realVerify));
    const result = await verifyExitEvidence(realPack);
    assert.equal(result.anchor.status, "not_anchored");
    await verifyExitEvidence({ pack: realPack });
    assert.equal(path(calls[0].url), "/exit/evidence/verify");
    assert.equal(calls[0].method, "POST");
    assert.deepEqual(Object.keys(JSON.parse(calls[0].body)), ["pack"]);
    assert.deepEqual(JSON.parse(calls[1].body), JSON.parse(calls[0].body));
  });
});

describe("reversibility client", () => {
  it("reads the plan's and the job's report", async () => {
    const calls = mockFetch(() => json(realPlanReversibility));
    const report = await fetchPlanReversibility("plan 1");
    assert.equal(report.gate.id, "g24_reversibility");
    await fetchJobReversibility("job-31");
    assert.deepEqual(calls.map((c) => path(c.url)), ["/transfer/plans/plan%201/reversibility", "/jobs/job-31/reversibility"]);
  });

  it("sets the plan policy with a merge PATCH", async () => {
    const calls = mockFetch(() => json({ plan: {} }));
    await setPlanRequireReversibility("plan-1", true);
    assert.equal(calls[0].method, "PATCH");
    assert.equal(path(calls[0].url), "/transfer/plans/plan-1");
    assert.equal(calls[0].body, '{"policies":{"require_reversibility":true}}');
  });
});
