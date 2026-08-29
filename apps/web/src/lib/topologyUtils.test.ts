/**
 * Run: npx --yes tsx --test apps/web/src/lib/topologyUtils.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { buildDataPlaneTopology, countSavedConnectionRoutes } from "./topologyUtils.js";
import type { TransferJob } from "./types.js";

function job(i: number, status = "completed"): TransferJob {
  return {
    _id: `j${i}`,
    status,
    created_at: "2026-01-01T00:00:00Z",
    source_type: "postgresql",
    source_name: "pg",
    destination_type: "mysql",
    destination_database: "warehouse",
    destination_collection: `overview_${status}_${i}`,
  } as TransferJob;
}

describe("Overview data plane does not invent a route per job collection", () => {
  it("collapses 50 job collections into one type-pair and zero saved-connection routes", () => {
    const jobs = Array.from({ length: 50 }, (_, i) => job(i));
    const topology = buildDataPlaneTopology([], jobs, []);
    assert.equal(topology.edges.length, 1, "one postgresql→mysql pair, not 50 collections");
    assert.equal(countSavedConnectionRoutes(topology), 0);
  });

  it("counts only edges whose both ends are saved connections", () => {
    const connectors = [
      { id: "src", name: "PG", type: "postgresql", status: "ok" },
      { id: "dst", name: "MySQL", type: "mysql", database: "warehouse", status: "ok" },
    ];
    const schedules = [
      {
        id: "s1",
        name: "nightly",
        enabled: true,
        source_connector_id: "src",
        dest_connector_id: "dst",
        interval: "0 2 * * *",
      },
    ];
    const topology = buildDataPlaneTopology(connectors as never, [job(1)], schedules as never);
    assert.equal(countSavedConnectionRoutes(topology), 1);
  });
});
