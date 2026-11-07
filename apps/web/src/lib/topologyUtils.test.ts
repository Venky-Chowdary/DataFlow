/**
 * Run: npx --yes tsx --test apps/web/src/lib/topologyUtils.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  buildDataPlaneTopology,
  countSavedConnectionRoutes,
  dataPlaneSubtitle,
  summarizeDataPlane,
} from "./topologyUtils.js";
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

  it("joins jobs by saved connector id so table names are not extra routes", () => {
    const connectors = [
      { id: "shop", name: "Shop MySQL", type: "mysql", database: "shop", status: "ok" },
      { id: "inv", name: "Inventory MySQL", type: "mysql", database: "defaultdb", status: "ok" },
      { id: "idle", name: "Unused", type: "postgresql", status: "ok", last_test_ok: false },
    ];
    const tables = ["qa_my_seed", "procedure_result", "generate_series"];
    const jobs = tables.map((table, i) => ({
      _id: `j${i}`,
      status: "completed",
      created_at: "2026-01-01T00:00:00Z",
      source_type: "mysql",
      source_name: table,
      source_connector_id: "shop",
      dest_connector_id: "inv",
      destination_type: "mysql",
      destination_database: "defaultdb",
      destination_collection: `t_${i}`,
    }));
    const topology = buildDataPlaneTopology(connectors as never, jobs as never, []);
    const summary = summarizeDataPlane(topology, connectors.length);
    assert.equal(summary.routeCount, 1, "three tables on one connector pair are one route");
    assert.equal(summary.savedRouteCount, 1);
    assert.equal(summary.sourceCount, 1);
    assert.equal(summary.destinationCount, 1);
    assert.equal(summary.inRouteCount, 2);
    assert.equal(summary.notInRouteCount, 1);
    assert.equal(dataPlaneSubtitle(summary).includes("no routes yet"), false);
    assert.match(dataPlaneSubtitle(summary), /1 route · 3 connections/);
  });

  it("says no routes only when the card also has no routes", () => {
    const connectors = [
      { id: "a", name: "A", type: "mysql", status: "ok" },
      { id: "b", name: "B", type: "postgresql", status: "ok" },
    ];
    const empty = summarizeDataPlane(buildDataPlaneTopology(connectors as never, [], []), 2);
    assert.equal(empty.routeCount, 0);
    assert.equal(dataPlaneSubtitle(empty), "2 connections · no routes yet");

    const fileJob = {
      _id: "file-1",
      status: "completed",
      created_at: "2026-01-01T00:00:00Z",
      source_type: "csv",
      source_name: "upload.csv",
      destination_type: "mysql",
      destination_database: "warehouse",
      destination_collection: "landed",
    };
    const observed = summarizeDataPlane(
      buildDataPlaneTopology(connectors as never, [fileJob as never], []),
      2,
    );
    assert.equal(observed.routeCount, 1);
    assert.equal(observed.savedRouteCount, 0);
    assert.equal(dataPlaneSubtitle(observed).includes("no routes yet"), false);
    assert.match(dataPlaneSubtitle(observed), /1 route · 0 between saved connections/);
  });

  it("a paused schedule still shows the route its jobs already ran", () => {
    const connectors = [
      { id: "shop", name: "Shop", type: "mysql", status: "ok" },
      { id: "inv", name: "Inventory", type: "mysql", database: "defaultdb", status: "ok" },
    ];
    const schedules = [{
      id: "paused",
      name: "mahi",
      enabled: false,
      source_connector_id: "shop",
      dest_connector_id: "inv",
      interval: "0 2 * * *",
    }];
    const jobs = [{
      _id: "ran",
      status: "failed",
      created_at: "2026-01-01T00:00:00Z",
      source_type: "mysql",
      source_name: "qa_my_seed",
      source_connector_id: "shop",
      dest_connector_id: "inv",
      destination_type: "mysql",
      destination_database: "defaultdb",
      destination_collection: "mahi",
    }];
    const summary = summarizeDataPlane(
      buildDataPlaneTopology(connectors as never, jobs as never, schedules as never),
      2,
    );
    assert.equal(summary.savedRouteCount, 1);
    assert.equal(summary.routeCount, 1);
  });

  it("zero connections and zero routes use the empty prompt", () => {
    const summary = summarizeDataPlane(buildDataPlaneTopology([], [], []), 0);
    assert.equal(dataPlaneSubtitle(summary), "Connect sources and destinations to map your topology");
  });
});
