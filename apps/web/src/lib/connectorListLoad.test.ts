import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  connectorListErrorText,
  connectorListFailed,
  connectorListLoaded,
  connectorListStarted,
  connectorListView,
  initialConnectorList,
} from "./connectorListLoad.js";

/** D410: round 52, 2 of 5 loads of #/connectors showed "Connections (0) / No
 *  connections yet" although GET /connectors/saved returned 200 with all 10
 *  connectors, next to ERR_ABORTED requests in the same load. The loader kept
 *  only the newest request: when that one failed (aborted, timed out) the
 *  older successful answer had already been dropped as stale, the list stayed
 *  [], "ready" was set in finally, and the page drew the empty state. A
 *  failed list (both routes non-OK) was also returned as [] — empty again. */
const ROWS = [{ id: "c1" }, { id: "c2" }];

describe("connector list load", () => {
  it("keeps a successful answer when a newer request for the same workspace fails", () => {
    let s = connectorListStarted(initialConnectorList<{ id: string }>(), { gen: 1, workspaceId: "ws" });
    s = connectorListStarted(s, { gen: 2, workspaceId: "ws" });
    s = connectorListFailed(s, { gen: 2, workspaceId: "ws", message: "Request timed out" });
    // Still waiting on gen 1: never the empty state while a request may answer.
    assert.equal(connectorListView(s).kind, "loading");
    s = connectorListLoaded(s, { gen: 1, workspaceId: "ws", rows: ROWS });
    assert.deepEqual(connectorListView(s), { kind: "list", rows: ROWS });
  });

  it("an aborted request after a successful one never empties the list", () => {
    let s = connectorListStarted(initialConnectorList<{ id: string }>(), { gen: 1, workspaceId: "ws" });
    s = connectorListLoaded(s, { gen: 1, workspaceId: "ws", rows: ROWS });
    s = connectorListStarted(s, { gen: 2, workspaceId: "ws" });
    s = connectorListFailed(s, { gen: 2, workspaceId: "ws", message: "Request timed out" });
    assert.deepEqual(connectorListView(s), { kind: "list", rows: ROWS });
  });

  it("an older answer never overwrites a newer successful one", () => {
    let s = connectorListStarted(initialConnectorList<{ id: string }>(), { gen: 1, workspaceId: "ws" });
    s = connectorListStarted(s, { gen: 2, workspaceId: "ws" });
    s = connectorListLoaded(s, { gen: 2, workspaceId: "ws", rows: ROWS });
    s = connectorListLoaded(s, { gen: 1, workspaceId: "ws", rows: [] });
    assert.deepEqual(connectorListView(s), { kind: "list", rows: ROWS });
  });

  it("shows the empty state only after a successful empty answer", () => {
    let s = connectorListStarted(initialConnectorList<{ id: string }>(), { gen: 1, workspaceId: "ws" });
    assert.equal(connectorListView(s).kind, "loading");
    s = connectorListLoaded(s, { gen: 1, workspaceId: "ws", rows: [] });
    assert.equal(connectorListView(s).kind, "empty");
  });

  it("a failed list with nothing loaded is an error with its reason, not empty", () => {
    let s = connectorListStarted(initialConnectorList<{ id: string }>(), { gen: 1, workspaceId: "ws" });
    s = connectorListFailed(s, { gen: 1, workspaceId: "ws", message: "Could not load connections (HTTP 503)." });
    assert.deepEqual(connectorListView(s), { kind: "error", message: "Could not load connections (HTTP 503)" });
    // A retry goes back to loading, and its success replaces the error.
    s = connectorListStarted(s, { gen: 2, workspaceId: "ws" });
    assert.equal(connectorListView(s).kind, "loading");
    s = connectorListLoaded(s, { gen: 2, workspaceId: "ws", rows: ROWS });
    assert.equal(connectorListView(s).kind, "list");
  });

  it("switching workspace mid-load drops the old workspace's rows and answers", () => {
    let s = connectorListStarted(initialConnectorList<{ id: string }>(), { gen: 1, workspaceId: "a" });
    s = connectorListLoaded(s, { gen: 1, workspaceId: "a", rows: ROWS });
    s = connectorListStarted(s, { gen: 2, workspaceId: "b" });
    assert.equal(connectorListView(s).kind, "loading");
    // A late answer for workspace a is not shown under workspace b.
    s = connectorListLoaded(s, { gen: 1, workspaceId: "a", rows: ROWS });
    assert.equal(connectorListView(s).kind, "loading");
    s = connectorListLoaded(s, { gen: 2, workspaceId: "b", rows: [] });
    assert.equal(connectorListView(s).kind, "empty");
  });
});

describe("connectorListErrorText", () => {
  it("names the cause in the page's words", () => {
    assert.equal(connectorListErrorText("Request timed out"), "The request for your connections timed out");
    assert.equal(connectorListErrorText("Authentication required — sign in again to load connectors"),
      "Your session has ended. Sign in again to see your connections");
    assert.equal(connectorListErrorText(""), "The request for your connections failed");
  });
});
