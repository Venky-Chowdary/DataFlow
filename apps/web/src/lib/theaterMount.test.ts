/**
 * Theater mount policy — completed Gate-8 must stay on the operator path.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  canPersistStudioSchedule,
  keepTheaterMountedOnStatus,
  routeBarLiveWhileWriting,
  showStudioResultDashboard,
} from "./theaterMount.js";

describe("keepTheaterMountedOnStatus", () => {
  it("keeps Theater on every terminal status, including success", () => {
    assert.equal(keepTheaterMountedOnStatus("completed"), true);
    assert.equal(keepTheaterMountedOnStatus("completed_with_quarantine"), true);
    assert.equal(keepTheaterMountedOnStatus("failed"), true);
    assert.equal(keepTheaterMountedOnStatus("cancelled"), true);
  });

  it("does not treat an in-flight status as a keep-mounted terminal", () => {
    assert.equal(keepTheaterMountedOnStatus("running"), false);
    assert.equal(keepTheaterMountedOnStatus("pending"), false);
    assert.equal(keepTheaterMountedOnStatus(undefined), false);
  });
});

describe("routeBarLiveWhileWriting", () => {
  it("drops live once the write stops, even if Theater is still mounted", () => {
    assert.equal(routeBarLiveWhileWriting(true), true);
    assert.equal(routeBarLiveWhileWriting(false), false);
  });
});

describe("showStudioResultDashboard", () => {
  it("hides the dashboard while Theater owns the job id", () => {
    assert.equal(
      showStudioResultDashboard({ hasResult: true, theaterJobId: "job-1" }),
      false,
    );
  });

  it("shows the dashboard only when there is a result and no Theater job", () => {
    assert.equal(
      showStudioResultDashboard({ hasResult: true, theaterJobId: null }),
      true,
    );
    assert.equal(
      showStudioResultDashboard({ hasResult: false, theaterJobId: null }),
      false,
    );
  });
});

describe("canPersistStudioSchedule", () => {
  it("requires saved connectors on both ends of a database dest", () => {
    assert.equal(
      canPersistStudioSchedule({
        isConnectorSource: true,
        sourceConnectorId: "src-1",
        destKindMode: "database",
        destConnectorId: "dst-1",
      }),
      true,
    );
  });

  it("refuses a file source even when dest is a saved connector", () => {
    assert.equal(
      canPersistStudioSchedule({
        isConnectorSource: false,
        sourceConnectorId: "",
        destKindMode: "database",
        destConnectorId: "dst-1",
      }),
      false,
    );
  });

  it("refuses file-export dest and an unsaved dest connector", () => {
    assert.equal(
      canPersistStudioSchedule({
        isConnectorSource: true,
        sourceConnectorId: "src-1",
        destKindMode: "file_export",
        destConnectorId: "dst-1",
      }),
      false,
    );
    assert.equal(
      canPersistStudioSchedule({
        isConnectorSource: true,
        sourceConnectorId: "src-1",
        destKindMode: "database",
        destConnectorId: "",
      }),
      false,
    );
  });
});
