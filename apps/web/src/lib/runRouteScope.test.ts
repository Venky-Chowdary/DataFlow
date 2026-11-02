import assert from "node:assert/strict";
import { test } from "node:test";
import {
  describeDestRoute,
  destRouteKey,
  runResultDescribesRoute,
  type DestRoute,
} from "./runRouteScope";

const DB_ROUTE: DestRoute = {
  destKindMode: "database",
  destType: "postgresql",
  targetDb: "dataflow",
  destSchema: "public",
  targetCollection: "orders",
  exportFormat: "json",
  destOutputPath: "",
};

test("the same destination is the same route", () => {
  assert.equal(destRouteKey(DB_ROUTE), destRouteKey({ ...DB_ROUTE }));
  assert.equal(
    destRouteKey(DB_ROUTE),
    destRouteKey({ ...DB_ROUTE, targetCollection: " orders " }),
  );
});

test("retargeting the table is a different route", () => {
  for (const changed of [
    { targetCollection: "orders_v2" },
    { targetDb: "other" },
    { destSchema: "staging" },
    { destType: "mysql" },
  ] as Partial<DestRoute>[]) {
    assert.notEqual(
      destRouteKey(DB_ROUTE),
      destRouteKey({ ...DB_ROUTE, ...changed }),
      `changing ${Object.keys(changed)[0]} must change the route`,
    );
  }
});

test("a file export is identified by format and path, not by the table fields", () => {
  const exportRoute: DestRoute = {
    ...DB_ROUTE,
    destKindMode: "file_export",
    exportFormat: "csv",
    destOutputPath: "C:/out/orders.csv",
  };
  assert.equal(
    destRouteKey(exportRoute),
    destRouteKey({ ...exportRoute, targetDb: "ignored", targetCollection: "ignored" }),
  );
  assert.notEqual(
    destRouteKey(exportRoute),
    destRouteKey({ ...exportRoute, exportFormat: "parquet" }),
  );
  assert.notEqual(
    destRouteKey(exportRoute),
    destRouteKey({ ...exportRoute, destOutputPath: "C:/out/other.csv" }),
  );
  assert.notEqual(destRouteKey(exportRoute), destRouteKey(DB_ROUTE));
});

test("a run is stale exactly when its route no longer matches", () => {
  const ran = destRouteKey(DB_ROUTE);
  assert.equal(runResultDescribesRoute(ran, ran), true);
  assert.equal(
    runResultDescribesRoute(ran, destRouteKey({ ...DB_ROUTE, targetCollection: "later" })),
    false,
  );
});

test("a run that was never executed here is not called stale", () => {
  assert.equal(runResultDescribesRoute(null, destRouteKey(DB_ROUTE)), true);
});

test("a recorded table route is named the way the result dashboard names it", () => {
  assert.equal(describeDestRoute(destRouteKey(DB_ROUTE)), "public.orders");
  assert.equal(
    describeDestRoute(destRouteKey({ ...DB_ROUTE, destSchema: "" })),
    "dataflow.orders",
  );
});

test("a recorded export route is named by format and path", () => {
  assert.equal(
    describeDestRoute(destRouteKey({
      ...DB_ROUTE,
      destKindMode: "file_export",
      exportFormat: "csv",
      destOutputPath: "C:/out/orders.csv",
    })),
    "CSV export · C:/out/orders.csv",
  );
});

test("an absent or unreadable route is named nothing rather than guessed", () => {
  assert.equal(describeDestRoute(null), "");
  assert.equal(describeDestRoute("not json"), "");
});
