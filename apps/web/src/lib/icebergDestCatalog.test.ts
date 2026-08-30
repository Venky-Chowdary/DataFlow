import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { icebergDestExtra, inferIcebergCatalogKind } from "./icebergDestCatalog.js";

describe("iceberg dest catalog extra", () => {
  it("infers REST from catalog URI and explicit nessie", () => {
    assert.equal(inferIcebergCatalogKind("http://127.0.0.1:8181"), "rest");
    assert.equal(inferIcebergCatalogKind("https://lake.example/ws"), "rest");
    assert.equal(inferIcebergCatalogKind("iceberg+rest://lake:8181"), "rest");
    assert.equal(inferIcebergCatalogKind("/data/iceberg-warehouse", "nessie"), "rest");
    assert.equal(inferIcebergCatalogKind("/data/iceberg-warehouse"), "filesystem");
    assert.equal(inferIcebergCatalogKind("http://lake", "filesystem"), "filesystem");
  });

  it("stamps leftover MERGE extra for REST warehouse, not Glue", () => {
    assert.deepEqual(icebergDestExtra("rest", "file:///tmp/wh"), {
      catalog_type: "rest",
      warehouse: "file:///tmp/wh",
    });
    assert.deepEqual(icebergDestExtra("filesystem"), { catalog_type: "filesystem" });
  });
});
