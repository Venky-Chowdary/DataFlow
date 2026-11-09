/**
 * Saved file-connector Configure dialogs attach a file, not only a path box.
 * Run: npx --yes tsx --test apps/web/src/lib/connectorFileUpload.test.ts
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  connectorAcceptsUpload,
  connectorFieldsFromUpload,
  connectorUploadAccept,
  uploadedFileMatchesConnector,
} from "./connectorFileUpload.js";

const here = dirname(fileURLToPath(import.meta.url));

describe("connector file upload", () => {
  it("offers an upload control for CSV, TSV, Excel, JSON, and SQLite", () => {
    for (const type of ["csv", "tsv", "excel", "json", "sqlite"]) {
      assert.equal(connectorAcceptsUpload(type), true, type);
      assert.match(connectorUploadAccept(type), /\./);
    }
    assert.equal(connectorAcceptsUpload("postgresql"), false);
    assert.equal(connectorAcceptsUpload("elasticsearch"), false);
  });

  it("rejects a file whose profiled type is a different connector", () => {
    assert.equal(uploadedFileMatchesConnector("csv", "csv", "orders.csv"), true);
    assert.equal(uploadedFileMatchesConnector("tsv", "tsv", "orders.tsv"), true);
    assert.equal(uploadedFileMatchesConnector("json", "json", "orders.json"), true);
    assert.equal(uploadedFileMatchesConnector("excel", "excel", "orders.xlsx"), true);
    assert.equal(uploadedFileMatchesConnector("sqlite", "sqlite", "shop.sqlite"), true);
    assert.equal(uploadedFileMatchesConnector("csv", "excel", "orders.xlsx"), false);
    assert.equal(uploadedFileMatchesConnector("json", "csv", "orders.csv"), false);
  });

  it("fills the path field the connector Test already reads", () => {
    const file = connectorFieldsFromUpload({
      type: "csv",
      authMode: "file_path",
      path: "/data/uploads/abc_orders.csv",
      fileId: "abc",
    });
    assert.equal(file.connectionString, "/data/uploads/abc_orders.csv");
    assert.equal(file.uploadedFileId, "abc");
    assert.equal(file.database, undefined);

    const sqlite = connectorFieldsFromUpload({
      type: "sqlite",
      authMode: "user_pass",
      path: "/data/uploads/abc_shop.sqlite",
      fileId: "abc",
    });
    assert.equal(sqlite.database, "/data/uploads/abc_shop.sqlite");
    assert.equal(sqlite.connectionString, undefined);

    const sqliteUrl = connectorFieldsFromUpload({
      type: "sqlite",
      authMode: "connection_string",
      path: "/data/uploads/abc_shop.sqlite",
      fileId: "abc",
    });
    assert.equal(sqliteUrl.connectionString, "/data/uploads/abc_shop.sqlite");
  });

  it("renders a real file input on the Configure dialog", () => {
    const modal = readFileSync(join(here, "../components/ConnectorModal.tsx"), "utf8");
    const hidden = readFileSync(join(here, "../components/ui/HiddenFileInput.tsx"), "utf8");
    assert.match(modal, /id="df2-connector-file"/);
    assert.match(modal, /htmlFor="df2-connector-file"/);
    assert.match(modal, /connectorAcceptsUpload\(resolvedType\)/);
    assert.match(modal, /Drop your data file here/);
    assert.match(hidden, /type="file"/);
    assert.match(hidden, /className="df2-sr-only"/);
    assert.doesNotMatch(modal, /display:\s*none/);
  });
});
