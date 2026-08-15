/**
 * Run: npx --yes tsx --test apps/web/src/lib/sourceReadMode.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  bindNamesFromSql,
  callableSourceExtra,
  dialectOffersProcedures,
  dialectOffersDestQuery,
  dialectOffersQuery,
  destWriteReady,
  isCallableDestMode,
  isCallableSourceMode,
  procedureStreamName,
  sourceExtractReady,
} from "./sourceReadMode.js";

describe("sourceReadMode", () => {
  it("offers procedures on SQL dialects, not Mongo", () => {
    assert.equal(dialectOffersProcedures("postgresql"), true);
    assert.equal(dialectOffersProcedures("sqlserver"), true);
    assert.equal(dialectOffersProcedures("mongodb"), false);
    assert.equal(dialectOffersProcedures("sqlite"), false);
    assert.equal(dialectOffersQuery("sqlite"), true);
    assert.equal(dialectOffersQuery("postgresql"), true);
    assert.equal(dialectOffersQuery("mongodb"), false);
    assert.equal(dialectOffersDestQuery("sqlite"), true);
    assert.equal(dialectOffersDestQuery("postgresql"), true);
    assert.equal(dialectOffersDestQuery("mongodb"), false);
  });

  it("extracts :name binds from CALL/SELECT text", () => {
    assert.deepEqual(bindNamesFromSql("CALL get_orders(:since, :limit)"), ["since", "limit"]);
    assert.deepEqual(bindNamesFromSql("SELECT * FROM t WHERE id = :id"), ["id"]);
    assert.deepEqual(bindNamesFromSql("CALL get_orders('2024-01-01')"), []);
  });

  it("names the stream from CALL / EXEC / bare ident", () => {
    assert.equal(procedureStreamName("CALL public.get_orders(:since)"), "get_orders");
    assert.equal(procedureStreamName("EXEC dbo.GetOrders"), "GetOrders");
    assert.equal(procedureStreamName("public.get_orders"), "get_orders");
    assert.equal(procedureStreamName(""), "procedure_result");
  });

  it("treats procedure and query as callable", () => {
    assert.equal(isCallableSourceMode("procedure"), true);
    assert.equal(isCallableSourceMode("query"), true);
    assert.equal(isCallableSourceMode("table"), false);
    assert.equal(isCallableDestMode("procedure"), true);
    assert.equal(isCallableDestMode("query"), true);
    assert.equal(isCallableDestMode("table"), false);
  });

  it("requires dest CALL/INSERT text before a dest write is ready", () => {
    assert.equal(destWriteReady({ destWriteMode: "table" }), true);
    assert.equal(destWriteReady({ destWriteMode: "procedure", destProcedureCall: "" }), false);
    assert.equal(destWriteReady({ destWriteMode: "procedure", destProcedureCall: "CALL upsert_order(:id)" }), true);
    assert.equal(destWriteReady({ destWriteMode: "query", destQuerySql: "   " }), false);
    assert.equal(destWriteReady({
      destWriteMode: "query",
      destQuerySql: "INSERT INTO orders (id) VALUES (:id)",
    }), true);
  });

  it("treats query/procedure SQL as the ready extract, not a table name", () => {
    assert.equal(sourceExtractReady({
      sourceKind: "database",
      sourceConnectorId: "pg-1",
      sourceReadMode: "query",
      procedureCall: "SELECT id, email FROM customers",
    }), true);
    assert.equal(sourceExtractReady({
      sourceKind: "database",
      sourceConnectorId: "pg-1",
      sourceReadMode: "query",
      procedureCall: "   ",
      sourceTable: "customers",
    }), false);
    assert.equal(sourceExtractReady({
      sourceKind: "database",
      sourceConnectorId: "pg-1",
      sourceReadMode: "procedure",
      procedureCall: "CALL public.get_orders()",
    }), true);
    assert.equal(sourceExtractReady({
      sourceKind: "database",
      sourceConnectorId: "pg-1",
      sourceReadMode: "table",
      sourceTable: "public.orders",
    }), true);
    assert.equal(sourceExtractReady({
      sourceKind: "database",
      sourceConnectorId: "pg-1",
      sourceReadMode: "table",
    }), false);
    assert.equal(sourceExtractReady({
      sourceKind: "file",
      parsed: true,
    }), true);
    assert.equal(sourceExtractReady({
      sourceKind: "cloud",
      sourceConnectorId: "s3-1",
      cloudPath: "s3://bucket/orders.csv",
    }), true);
    assert.equal(sourceExtractReady({
      sourceKind: "database",
      sourceReadMode: "query",
      procedureCall: "SELECT 1",
    }), false);
  });

  it("stamps Execute source_extra for CALL/SELECT and leaves tables alone", () => {
    assert.equal(callableSourceExtra("table", "orders"), undefined);
    assert.deepEqual(callableSourceExtra("procedure", "CALL get_orders(:since)", { since: "2024-01-01" }), {
      source_read_mode: "procedure",
      procedure_call: "CALL get_orders(:since)",
      procedure_params: { since: "2024-01-01" },
    });
    assert.deepEqual(callableSourceExtra("query", "SELECT * FROM get_orders()"), {
      source_read_mode: "query",
      source_query: "SELECT * FROM get_orders()",
    });
  });
});
