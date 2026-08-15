/**
 * Run: npx --yes tsx --test apps/web/src/lib/sourceReadMode.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  bindNamesFromSql,
  dialectOffersProcedures,
  dialectOffersQuery,
  isCallableSourceMode,
  procedureStreamName,
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
  });
});
