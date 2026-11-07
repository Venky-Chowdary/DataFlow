/**
 * Contracts list + drawer render the server's status/version and real columns.
 *
 * QA (production 557f556c): after a 200 Sign (status=signed, version 3) the list
 * and drawer still read "Draft v1 (Signed 0)" — even after a reload — and the
 * Columns tab showed col_0..col_7. API side: apps/api/tests/test_r7_contract_status_read_model_e2e.py.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import type { DataContractSummary } from "./api.js";
import {
  applyContractResponse,
  contractColumnRows,
  contractStatusCounts,
  contractStatusLabel,
  reconcileContracts,
} from "./contractReadModel.js";
import { ContractColumnsTable, ContractStatusFacts } from "../components/contracts/ContractFacts.js";

const COLUMNS = [
  ["order_id", "INTEGER"],
  ["customer_name", "VARCHAR(120)"],
  ["région", "VARCHAR(40)"],
  ["amount", "NUMERIC(12,2)"],
  ["status", "VARCHAR(20)"],
  ["created_at", "TIMESTAMP"],
  ["notes", "TEXT"],
  ["is_paid", "BOOLEAN"],
] as const;

function contract(over: Partial<DataContractSummary> = {}): DataContractSummary {
  return {
    id: "dfc-r7",
    name: "orders → orders_copy",
    version: 1,
    status: "draft",
    source: { format: "postgresql", table: "orders" },
    destination: { format: "postgresql", table: "orders_copy" },
    // The shape the API stores and returns for a column rule.
    columns: COLUMNS.map(([name, type]) => ({
      source_name: name,
      target_name: name,
      source_type: type,
      target_type: type,
      transform: null,
      nullable: true,
      primary_key: name === "order_id",
    })),
    mappings: COLUMNS.map(([name]) => ({ source: name, target: name })),
    quality_rules: [],
    strict: true,
    created_at: "2026-10-30T10:00:00+00:00",
    updated_at: "2026-10-30T10:00:00+00:00",
    metadata: {},
    ...over,
  };
}

describe("contract read model (list + drawer)", () => {
  it("a stale Save-as-contract copy never beats the server's signed row", () => {
    const savedDraft = contract(); // sessionStorage copy from Save as contract
    const server = contract({
      status: "signed",
      version: 3,
      status_label: "Signed v3",
      updated_at: "2026-10-30T10:05:00+00:00",
    });
    const { rows, dropOptimistic } = reconcileContracts([server], savedDraft);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].status, "signed");
    assert.equal(rows[0].version, 3);
    assert.equal(dropOptimistic, true, "the stale copy must be cleared once the server lists it");
    assert.equal(contractStatusLabel(rows[0]), "Signed v3");
    const counts = contractStatusCounts(rows);
    assert.equal(counts.signed, 1);
    assert.equal(counts.draft, 0);
  });

  it("keeps a just-saved contract the server list has not caught up with", () => {
    const savedDraft = contract({ id: "dfc-new" });
    const { rows, dropOptimistic } = reconcileContracts([contract({ id: "dfc-old" })], savedDraft);
    assert.deepEqual(rows.map((c) => c.id), ["dfc-new", "dfc-old"]);
    assert.equal(dropOptimistic, false);
  });

  it("applies the Sign response in place", () => {
    const rows = [contract({ id: "a" }), contract()];
    const signed = contract({ status: "signed", version: 3 });
    const next = applyContractResponse(rows, signed);
    assert.deepEqual(next.map((c) => c.id), ["a", "dfc-r7"]);
    assert.equal(next[1].status, "signed");
    assert.equal(contractStatusLabel(next[1]), "Signed v3");
  });

  it("derives the label like the server when status_label is absent", () => {
    assert.equal(contractStatusLabel({ status: "draft", version: 1 }), "Draft v1");
    assert.equal(contractStatusLabel({ status: "SIGNED", version: 2 }), "Signed v2");
    assert.equal(contractStatusLabel({ status: "", version: 0 }), "Draft v1");
  });

  it("column rows come from the bound schema, NULL/blank safe", () => {
    const rows = contractColumnRows(contract());
    assert.deepEqual(rows.map((r) => r.name), COLUMNS.map(([n]) => n));
    assert.deepEqual(rows.map((r) => r.type), COLUMNS.map(([, t]) => t));
    const odd = contractColumnRows({
      columns: [{ name: "  ", source_name: "src_only", target_name: "", source_type: "TEXT" }, { nullable: null }],
    });
    assert.equal(odd[0].name, "src_only");
    assert.equal(odd[0].type, "TEXT");
    assert.equal(odd[1].name, "");
    assert.equal(odd[1].nullable, null);
  });

  it("the drawer Columns tab renders real column names, not col_0..col_7", () => {
    const html = renderToStaticMarkup(<ContractColumnsTable contract={contract()} />);
    for (const [name, type] of COLUMNS) {
      assert.ok(html.includes(`>${name}<`), `missing column ${name}`);
      assert.ok(html.includes(`>${type}<`), `missing type ${type}`);
    }
    assert.doesNotMatch(html, /col_\d/);
  });

  it("the drawer facts show the server's status and version", () => {
    const html = renderToStaticMarkup(
      <ContractStatusFacts contract={contract({ status: "signed", version: 3 })} />,
    );
    assert.match(html, /<strong>Signed<\/strong>/);
    assert.match(html, /<strong>v3<\/strong>/);
    assert.doesNotMatch(html, /Draft/);
  });
});
