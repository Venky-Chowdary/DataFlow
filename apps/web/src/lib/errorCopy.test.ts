/**
 * errorCopy reads the API's error envelope by code (services/error_taxonomy.py).
 * ERR-36..ERR-40: no page guesses a cause from error text, the reference is
 * split out for its own chip, and a 502/504 names the caller's operation.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { gatewayTimeoutCopy, operationName } from "./api";
import {
  errorCodeOf,
  errorTitle,
  isCapacityError,
  isConnectionError,
  isDuplicateKeyError,
  isMissingObjectError,
  isUploadExpired,
  jobFailureCopy,
  readErrorEnvelope,
  splitReference,
} from "./errorCopy";
import { inferTransferFailureHint, isDestinationCapacityFailure } from "./transferFailure";

const ENVELOPE = {
  error: {
    code: "access.source_read_denied",
    title: "Source read not permitted",
    message: "Datawrap couldn't read table \"orders\" on the source: the connection's database role doesn't have SELECT on it.",
    action: { label: "Grant SELECT on the table to the source connection's role, then run again.", ui: "open_source", api: "" },
    object: { table: "orders", side: "source" },
    retriable: false,
    reference: "8f2c4d1e9a7b4c3d",
  },
  detail: "Datawrap couldn't read table \"orders\" on the source … Reference: 8f2c4d1e9a7b4c3d",
};

describe("readErrorEnvelope", () => {
  it("reads code, message, the one action and the reference", () => {
    const copy = readErrorEnvelope(ENVELOPE);
    assert.ok(copy);
    assert.equal(copy.code, "access.source_read_denied");
    assert.equal(copy.title, "Source read not permitted");
    assert.match(copy.message, /SELECT/);
    assert.equal(copy.action?.ui, "open_source");
    assert.equal(copy.reference, "8f2c4d1e9a7b4c3d");
  });

  it("is null for a body with no coded envelope (legacy {detail})", () => {
    assert.equal(readErrorEnvelope({ detail: "Connector not found" }), null);
    assert.equal(readErrorEnvelope({ error: "Internal Server Error", detail: "x" }), null);
    assert.equal(readErrorEnvelope(null), null);
  });
});

describe("splitReference", () => {
  it("splits the trailing reference off for its own chip, never clipping it", () => {
    const long = `${"word ".repeat(80)}Reference: 8eb1d0c5a78649928d6728611359fb87`;
    const { text, reference } = splitReference(long);
    assert.equal(reference, "8eb1d0c5a78649928d6728611359fb87");
    assert.ok(!text.includes("Reference"));
  });

  it("leaves text without a reference alone", () => {
    assert.deepEqual(splitReference("Connected"), { text: "Connected", reference: "" });
  });
});

describe("decisions are made on the code, not the text", () => {
  it("a connection failure whose message says 'does not exist' is not a missing table (ERR-37)", () => {
    assert.equal(isMissingObjectError("auth.password_rejected"), false);
    assert.equal(isMissingObjectError("object.database_not_found"), false);
    assert.equal(isMissingObjectError("object.table_not_found"), true);
    assert.equal(isConnectionError("object.database_not_found"), true);
    assert.equal(isConnectionError("network.refused"), true);
  });

  it("a retry refusal that mentions Transfer Studio is not an expired upload unless coded so (ERR-38)", () => {
    assert.equal(isUploadExpired(errorCodeOf(new Error("Retry failed. Open Transfer Studio for details."))), false);
    const coded = Object.assign(new Error("This job's uploaded file is no longer on the server."), {
      code: "object.upload_expired",
    });
    assert.equal(isUploadExpired(errorCodeOf(coded)), true);
  });

  it("capacity and duplicate keys come from codes", () => {
    assert.equal(isCapacityError("capacity.disk_full"), true);
    assert.equal(isCapacityError("value.too_long"), false);
    assert.equal(isDuplicateKeyError("value.duplicate_key"), true);
    assert.equal(isDuplicateKeyError("duplicate_primary_key"), true);
    assert.equal(errorTitle("network.refused"), "Can't reach the database");
  });
});

describe("transferFailure reads the job's classification only (ERR-6)", () => {
  it("does not title a PostgreSQL schema error as Snowflake from its text", () => {
    assert.equal(inferTransferFailureHint('schema "public" does not exist'), null);
    const hint = inferTransferFailureHint(
      'Schema "public" doesn\'t exist on the destination.',
      "object.schema_not_found",
      "Schema not found",
      "Check the schema name on the connection (names can be case-sensitive), or create the schema.",
      "high",
    );
    assert.equal(hint?.title, "Schema not found");
    assert.doesNotMatch(String(hint?.title), /Snowflake/);
  });

  it("decides capacity on the code, not on 'no space left' text", () => {
    assert.equal(isDestinationCapacityFailure(null, "No space left on device"), false);
    assert.equal(
      isDestinationCapacityFailure({ code: "capacity.disk_full", title: "t", fix: "f", confidence: "high" }),
      true,
    );
  });
});

describe("jobFailureCopy", () => {
  it("shows a classified job's message once and its action separately", () => {
    const copy = jobFailureCopy({
      _id: "6af7c5",
      error: "A row has no value for required column \"v\". Map a source column with values to it.",
      error_code: "value.not_null",
      error_title: "Required value missing",
      error_details: {
        message: "A row has no value for required column \"v\".",
        action: { label: "Map a source column with values to it.", ui: "open_map", api: "" },
      },
    });
    assert.equal(copy.message, "A row has no value for required column \"v\".");
    assert.equal(copy.action?.ui, "open_map");
    assert.equal(copy.reference, "6af7c5");
  });
});

describe("502/504 names the caller's operation (ERR-36)", () => {
  it("says Validate only for Validate", () => {
    assert.match(gatewayTimeoutCopy(504, "Validate"), /while Validate was running/);
    const query = gatewayTimeoutCopy(504, operationName("Query failed"));
    assert.doesNotMatch(query, /Validate/);
    assert.match(query, /Query/);
    assert.equal(operationName("Failed to load jobs"), "load jobs");
  });
});
