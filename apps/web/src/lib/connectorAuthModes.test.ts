/**
 * Run: npx --yes tsx --test apps/web/src/lib/connectorAuthModes.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { AUTH_MODE_DETAIL, getAuthModes, validateConnectorPayload } from "./connectorFormConfig.js";
import { TRANSFER_LIVE_TYPES } from "./connectorTypes.js";

const SNOWFLAKE_MODES = ["user_pass", "pat", "key_pair", "connection_string"];

describe("connector auth modes", () => {
  it("gives every transfer-live type at least one auth mode", () => {
    const missing: string[] = [];
    for (const type of TRANSFER_LIVE_TYPES) {
      const modes = getAuthModes(type);
      if (!modes.length) missing.push(type);
    }
    assert.deepEqual(missing, []);
  });

  it("documents every auth mode for the setup rail", () => {
    for (const mode of Object.keys(AUTH_MODE_DETAIL)) {
      assert.ok(AUTH_MODE_DETAIL[mode as keyof typeof AUTH_MODE_DETAIL].length > 12);
    }
  });

  it("lists Snowflake password, PAT, key-pair, and login URL", () => {
    assert.deepEqual(
      getAuthModes("snowflake").map((m) => m.value),
      SNOWFLAKE_MODES,
    );
  });

  it("rejects empty required secrets instead of sending them to the driver", () => {
    assert.equal(
      validateConnectorPayload("postgresql", { host: "db.example", port: 5432, username: "u" }, "user_pass"),
      "Username and password are required.",
    );
    assert.equal(
      validateConnectorPayload("snowflake", { host: "bq73198", username: "SVC" }, "pat"),
      "Programmatic access token is required.",
    );
    assert.equal(
      validateConnectorPayload(
        "snowflake",
        { connection_string: "https://bq73198.snowflakecomputing.com" },
        "connection_string",
      )?.includes("account host") || false,
      true,
    );
  });
});
