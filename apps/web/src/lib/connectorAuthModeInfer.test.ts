// Auth-mode prefill for the connector edit form — the d878f487 failure shape:
// a stored masked private_key must not pre-select key-pair when a password/PAT
// is present (backend refuses the silent key-pair route as of the auth fix).
import assert from "node:assert/strict";
import { test } from "node:test";
import { inferConnectorAuthMode } from "./connectorFormConfig.js";

test("snowflake: masked stray key + password does not infer key_pair", () => {
  assert.equal(
    inferConnectorAuthMode(
      { private_key: "****", password: "****" },
      "snowflake",
    ),
    "user_pass",
  );
  assert.equal(
    inferConnectorAuthMode(
      { private_key: "truncated-no-pem", password: "****" },
      "snowflake",
    ),
    "user_pass",
  );
});

test("snowflake: key-only record still infers key_pair", () => {
  assert.equal(
    inferConnectorAuthMode({ private_key: "****" }, "snowflake"),
    "key_pair",
  );
});

test("snowflake: stored auth_mode always wins", () => {
  assert.equal(
    inferConnectorAuthMode(
      { auth_mode: "pat", private_key: "****", password: "****" },
      "snowflake",
    ),
    "pat",
  );
});

test("non-snowflake inference unchanged", () => {
  assert.equal(inferConnectorAuthMode(null, "postgresql"), "user_pass");
  assert.equal(inferConnectorAuthMode({ api_key: "k" }, "stripe"), "api_key");
  assert.equal(inferConnectorAuthMode({}, "csv"), "file_path");
  assert.equal(
    inferConnectorAuthMode({ connection_string: "x" }, "postgresql"),
    "connection_string",
  );
});
