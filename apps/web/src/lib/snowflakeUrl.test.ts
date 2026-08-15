/**
 * Run: npx --yes tsx --test apps/web/src/lib/snowflakeUrl.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { validateConnectorPayload } from "./connectorFormConfig.js";
import {
  SNOWFLAKE_HOST_ONLY_URL_MSG,
  isSnowflakeAccountHostOnly,
  normalizeSnowflakeAccount,
  parseSnowflakeUrl,
  validateSnowflakeConnectionString,
} from "./snowflakeUrl.js";

describe("snowflakeUrl", () => {
  it("treats a browser host as account-only", () => {
    const parsed = parseSnowflakeUrl("https://bq73198.snowflakecomputing.com");
    assert.deepEqual(parsed, { account: "bq73198" });
    assert.equal(isSnowflakeAccountHostOnly(parsed), true);
    assert.equal(validateSnowflakeConnectionString("https://bq73198.snowflakecomputing.com"), SNOWFLAKE_HOST_ONLY_URL_MSG);
  });

  it("keeps @ inside the password by splitting on the last @", () => {
    const parsed = parseSnowflakeUrl(
      "snowflake://VENKY170259:venkatesh@170259@bq73198/EMPLOYEE_DB/PUBLIC?warehouse=COMPUTE_WH&role=ACCOUNTADMIN",
    );
    assert.equal(parsed.account, "bq73198");
    assert.equal(parsed.user, "VENKY170259");
    assert.equal(parsed.password, "venkatesh@170259");
    assert.equal(parsed.database, "EMPLOYEE_DB");
    assert.equal(parsed.schema, "PUBLIC");
    assert.equal(parsed.warehouse, "COMPUTE_WH");
    assert.equal(parsed.role, "ACCOUNTADMIN");
    assert.equal(isSnowflakeAccountHostOnly(parsed), false);
    assert.equal(
      validateSnowflakeConnectionString(
        "snowflake://VENKY170259:venkatesh@170259@bq73198/EMPLOYEE_DB/PUBLIC?warehouse=COMPUTE_WH&role=ACCOUNTADMIN",
      ),
      null,
    );
  });

  it("decodes %40 in the password", () => {
    const parsed = parseSnowflakeUrl("snowflake://svc:p%40ss%40word@myorg-acct/ANALYTICS/PUBLIC");
    assert.equal(parsed.account, "myorg-acct");
    assert.equal(parsed.password, "p@ss@word");
  });

  it("strips privatelink and region hosts", () => {
    assert.equal(normalizeSnowflakeAccount("https://xy12345.us-east-1.snowflakecomputing.com"), "xy12345.us-east-1");
    assert.equal(normalizeSnowflakeAccount("myorg-acct.privatelink.snowflakecomputing.com:443"), "myorg-acct");
  });

  it("rejects a browser host in the connector form before Test", () => {
    const msg = validateConnectorPayload(
      "snowflake",
      { connection_string: "https://bq73198.snowflakecomputing.com" },
      "connection_string",
    );
    assert.equal(msg, SNOWFLAKE_HOST_ONLY_URL_MSG);
  });
});
