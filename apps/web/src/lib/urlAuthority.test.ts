/**
 * Run: npx --yes tsx --test apps/web/src/lib/urlAuthority.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { getAuthModes, validateConnectorPayload } from "./connectorFormConfig.js";
import { looksLikeUserinfoHost, parseUrlAuthority } from "./urlAuthority.js";

describe("urlAuthority", () => {
  it("keeps @ inside a SQL password by splitting on the last @", () => {
    const parsed = parseUrlAuthority("postgresql://postgres:p@ss@tokaido.proxy.rlwy.net:27396/railway");
    assert.equal(parsed.host, "tokaido.proxy.rlwy.net");
    assert.equal(parsed.user, "postgres");
    assert.equal(parsed.password, "p@ss");
    assert.equal(parsed.port, 27396);
    assert.equal(looksLikeUserinfoHost("postgres:p@ss@tokaido.proxy.rlwy.net:27396/railway"), true);
  });

  it("parses Mongo and SFTP the same way", () => {
    const mongo = parseUrlAuthority("mongodb://mongo:p@ss@cluster0.mongodb.net/app");
    assert.equal(mongo.host, "cluster0.mongodb.net");
    assert.equal(mongo.password, "p@ss");
    const sftp = parseUrlAuthority("sftp://alice:secr@t@ftp.example.com:2222/data/file.csv");
    assert.equal(sftp.host, "ftp.example.com");
    assert.equal(sftp.password, "secr@t");
    assert.equal(sftp.port, 2222);
  });

  it("exposes Snowflake PAT as a first-class auth mode", () => {
    const modes = getAuthModes("snowflake").map((m) => m.value);
    assert.deepEqual(modes.includes("pat"), true);
    assert.equal(
      validateConnectorPayload("snowflake", { host: "bq73198", username: "SVC", password: "token" }, "pat"),
      null,
    );
    assert.equal(
      validateConnectorPayload("snowflake", { host: "bq73198", username: "SVC" }, "pat"),
      "Programmatic access token is required.",
    );
  });
});
