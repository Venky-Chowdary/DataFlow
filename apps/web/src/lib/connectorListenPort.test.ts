/**
 * Run: npx --yes tsx --test apps/web/src/lib/connectorListenPort.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { connectorListenPort, portInEndpointUrl } from "./connectorTypes.js";

describe("S3 listen port", () => {
  it("keeps a typed MinIO port instead of stamping 443", () => {
    assert.equal(connectorListenPort("s3", 9000, ""), 9000);
    assert.equal(connectorListenPort("minio", 9000, ""), 9000);
  });

  it("takes the port from an endpoint URL so the preview matches the dial", () => {
    assert.equal(portInEndpointUrl("http://minio:9000"), 9000);
    assert.equal(connectorListenPort("s3", 443, "http://minio:9000"), 9000);
    assert.equal(connectorListenPort("s3", 443, "https://play.min.io:9000"), 9000);
  });

  it("keeps 443 for real AWS when neither field names another port", () => {
    assert.equal(connectorListenPort("s3", 443, ""), 443);
    assert.equal(connectorListenPort("s3", 443, "https://s3.amazonaws.com"), 443);
    assert.equal(portInEndpointUrl("https://s3.amazonaws.com"), 0);
  });

  it("does not invent 443 for a database that left the port empty", () => {
    assert.equal(connectorListenPort("postgresql", 0, ""), 0);
    assert.equal(connectorListenPort("postgresql", 5432, ""), 5432);
  });
});
