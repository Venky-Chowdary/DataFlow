/**
 * Run: npx --yes tsx --test apps/web/src/lib/cdcExactlyOnce.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  CDC_DELIVERY_AT_LEAST_ONCE,
  CDC_DELIVERY_EXACTLY_ONCE,
  cdcDeliveryResultCopy,
  exactlyOnceWiredDest,
  jobStudioDeliveryGuarantee,
  namedCdcDeliveryGuarantee,
  studioDeliveryGuarantee,
} from "./cdcExactlyOnce.ts";

describe("cdcExactlyOnce", () => {
  it("defaults unknown tokens to at_least_once", () => {
    assert.equal(namedCdcDeliveryGuarantee(""), CDC_DELIVERY_AT_LEAST_ONCE);
    assert.equal(namedCdcDeliveryGuarantee("at-most-once"), CDC_DELIVERY_AT_LEAST_ONCE);
    assert.equal(namedCdcDeliveryGuarantee("exactly_once"), CDC_DELIVERY_EXACTLY_ONCE);
    assert.equal(namedCdcDeliveryGuarantee("eos"), CDC_DELIVERY_EXACTLY_ONCE);
  });

  it("transactional SQL dests are wired; files are not", () => {
    assert.equal(exactlyOnceWiredDest("sqlite"), true);
    assert.equal(exactlyOnceWiredDest("postgresql"), true);
    assert.equal(exactlyOnceWiredDest("mysql"), true);
    assert.equal(exactlyOnceWiredDest("sqlserver"), true);
    assert.equal(exactlyOnceWiredDest("amazon_rds_sql_server"), true);
    assert.equal(exactlyOnceWiredDest("csv"), false);
    assert.equal(exactlyOnceWiredDest("iceberg"), false);
  });

  it("refuses to persist exactly_once on non-CDC or append-only", () => {
    assert.equal(
      studioDeliveryGuarantee({
        syncMode: "full_refresh_overwrite",
        deliveryGuarantee: "exactly_once",
      }),
      CDC_DELIVERY_AT_LEAST_ONCE,
    );
    assert.equal(
      studioDeliveryGuarantee({
        syncMode: "cdc",
        deliveryGuarantee: "exactly_once",
        allowAppendOnly: true,
      }),
      CDC_DELIVERY_AT_LEAST_ONCE,
    );
    assert.equal(
      studioDeliveryGuarantee({
        syncMode: "cdc",
        deliveryGuarantee: "exactly_once",
        callableSource: true,
      }),
      CDC_DELIVERY_AT_LEAST_ONCE,
    );
    assert.equal(
      studioDeliveryGuarantee({
        syncMode: "cdc",
        deliveryGuarantee: "exactly_once",
      }),
      CDC_DELIVERY_EXACTLY_ONCE,
    );
  });

  it("restores job delivery without inventing exactly_once", () => {
    assert.equal(jobStudioDeliveryGuarantee({}), CDC_DELIVERY_AT_LEAST_ONCE);
    assert.equal(
      jobStudioDeliveryGuarantee({ cdc_delivery: null, exactly_once_active: null }),
      CDC_DELIVERY_AT_LEAST_ONCE,
    );
    assert.equal(
      jobStudioDeliveryGuarantee({
        delivery_guarantee: "at_least_once",
        transfer_request: { delivery_guarantee: "exactly_once" },
      }),
      CDC_DELIVERY_EXACTLY_ONCE,
    );
    assert.equal(
      cdcDeliveryResultCopy({ cdcDelivery: "exactly_once", exactlyOnceActive: true }),
      "exactly_once dest-owned watermark · dest authoritative · not platform-wide",
    );
    assert.equal(
      cdcDeliveryResultCopy({
        cdcDelivery: "exactly_once",
        exactlyOnceActive: true,
        protocol: "dest_authoritative_open_bundle",
        destLsn: "0/20",
      }),
      "exactly_once dest-owned watermark · dest authoritative · not platform-wide · shared-log bundle · dest Open · dest LSN 0/20",
    );
  });
});
