/**
 * Run: npx --yes tsx --test apps/web/src/lib/cdcExactlyOnce.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  CDC_DELIVERY_AT_LEAST_ONCE,
  CDC_DELIVERY_EXACTLY_ONCE,
  exactlyOnceWiredDest,
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

  it("only sqlite is wired for dest-owned watermark EOS", () => {
    assert.equal(exactlyOnceWiredDest("sqlite"), true);
    assert.equal(exactlyOnceWiredDest("postgresql"), false);
    assert.equal(exactlyOnceWiredDest("csv"), false);
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
      }),
      CDC_DELIVERY_EXACTLY_ONCE,
    );
  });
});
