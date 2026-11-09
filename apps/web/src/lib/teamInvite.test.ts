/**
 * Run: npx --yes tsx --test apps/web/src/lib/teamInvite.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { INVITE_DEFAULTS, inviteExpiryLabel, isInviteExpired } from "./teamInvite.js";

const NOW = Date.parse("2026-11-08T12:00:00Z");
const at = (ms: number) => new Date(NOW + ms).toISOString();

describe("team invite defaults", () => {
  it("emails an invitation by default and never mints a login unasked", () => {
    assert.equal(INVITE_DEFAULTS.mode, "email");
    assert.equal(INVITE_DEFAULTS.createLogin, false);
  });
});

describe("inviteExpiryLabel", () => {
  it("counts days, then hours, then the last hour", () => {
    assert.equal(inviteExpiryLabel(at(7 * 86_400_000), NOW), "Expires in 7 days");
    assert.equal(inviteExpiryLabel(at(30 * 3_600_000), NOW), "Expires in 30 hours");
    assert.equal(inviteExpiryLabel(at(3_600_000), NOW), "Expires in 1 hour");
    assert.equal(inviteExpiryLabel(at(10 * 60_000), NOW), "Expires within the hour");
  });

  it("marks lapsed links and ignores missing or bad stamps", () => {
    assert.equal(inviteExpiryLabel(at(-1), NOW), "Expired");
    assert.equal(isInviteExpired(at(-1), NOW), true);
    assert.equal(isInviteExpired(at(60_000), NOW), false);
    assert.equal(inviteExpiryLabel(undefined, NOW), "");
    assert.equal(inviteExpiryLabel("not-a-date", NOW), "");
  });
});

describe("inviteExpiryLabel rounding (F4)", () => {
  it("reads 7 days for a fresh 7-day invite, like the email", () => {
    assert.equal(inviteExpiryLabel(at(7 * 86_400_000 - 5_000), NOW), "Expires in 7 days");
    assert.equal(inviteExpiryLabel(at(2 * 86_400_000 + 3_600_000), NOW), "Expires in 3 days");
  });
});
