/**
 * Review-12 item 2: the screenshot harness writes live store rows
 * (otp_challenges, mail_outbox, users), so it must refuse to run against
 * anything but a local/scratch Mongo — never production or a shared
 * staging store.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
// @ts-expect-error plain ESM helper without type declarations
import { mongoUriIsLocal } from "../scripts/screenshot-scratch-gate.mjs";

const WEB = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HARNESS = path.join(WEB, "scripts", "signup-screenshots.mjs");

test("local and scratch hosts are accepted", () => {
  assert.equal(mongoUriIsLocal("mongodb://localhost:27017/"), true);
  assert.equal(mongoUriIsLocal("mongodb://127.0.0.1:27017/datawrap"), true);
  assert.equal(mongoUriIsLocal("mongodb://[::1]:27017/"), true);
  assert.equal(mongoUriIsLocal("mongodb://0.0.0.0:27017/"), true);
  assert.equal(mongoUriIsLocal("mongodb://dev.localhost:27017/"), true);
  // docker/compose service names — single-label hostnames
  assert.equal(mongoUriIsLocal("mongodb://df-mongo:27017/"), true);
  assert.equal(mongoUriIsLocal("mongodb://user:pw@mongo:27017/db"), true);
  // replica-set seed lists are fine when every host is local
  assert.equal(
    mongoUriIsLocal("mongodb://mongo1:27017,mongo2:27017/db?replicaSet=rs0"),
    true,
  );
});

test("remote hosts are rejected", () => {
  assert.equal(mongoUriIsLocal("mongodb://prod.mongo.example.com:27017/"), false);
  assert.equal(mongoUriIsLocal("mongodb+srv://cluster0.example.mongodb.net/"), false);
  assert.equal(mongoUriIsLocal("mongodb://34.102.55.9:27017/"), false);
  // one remote host in a seed list poisons the whole URI
  assert.equal(
    mongoUriIsLocal("mongodb://localhost:27017,prod.example.com:27017/db"),
    false,
  );
  // a remote host behind credentials is still remote
  assert.equal(
    mongoUriIsLocal("mongodb://u:p@prod.example.com:27017/db"),
    false,
  );
});

test("harness refuses a non-scratch Mongo URI", () => {
  const res = spawnSync(process.execPath, [HARNESS], {
    env: { ...process.env, MONGODB_URI: "mongodb://prod.mongo.example.com:27017/" },
    encoding: "utf8",
    timeout: 30_000,
  });
  assert.equal(res.status, 1);
  assert.match(String(res.stderr), /refusing to run/i);
});
