/**
 * UX-43: after sign-in the shell appeared, then the target screen's code was
 * only requested (lazy chunk), so the content area sat on "Loading Transfer…"
 * for 0.7-1.4 s locally and about 4 s in the audit. The chunk is now fetched
 * while the sign-in form is on screen, in parallel with typing and the login
 * request.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { lazyNamed } from "./lazyPage.js";

const src = join(dirname(fileURLToPath(import.meta.url)), "..");

describe("lazy screens can be preloaded", () => {
  it("preload() starts the import once and the render reuses it", async () => {
    let calls = 0;
    const Page = lazyNamed(async () => {
      calls += 1;
      return { Page: () => null };
    }, "Page");
    assert.equal(typeof Page.preload, "function");
    Page.preload();
    Page.preload();
    await new Promise((r) => setTimeout(r, 0));
    assert.equal(calls, 1);
  });

  it("a failed preload does not poison the later render", async () => {
    let calls = 0;
    const Page = lazyNamed(async () => {
      calls += 1;
      if (calls === 1) throw new Error("offline");
      return { Page: () => null };
    }, "Page");
    Page.preload();
    await new Promise((r) => setTimeout(r, 0));
    Page.preload();
    await new Promise((r) => setTimeout(r, 0));
    assert.equal(calls, 2);
  });
});

describe("the sign-in stage preloads the screen it will open", () => {
  const app = readFileSync(join(src, "DataTransferApp.tsx"), "utf8");
  it("every lazy screen is in the preload map", () => {
    const lazyNames = [...app.matchAll(/^const (\w+) = lazyNamed\(/gm)].map((m) => m[1]);
    assert.ok(lazyNames.length >= 10);
    const map = app.slice(app.indexOf("const SCREEN_CHUNKS"), app.indexOf("};", app.indexOf("const SCREEN_CHUNKS")));
    for (const name of lazyNames) assert.match(map, new RegExp(`\\b${name}\\b`), `${name} is not preloadable by screen`);
  });
  it("preloads the entry screen while the sign-in form is shown", () => {
    assert.match(app, /if \(stage === "login"\) SCREEN_CHUNKS\[entryScreen\]\?\.preload\(\);/);
  });
});
