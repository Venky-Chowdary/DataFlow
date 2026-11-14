/**
 * UX-2: the Query export "Download" was a bare <a href> to an API path that
 * needs the bearer token, so it answered 401. The file is now fetched with the
 * session and saved as a Blob; a presigned bucket URL still opens directly.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { after, describe, it } from "node:test";
import { JSDOM } from "jsdom";

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost:5173/" });
Object.assign(globalThis, {
  window: dom.window,
  document: dom.window.document,
  localStorage: dom.window.localStorage,
  HTMLElement: dom.window.HTMLElement,
});
const clicks: { href: string; download: string }[] = [];
dom.window.HTMLAnchorElement.prototype.click = function click(this: HTMLAnchorElement) {
  clicks.push({ href: this.href, download: this.download });
};
const blobs: Blob[] = [];
Object.assign(URL, {
  createObjectURL: (b: Blob) => {
    blobs.push(b);
    return `blob:http://localhost:5173/${blobs.length}`;
  },
  revokeObjectURL: () => {},
});
localStorage.setItem(
  "df2.session",
  JSON.stringify({ email: "a@example.com", name: "", role: "admin", token: "tok-123", expires_at: 0, signed_in_at: 0 }),
);

const requests: { url: string; auth: string | null }[] = [];
let status = 200;
const realFetch = globalThis.fetch;
globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
  requests.push({ url: String(input), auth: new Headers(init?.headers).get("Authorization") });
  if (status !== 200) return new Response(JSON.stringify({ detail: "Not authenticated" }), { status });
  return new Response("id,name\n1,a\n", { status: 200, headers: { "Content-Type": "text/csv" } });
}) as typeof fetch;

const { downloadExportFile } = await import("./api.js");
const { exportUrlNeedsSession } = await import("./jobExportDownload.js");

after(() => {
  globalThis.fetch = realFetch;
  dom.window.close();
});

describe("query export download", () => {
  it("knows which URLs need the session", () => {
    assert.equal(exportUrlNeedsSession("/api/v1/transfer/download/q.csv", "/api/v1", "http://localhost:5173"), true);
    assert.equal(exportUrlNeedsSession("http://localhost:5173/api/v1/transfer/download/q.csv", "/api/v1", "http://localhost:5173"), true);
    assert.equal(exportUrlNeedsSession("https://api.example.com/api/v1/transfer/download/q.csv", "https://api.example.com/api/v1"), true);
    assert.equal(exportUrlNeedsSession("https://bucket.s3.amazonaws.com/q.csv?X-Amz-Signature=1", "https://api.example.com/api/v1"), false);
  });

  it("fetches an API download with the bearer token and saves the Blob", async () => {
    requests.length = 0;
    clicks.length = 0;
    await downloadExportFile("/api/v1/transfer/download/query_export.csv", "query_export.csv");
    assert.equal(requests.length, 1);
    assert.match(requests[0].url, /\/api\/v1\/transfer\/download\/query_export\.csv$/);
    assert.equal(requests[0].auth, "Bearer tok-123");
    assert.equal(clicks.length, 1);
    assert.match(clicks[0].href, /^blob:/);
    assert.equal(clicks[0].download, "query_export.csv");
    assert.equal(await blobs.at(-1)!.text(), "id,name\n1,a\n");
  });

  it("opens a presigned bucket URL directly, without our credentials", async () => {
    requests.length = 0;
    clicks.length = 0;
    await downloadExportFile("https://bucket.example.net/q.csv?sig=1", "q.csv");
    assert.equal(requests.length, 0);
    assert.equal(clicks[0].href, "https://bucket.example.net/q.csv?sig=1");
  });

  it("reports a refused download instead of saving an error page", async () => {
    status = 410;
    clicks.length = 0;
    await assert.rejects(() => downloadExportFile("/api/v1/transfer/download/old.csv", "old.csv"), /expired/);
    assert.equal(clicks.length, 0);
    status = 200;
  });

  it("the Query page no longer renders the download as a bare link", () => {
    const page = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "pages", "QueryPage.tsx"), "utf8");
    assert.doesNotMatch(page, /href=\{exportResult\.download_url\}/);
    assert.match(page, /downloadExportFile\(exportResult\.download_url/);
  });
});
