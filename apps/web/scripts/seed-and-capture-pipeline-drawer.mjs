/**
 * Seed two saved connectors + one pipeline via UI/API, then capture detail drawer.
 */
import { chromium } from "playwright";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.resolve(__dirname, "../public/docs/screenshots");
const API = "http://127.0.0.1:8001/api/v1";
const EMAIL = process.env.DATAWRAP_EMAIL;
const PASS = process.env.DATAWRAP_PASSWORD;
if (!EMAIL || !PASS) {
  throw new Error("Set DATAWRAP_EMAIL and DATAWRAP_PASSWORD for local screenshot capture.");
}

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({
  viewport: { width: 1920, height: 1080 },
  deviceScaleFactor: 2,
});
const page = await context.newPage();

await page.goto("http://127.0.0.1:5173/#/login", { waitUntil: "domcontentloaded" });
if (!(await page.getByRole("textbox", { name: /work email/i }).count())) {
  await page.getByRole("button", { name: /^log in$/i }).first().click();
}
await page.getByRole("textbox", { name: /work email/i }).fill(EMAIL);
await page.getByRole("textbox", { name: /password/i }).fill(PASS);
await page.getByRole("button", { name: /sign in to workspace/i }).click();
await page.waitForURL(/#\//, { timeout: 30000 });
await page.waitForTimeout(800);

// Discover auth header used by the app
const auth = await page.evaluate(async () => {
  const keys = Object.keys(localStorage);
  const dump = {};
  for (const k of keys) dump[k] = localStorage.getItem(k);
  // also try sessionStorage
  const sdump = {};
  for (const k of Object.keys(sessionStorage)) sdump[k] = sessionStorage.getItem(k);
  return { localStorage: dump, sessionStorage: sdump };
});
console.log("auth keys", Object.keys(auth.localStorage), Object.keys(auth.sessionStorage));

const token =
  auth.localStorage.access_token ||
  auth.localStorage.token ||
  auth.localStorage.df_access_token ||
  auth.localStorage["dataflow_token"] ||
  Object.values(auth.localStorage).find((v) => typeof v === "string" && v.length > 40 && !v.startsWith("{")) ||
  "";

async function api(method, url, body) {
  const res = await page.evaluate(
    async ({ method, url, body, token }) => {
      const headers = { "Content-Type": "application/json" };
      if (token) headers.Authorization = `Bearer ${token}`;
      // cookies from browser session also sent when same-origin; API may be different origin
      const r = await fetch(url, { method, headers, body: body ? JSON.stringify(body) : undefined, credentials: "include" });
      const text = await r.text();
      return { status: r.status, text };
    },
    { method, url, body, token },
  );
  console.log(method, url, res.status, res.text.slice(0, 180));
  return res;
}

// Login via API for bearer if needed
const loginRes = await page.evaluate(async ({ email, password }) => {
  const r = await fetch("http://127.0.0.1:8001/api/v1/auth/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  });
  return { status: r.status, text: await r.text() };
}, { email: EMAIL, password: PASS });
console.log("api login", loginRes.status, loginRes.text.slice(0, 200));
let bearer = token;
try {
  const j = JSON.parse(loginRes.text);
  bearer = j.access_token || j.token || bearer;
} catch {
  /* ok */
}

async function apiBearer(method, urlPath, body) {
  const res = await page.evaluate(
    async ({ method, url, body, bearer }) => {
      const headers = { "Content-Type": "application/json" };
      if (bearer) headers.Authorization = `Bearer ${bearer}`;
      const r = await fetch(url, { method, headers, body: body ? JSON.stringify(body) : undefined });
      return { status: r.status, text: await r.text() };
    },
    { method, url: `${API}${urlPath}`, body, bearer },
  );
  console.log(method, urlPath, res.status, res.text.slice(0, 220));
  let json = null;
  try { json = JSON.parse(res.text); } catch { /* ok */ }
  return { ...res, json };
}

const src = await apiBearer("POST", "/connectors/saved", {
  name: "Docs Source PG",
  type: "postgresql",
  host: "127.0.0.1",
  port: 5432,
  database: "orders",
  username: "demo",
  password: "demo",
  role: "source",
  last_test_ok: true,
});
const dst = await apiBearer("POST", "/connectors/saved", {
  name: "Docs Dest PG",
  type: "postgresql",
  host: "127.0.0.1",
  port: 5432,
  database: "warehouse",
  username: "demo",
  password: "demo",
  role: "destination",
  last_test_ok: true,
});

const srcId = src.json?.id;
const dstId = dst.json?.id;
if (srcId && dstId) {
  await apiBearer("POST", "/schedules/", {
    name: "Docs sample nightly orders",
    source_connector_id: srcId,
    source_table: "orders",
    dest_connector_id: dstId,
    dest_table: "orders_warehouse",
    interval: "daily",
    sync_mode: "full_refresh_append",
    validation_mode: "balanced",
    schema_policy: "manual_review",
    notify_on_failure: true,
  });
}

const expand = page.getByRole("button", { name: /expand navigation/i }).first();
if (await expand.count()) {
  try { await expand.click({ timeout: 1500 }); } catch { /* ok */ }
}
await page.getByRole("button", { name: /^pipelines$/i }).first().click();
await page.waitForTimeout(1200);
await page.screenshot({ path: path.join(OUT, "app-pipelines-list.png"), type: "png" });
console.log("wrote list");

// Click pipeline by name
const nameHit = page.getByText(/Docs sample nightly orders/i).first();
if (await nameHit.count()) {
  await nameHit.click();
  await page.waitForTimeout(1000);
} else {
  // click first visible card-ish control in main
  const card = page.locator("main").locator("button, [role='button'], tr").filter({ hasText: /daily|hourly|active|paused|orders/i }).first();
  if (await card.count()) await card.click({ force: true });
  await page.waitForTimeout(1000);
}

await page.screenshot({ path: path.join(OUT, "app-pipelines-drawer.png"), type: "png" });
console.log("wrote drawer; run now?", await page.getByRole("button", { name: /run now/i }).count());

const history = page.getByRole("tab", { name: /history/i }).or(page.getByRole("button", { name: /^history$/i })).first();
if (await history.count()) {
  await history.click();
  await page.waitForTimeout(500);
  await page.screenshot({ path: path.join(OUT, "app-pipelines-history.png"), type: "png" });
  console.log("wrote history");
}

// Also refresh create form with connectors now available
await page.keyboard.press("Escape");
await page.waitForTimeout(400);
const newBtn = page.getByRole("button", { name: /new pipeline/i }).first();
if (await newBtn.count()) {
  await newBtn.click();
  await page.waitForTimeout(700);
  await page.locator("#sched-name").fill("Second docs pipeline");
  await page.locator("#sched-src-table").fill("orders");
  await page.locator("#sched-dst-table").fill("orders_warehouse");
  await page.screenshot({ path: path.join(OUT, "app-pipelines-create.png"), type: "png" });
  console.log("rewrote create with connectors");
  const cancel = page.getByRole("button", { name: /^cancel$/i }).first();
  if (await cancel.count()) await cancel.click();
}

await browser.close();
