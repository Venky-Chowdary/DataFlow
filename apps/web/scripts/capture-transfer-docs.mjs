/**
 * Capture full-bleed Transfer Studio docs screenshots at 1920x1080 @2x (3840x2160).
 * Usage: node scripts/capture-transfer-docs.mjs
 */
import { chromium } from "playwright";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.resolve(__dirname, "../public/docs/screenshots");
const BASE = process.env.DATAWRAP_URL || "http://127.0.0.1:5173";
const EMAIL = process.env.DATAWRAP_EMAIL;
const PASS = process.env.DATAWRAP_PASSWORD;
if (!EMAIL || !PASS) {
  throw new Error("Set DATAWRAP_EMAIL and DATAWRAP_PASSWORD for local screenshot capture.");
}

async function shot(page, name) {
  const file = path.join(OUT, name);
  await page.waitForTimeout(700);
  await page.screenshot({ path: file, type: "png", fullPage: false });
  console.log("wrote", name);
}

async function ensureExpandedNav(page) {
  const expand = page.getByRole("button", { name: /expand navigation/i }).first();
  if (await expand.count()) {
    try {
      await expand.click({ timeout: 2000 });
      await page.waitForTimeout(300);
    } catch {
      /* already expanded */
    }
  }
}

async function openTransferStudio(page) {
  await page.goto(`${BASE}/#/transfer`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(500);
  const transferNav = page.getByRole("button", { name: /^transfer$/i }).first();
  if (await transferNav.count()) await transferNav.click();
  await page.getByRole("heading", { name: /transfer studio/i }).waitFor({ timeout: 20000 });
  const fresh = page.getByRole("button", { name: /new transfer/i }).first();
  if (await fresh.count()) {
    await fresh.click();
    await page.waitForTimeout(400);
  }
  await page.getByText(/where is your data/i).first().waitFor({ timeout: 15000 });
}

async function acceptAllRisks(page) {
  for (let i = 0; i < 16; i++) {
    const risk = page.getByRole("button", { name: /^accept risk$/i }).first();
    if (!(await risk.count())) break;
    await risk.click();
    await page.waitForTimeout(220);
  }
  for (let i = 0; i < 8; i++) {
    const approve = page.getByRole("button", { name: /^approve$/i }).first();
    if (!(await approve.count())) break;
    try {
      await approve.click({ timeout: 800 });
    } catch {
      break;
    }
    await page.waitForTimeout(180);
  }
  const eligible = page.getByRole("button", { name: /approve eligible/i }).first();
  if (await eligible.count()) {
    try {
      await eligible.click({ timeout: 1500 });
    } catch {
      /* ok */
    }
  }
}

/** Force Transfer Studio step via React state (1 Source … 5 Run). */
async function forceStep(page, stepN) {
  await page.evaluate((n) => {
    function fiberOf(el) {
      if (!el) return null;
      for (const k of Object.keys(el)) {
        if (k.startsWith("__reactFiber$") || k.startsWith("__reactInternalInstance$")) return el[k];
      }
      return null;
    }
    function walk(f, d = 0) {
      if (!f || d > 140) return false;
      let s = f.memoizedState;
      while (s) {
        if (typeof s.memoizedState === "number" && s.queue && s.memoizedState >= 1 && s.memoizedState <= 5) {
          try {
            s.queue.dispatch(n);
            return true;
          } catch {
            /* continue */
          }
        }
        s = s.next;
      }
      return walk(f.child, d + 1) || walk(f.sibling, d + 1);
    }
    const root = document.querySelector("#root") || document.body;
    walk(fiberOf(root.firstElementChild) || fiberOf(root));
  }, stepN);
}

async function main() {
  await mkdir(OUT, { recursive: true });
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    viewport: { width: 1920, height: 1080 },
    deviceScaleFactor: 2,
  });
  const page = await context.newPage();

  await page.goto(`${BASE}/#/login`, { waitUntil: "domcontentloaded" });
  if (!(await page.getByRole("textbox", { name: /work email/i }).count())) {
    const loginBtn = page.getByRole("button", { name: /^log in$/i }).first();
    if (await loginBtn.count()) await loginBtn.click();
  }
  await page.getByRole("textbox", { name: /work email/i }).fill(EMAIL);
  await page.getByRole("textbox", { name: /password/i }).fill(PASS);
  await page.getByRole("button", { name: /sign in to workspace/i }).click();
  await page.waitForURL(/#\/(overview|transfer|connectors)/, { timeout: 30000 });
  await page.waitForTimeout(800);

  await ensureExpandedNav(page);
  await openTransferStudio(page);

  // --- Source ---
  await page.getByRole("button", { name: /load sample orders csv/i }).click();
  await page.getByText(/sample-orders\.csv|Sample-Orders\.csv/i).first().waitFor({ timeout: 25000 });
  await page.getByText(/detected structure/i).first().waitFor({ timeout: 25000 });
  await page.getByRole("button", { name: /continue to destination/i }).waitFor({ state: "visible", timeout: 20000 });
  await shot(page, "app-transfer-source.png");

  await page.getByRole("button", { name: /continue to destination/i }).click();
  await page.getByText(/destination mode/i).first().waitFor({ timeout: 15000 });

  // --- Destination ---
  await page.getByRole("tab", { name: /file export/i }).click();
  await page.getByRole("tab", { name: /^csv$/i }).click();
  const pathInput = page.locator('input[placeholder*="export"]').first();
  await pathInput.waitFor({ timeout: 10000 });
  await pathInput.fill("exports/sample-orders.csv");
  await page.waitForTimeout(400);
  await shot(page, "app-transfer-destination.png");

  await page.getByRole("button", { name: /continue to map/i }).click();
  await page.getByRole("heading", { name: /map columns/i }).waitFor({ timeout: 60000 });
  await page.waitForTimeout(1200);

  // --- Map ---
  await acceptAllRisks(page);
  const contVal = page.getByRole("button", { name: /continue to validate/i });
  for (let i = 0; i < 30; i++) {
    if (!(await contVal.isDisabled())) break;
    await acceptAllRisks(page);
    await page.waitForTimeout(250);
  }
  await shot(page, "app-transfer-map.png");

  // Prefer force step — Continue auto-runs preflight and can bounce back to Map.
  await forceStep(page, 4);

  // --- Validate (capture before Run preflight; that path may return to Map) ---
  for (let i = 0; i < 16; i++) {
    const onValidate =
      (await page.getByRole("button", { name: /run preflight/i }).count()) > 0 ||
      (await page.getByText(/0% PENDING|NOT RUN|Validation rules/i).count()) > 0;
    const onMap = (await page.getByRole("heading", { name: /map columns/i }).count()) > 0;
    if (onValidate && !onMap) break;
    await forceStep(page, 4);
    await page.waitForTimeout(350);
  }
  await page.waitForTimeout(700);
  await shot(page, "app-transfer-validate.png");

  // --- Run ---
  await forceStep(page, 5);
  await page.waitForTimeout(900);
  if (!(await page.getByText(/execute transfer|confirm validate before|preflight/i).count())) {
    await forceStep(page, 5);
    await page.waitForTimeout(600);
  }
  await shot(page, "app-transfer-run.png");

  await browser.close();
  console.log("done — all five Transfer Studio shots at 3840x2160");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
