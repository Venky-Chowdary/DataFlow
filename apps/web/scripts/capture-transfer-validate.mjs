/**
 * Capture Validate step only — accept map risks, force step 4, screenshot
 * before Run preflight (which can bounce back to Map when review remains).
 */
import { chromium } from "playwright";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.resolve(__dirname, "../public/docs/screenshots");
const EMAIL = process.env.DATAWRAP_EMAIL;
const PASS = process.env.DATAWRAP_PASSWORD;
if (!EMAIL || !PASS) {
  throw new Error("Set DATAWRAP_EMAIL and DATAWRAP_PASSWORD for local screenshot capture.");
}

async function forceStep(page, stepN) {
  return page.evaluate((n) => {
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
    return walk(fiberOf(root.firstElementChild) || fiberOf(root));
  }, stepN);
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
await page.waitForTimeout(400);

const expand = page.getByRole("button", { name: /expand navigation/i }).first();
if (await expand.count()) {
  try { await expand.click({ timeout: 1500 }); } catch { /* ok */ }
}

await page.getByRole("button", { name: /^transfer$/i }).first().click();
await page.getByRole("button", { name: /new transfer/i }).first().click();
await page.getByRole("button", { name: /load sample orders csv/i }).click();
await page.getByRole("button", { name: /continue to destination/i }).click({ timeout: 25000 });
await page.getByRole("tab", { name: /file export/i }).click();
await page.getByRole("tab", { name: /^csv$/i }).click();
await page.locator('input[placeholder*="export"]').first().fill("exports/sample-orders.csv");
await page.getByRole("button", { name: /continue to map/i }).click();
await page.getByRole("heading", { name: /map columns/i }).waitFor({ timeout: 60000 });
await page.waitForTimeout(900);

let n = await page.getByRole("button", { name: /accept risk/i }).count();
while (n > 0) {
  await page.getByRole("button", { name: /accept risk/i }).first().click();
  await page.waitForTimeout(220);
  n = await page.getByRole("button", { name: /accept risk/i }).count();
}
let a = await page.getByRole("button", { name: /^approve$/i }).count();
while (a > 0) {
  await page.getByRole("button", { name: /^approve$/i }).first().click();
  await page.waitForTimeout(180);
  a = await page.getByRole("button", { name: /^approve$/i }).count();
}

await forceStep(page, 4);
await page.waitForTimeout(700);

// Confirm Validate UI before anything can bounce back to Map
for (let i = 0; i < 15; i++) {
  const onValidate =
    (await page.getByRole("button", { name: /run preflight/i }).count()) > 0 ||
    (await page.getByText(/0% PENDING|NOT RUN|Validation rules|validation dashboard/i).count()) > 0;
  const onMap = (await page.getByRole("heading", { name: /map columns/i }).count()) > 0;
  if (onValidate && !onMap) break;
  if (onMap) await forceStep(page, 4);
  await page.waitForTimeout(350);
}

await page.screenshot({ path: path.join(OUT, "app-transfer-validate.png"), type: "png" });
const text = (await page.locator("body").innerText()).slice(0, 280).replace(/\s+/g, " ");
console.log("wrote app-transfer-validate.png", text);
await browser.close();
