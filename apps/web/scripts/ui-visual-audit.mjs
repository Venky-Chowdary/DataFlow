import { chromium } from "playwright";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const outDir = path.resolve(__dirname, "../.ui-audit");
const base = process.env.UI_AUDIT_URL || "http://127.0.0.1:5173/";

const SESSION = {
  email: "test@gmail.com",
  name: "Test User",
  role: "admin",
  token: "dev-ui-preview",
  expires_at: Math.floor(Date.now() / 1000) + 86400,
  signed_in_at: Date.now(),
};

const PUBLIC = [
  ["landing", "#/"],
  ["pricing", "#/pricing"],
  ["enterprise", "#/enterprise"],
  ["customers", "#/customers"],
  ["integrations", "#/integrations"],
  ["security", "#/security"],
  ["help", "#/help"],
  ["product-transfer", "#/product/transfer"],
  ["contact", "#/contact"],
];

const APP = [
  ["overview", "#/overview"],
  ["transfer", "#/transfer"],
  ["connectors", "#/connectors"],
  ["pipelines", "#/pipelines"],
  ["transforms", "#/transforms"],
  ["jobs", "#/jobs"],
  ["query", "#/query"],
  ["pilot", "#/pilot"],
  ["contracts", "#/contracts"],
  ["mcp", "#/mcp"],
  ["settings", "#/settings"],
  ["docs", "#/docs"],
  ["proofs", "#/proofs"],
];

const VIEWPORTS = [
  { name: "desktop", width: 1440, height: 900 },
  { name: "laptop", width: 1280, height: 800 },
  { name: "narrow", width: 768, height: 900 },
];

async function waitSettled(page) {
  await page.waitForTimeout(700);
  await page.evaluate(() => document.fonts?.ready?.catch?.(() => undefined));
}

async function shot(page, name) {
  const file = path.join(outDir, `${name}.png`);
  await page.screenshot({ path: file, fullPage: false });
  return file;
}

async function main() {
  await mkdir(outDir, { recursive: true });
  const browser = await chromium.launch({ headless: true });
  const notes = [];

  try {
    for (const vp of VIEWPORTS) {
      const context = await browser.newContext({
        viewport: { width: vp.width, height: vp.height },
        deviceScaleFactor: 1,
      });
      const page = await context.newPage();

      await page.goto(base, { waitUntil: "domcontentloaded", timeout: 30000 });
      await waitSettled(page);

      for (const [name, hash] of PUBLIC) {
        await page.goto(`${base}${hash}`, { waitUntil: "domcontentloaded", timeout: 30000 });
        await waitSettled(page);
        await shot(page, `${vp.name}-${name}`);
        const overflow = await page.evaluate(() => ({
          x: document.documentElement.scrollWidth - window.innerWidth,
          y: document.documentElement.scrollHeight,
        }));
        if (overflow.x > 8) notes.push(`${vp.name}-${name}: horizontal overflow ${overflow.x}px`);
      }

      await page.evaluate((session) => {
        localStorage.removeItem("df2.session");
      }, SESSION);
      await page.goto(`${base}#/overview`, { waitUntil: "domcontentloaded" });
      await waitSettled(page);
      await shot(page, `${vp.name}-login`);

      const loginOk = await page.locator(".lp-login--gate").count();
      if (!loginOk) notes.push(`${vp.name}-login: gate markup missing`);
      const hiddenSub = await page.evaluate(() => {
        const el = document.querySelector(".lp-login-auth-sub");
        if (!el) return "missing";
        const s = getComputedStyle(el);
        return s.display === "none" || s.visibility === "hidden" ? "hidden" : "visible";
      });
      if (hiddenSub !== "visible") notes.push(`${vp.name}-login: auth-sub ${hiddenSub}`);

      await page.evaluate((session) => {
        localStorage.setItem("df2.session", JSON.stringify(session));
      }, SESSION);
      await page.goto(`${base}#/overview`, { waitUntil: "domcontentloaded" });
      await page.reload({ waitUntil: "domcontentloaded" });
      await waitSettled(page);

      for (const [name, hash] of APP) {
        await page.goto(`${base}${hash}`, { waitUntil: "domcontentloaded", timeout: 30000 });
        await waitSettled(page);
        await shot(page, `${vp.name}-${name}`);
        const overflow = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
        if (overflow > 8) notes.push(`${vp.name}-${name}: horizontal overflow ${overflow}px`);
        const app = await page.locator(".df2-app").count();
        if (!app) notes.push(`${vp.name}-${name}: workspace shell missing`);
      }

      await context.close();
    }
  } finally {
    await browser.close();
  }

  await writeFile(path.join(outDir, "notes.json"), JSON.stringify(notes, null, 2));
  console.log(`Wrote screenshots to ${outDir}`);
  console.log(notes.length ? `Issues:\n- ${notes.join("\n- ")}` : "No automated overflow/visibility issues.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
