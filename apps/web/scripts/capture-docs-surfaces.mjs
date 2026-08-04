/**
 * Capture full-desktop docs screenshots for Pipelines create form/drawer
 * and other menu surfaces missing from help docs.
 * Usage: node scripts/capture-docs-surfaces.mjs
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
  await page.waitForTimeout(600);
  await page.screenshot({ path: file, type: "png", fullPage: false });
  console.log("wrote", name);
}

async function expandNav(page) {
  const expand = page.getByRole("button", { name: /expand navigation/i }).first();
  if (await expand.count()) {
    try {
      await expand.click({ timeout: 1500 });
      await page.waitForTimeout(250);
    } catch {
      /* ok */
    }
  }
}

async function goNav(page, label) {
  await expandNav(page);
  const btn = page.getByRole("button", { name: new RegExp(`^${label}$`, "i") }).first();
  await btn.click();
  await page.waitForTimeout(700);
}

async function login(page) {
  await page.goto(`${BASE}/#/login`, { waitUntil: "domcontentloaded" });
  if (!(await page.getByRole("textbox", { name: /work email/i }).count())) {
    const loginBtn = page.getByRole("button", { name: /^log in$/i }).first();
    if (await loginBtn.count()) await loginBtn.click();
  }
  await page.getByRole("textbox", { name: /work email/i }).fill(EMAIL);
  await page.getByRole("textbox", { name: /password/i }).fill(PASS);
  await page.getByRole("button", { name: /sign in to workspace/i }).click();
  await page.waitForURL(/#\//, { timeout: 30000 });
  await page.waitForTimeout(600);
  await expandNav(page);
}

async function main() {
  await mkdir(OUT, { recursive: true });
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    viewport: { width: 1920, height: 1080 },
    deviceScaleFactor: 2,
  });
  const page = await context.newPage();
  await login(page);

  // --- Overview / Connectors / Jobs / Query / Pilot (refresh chrome) ---
  await goNav(page, "Overview");
  await shot(page, "app-overview.png");

  await goNav(page, "Connectors");
  await page.getByRole("heading", { name: /connectors/i }).first().waitFor({ timeout: 15000 });
  await shot(page, "app-connectors.png");
  // New connection wizard / dialog if present
  const newConn = page.getByRole("button", { name: /new connection|add connector|add connection/i }).first();
  if (await newConn.count()) {
    await newConn.click();
    await page.waitForTimeout(800);
    await shot(page, "app-connectors-wizard.png");
    await page.keyboard.press("Escape");
    const cancel = page.getByRole("button", { name: /^cancel$/i }).first();
    if (await cancel.count()) {
      try { await cancel.click({ timeout: 1000 }); } catch { /* ok */ }
    }
    await page.waitForTimeout(400);
  }

  await goNav(page, "Contracts");
  await page.waitForTimeout(800);
  await shot(page, "app-contracts.png");
  const firstContract = page.locator("button, a, [role='row'], .df2-card").filter({ hasText: /./ }).first();
  // Prefer opening a contract drawer if a card/row exists
  const contractCard = page.getByRole("button", { name: /open|view|contract/i }).first();
  if (await page.locator(".df2-drawer, [role='dialog'], aside").count() === 0) {
    const clickable = page.locator("[data-testid*='contract'], .df2-contract-card, .df2-list-row, table tbody tr").first();
    if (await clickable.count()) {
      await clickable.click();
      await page.waitForTimeout(700);
      if (await page.locator(".df2-drawer, [role='dialog']").count()) {
        await shot(page, "app-contracts-drawer.png");
        await page.keyboard.press("Escape");
      }
    } else if (await contractCard.count()) {
      await contractCard.click();
      await page.waitForTimeout(700);
      await shot(page, "app-contracts-drawer.png");
      await page.keyboard.press("Escape");
    }
  }

  await goNav(page, "Jobs");
  await page.waitForTimeout(900);
  await shot(page, "app-jobs.png");

  await goNav(page, "Transforms");
  await page.waitForTimeout(800);
  await shot(page, "app-transforms.png");

  await goNav(page, "Query");
  await page.waitForTimeout(800);
  await shot(page, "app-query.png");

  await goNav(page, "Pilot");
  await page.waitForTimeout(800);
  await shot(page, "app-pilot.png");

  await goNav(page, "MCP");
  await page.waitForTimeout(900);
  await shot(page, "app-mcp.png");

  await goNav(page, "Settings");
  await page.waitForTimeout(800);
  await shot(page, "app-settings.png");
  const ssoTab = page.getByRole("button", { name: /^sso$/i }).or(page.getByRole("tab", { name: /sso/i })).first();
  if (await ssoTab.count()) {
    await ssoTab.click();
    await page.waitForTimeout(600);
    await shot(page, "app-settings-sso.png");
  }
  const teamTab = page.getByRole("button", { name: /^team$/i }).or(page.getByRole("tab", { name: /team/i })).first();
  if (await teamTab.count()) {
    await teamTab.click();
    await page.waitForTimeout(600);
    await shot(page, "app-settings-team.png");
  }

  await goNav(page, "Proofs");
  await page.waitForTimeout(800);
  await shot(page, "app-proofs.png");

  // --- Pipelines: empty / form panels / list / drawer ---
  await goNav(page, "Pipelines");
  await page.getByRole("heading", { name: /pipelines/i }).first().waitFor({ timeout: 15000 });

  // If form already open, cancel
  const cancelForm = page.getByRole("button", { name: /^cancel$/i }).first();
  if (await page.getByText(/create recurring sync/i).count()) {
    if (await cancelForm.count()) await cancelForm.click();
    await page.waitForTimeout(400);
  }

  // Prefer empty-state shot when no pipelines; otherwise list shot
  const hasCreateEmpty = await page.getByRole("button", { name: /^create pipeline$/i }).count();
  if (hasCreateEmpty) {
    await shot(page, "app-pipelines.png");
  } else {
    await shot(page, "app-pipelines-list.png");
    // also keep a pipelines home alias
    await shot(page, "app-pipelines.png");
  }

  // Open create form
  const createBtn = page.getByRole("button", { name: /^(create pipeline|new pipeline)$/i }).first();
  await createBtn.click();
  await page.getByText(/create recurring sync/i).first().waitFor({ timeout: 10000 });
  await page.waitForTimeout(500);

  // Fill identity fields if connectors exist
  const nameInput = page.locator("#sched-name");
  if (await nameInput.count()) await nameInput.fill("Docs sample nightly orders");
  const srcTable = page.locator("#sched-src-table");
  if (await srcTable.count()) await srcTable.fill("orders");
  const dstTable = page.locator("#sched-dst-table");
  if (await dstTable.count()) await dstTable.fill("orders_warehouse");

  await shot(page, "app-pipelines-create.png");

  // Scroll cadence into view
  const cadence = page.getByText(/^Cadence$/i).first();
  if (await cadence.count()) {
    await cadence.scrollIntoViewIfNeeded();
    await page.waitForTimeout(400);
    await shot(page, "app-pipelines-cadence.png");
  }

  // Sync mode panel
  const sync = page.getByText(/^Sync mode$/i).first();
  if (await sync.count()) {
    await sync.scrollIntoViewIfNeeded();
    // Pick Incremental deduped if available to show cursor/PK
    const dedupe = page.getByRole("button", { name: /incremental deduped/i }).first();
    if (await dedupe.count()) {
      await dedupe.click();
      await page.waitForTimeout(300);
      const cursor = page.locator("#sched-cursor");
      if (await cursor.count()) await cursor.fill("updated_at");
      const pk = page.locator("#sched-pk");
      if (await pk.count()) await pk.fill("order_id");
    }
    await page.waitForTimeout(400);
    await shot(page, "app-pipelines-sync.png");
  }

  // Data contract + retry panels + save footer
  const contract = page.getByText(/^Data contract$/i).first();
  if (await contract.count()) {
    await contract.scrollIntoViewIfNeeded();
    await page.waitForTimeout(400);
    await shot(page, "app-pipelines-contract.png");
  }

  const saveBtn = page.getByRole("button", { name: /save pipeline/i }).first();
  if (await saveBtn.count()) {
    await saveBtn.scrollIntoViewIfNeeded();
    await page.waitForTimeout(300);
    await shot(page, "app-pipelines-save.png");
  }

  // Try save if enabled
  if (await saveBtn.count() && !(await saveBtn.isDisabled())) {
    await saveBtn.click();
    await page.waitForTimeout(1500);
  } else {
    // Cancel back to list/empty
    if (await cancelForm.count()) {
      await cancelForm.click();
      await page.waitForTimeout(500);
    }
  }

  // If we have a pipeline card, open detail drawer
  await goNav(page, "Pipelines");
  await page.waitForTimeout(700);
  const pipelineRow = page.locator("table tbody tr, .df2-pipeline-card, [class*='pipeline']").filter({ hasText: /docs sample|nightly|orders|daily|hourly|active|paused/i }).first();
  const anyRow = page.locator("table tbody tr").first();
  const target = (await pipelineRow.count()) ? pipelineRow : anyRow;
  if (await target.count()) {
    await shot(page, "app-pipelines-list.png");
    await target.click();
    await page.waitForTimeout(900);
    // Drawer should be open
    if (await page.getByRole("button", { name: /run now/i }).count() || await page.getByText(/overview|history|config/i).count()) {
      await shot(page, "app-pipelines-drawer.png");
      const history = page.getByRole("tab", { name: /history/i }).or(page.getByRole("button", { name: /^history$/i })).first();
      if (await history.count()) {
        await history.click();
        await page.waitForTimeout(500);
        await shot(page, "app-pipelines-history.png");
      }
      await page.keyboard.press("Escape");
    }
  } else {
    console.log("no pipeline rows to open drawer — create shot set still written");
  }

  // Map shot already exists; refresh transfer map if reachable quickly skipped
  await browser.close();
  console.log("done — docs surface screenshots captured");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
