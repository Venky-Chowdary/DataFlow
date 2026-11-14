import { createRequire } from "node:module";
import { readFileSync } from "node:fs";

import { expect, test, type Page } from "@playwright/test";

/**
 * Accessibility gate (UX-16): axe-core over the main screens, WCAG 2.2 A/AA
 * rules. Any critical or serious violation fails the job; the report names the
 * rule, the impact and the first offending nodes so the fix is findable.
 *
 * Runs against the dev server in the workspace preview (API with auth off),
 * the same setup as the wizard project in the studio-ui job.
 */

const require = createRequire(import.meta.url);
// AXE_CORE_PATH lets a machine without the npm package point at a local copy.
const AXE_SOURCE = readFileSync(process.env.AXE_CORE_PATH || require.resolve("axe-core/axe.min.js"), "utf8");

const WCAG_TAGS = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"];
const BLOCKING = new Set(["critical", "serious"]);

type Violation = { id: string; impact: string | null; help: string; nodes: string[] };

async function axeViolations(page: Page): Promise<Violation[]> {
  await page.addScriptTag({ content: AXE_SOURCE });
  return page.evaluate(async (tags) => {
    const axe = (window as unknown as { axe: { run: (ctx: Document, opts: unknown) => Promise<{ violations: Array<{ id: string; impact: string | null; help: string; nodes: Array<{ target: string[] }> }> }> } }).axe;
    const result = await axe.run(document, { runOnly: { type: "tag", values: tags } });
    return result.violations.map((v) => ({
      id: v.id,
      impact: v.impact,
      help: v.help,
      nodes: v.nodes.slice(0, 5).map((n) => n.target.join(" ")),
    }));
  }, WCAG_TAGS);
}

function report(violations: Violation[]): string {
  return violations.map((v) => `${v.impact} ${v.id}: ${v.help}\n    ${v.nodes.join("\n    ")}`).join("\n");
}

/**
 * Let entrance transitions finish: axe blends a fading element with its
 * background and reports a contrast failure the settled page does not have.
 * Infinite animations (spinners) are skipped.
 */
async function settleAnimations(page: Page) {
  await expect(page.locator(".is-first-screen")).toHaveCount(0, { timeout: 10_000 }).catch(() => {});
  await page.evaluate(async () => {
    const finite = document.getAnimations().filter((a) => {
      const timing = a.effect?.getComputedTiming();
      return timing && Number.isFinite(timing.endTime as number);
    });
    await Promise.all(finite.map((a) => a.finished.catch(() => undefined)));
  });
}

async function expectNoBlockingViolations(page: Page, label: string) {
  await settleAnimations(page);
  const blocking = (await axeViolations(page)).filter((v) => v.impact && BLOCKING.has(v.impact));
  expect(blocking, `${label}: axe critical/serious violations\n${report(blocking)}`).toEqual([]);
}

async function enterWorkspace(page: Page) {
  await page.goto("/#/dashboard");
  const preview = page.locator("button", { hasText: "Preview workspace (UI only)" });
  await expect(preview).toBeAttached();
  await preview.evaluate((node: HTMLButtonElement) => node.click());
  await expect(page.locator("main#datawrap-main-content")).toBeVisible();
}

/** Screens a signed-in user reaches from the sidebar, plus the heading each shows. */
const SCREENS: Array<{ hash: string; label: string }> = [
  { hash: "dashboard", label: "Overview" },
  { hash: "transfer", label: "Transfer Studio" },
  { hash: "connectors", label: "Connectors" },
  { hash: "contracts", label: "Contracts" },
  { hash: "jobs", label: "Jobs" },
  { hash: "schedules", label: "Schedules" },
  { hash: "transforms", label: "Transforms" },
  { hash: "query", label: "Query" },
  { hash: "pilot", label: "Pilot" },
  { hash: "settings", label: "Settings" },
];

test.describe("accessibility gate", () => {
  test.beforeEach(async ({ page }) => {
    await page.emulateMedia({ reducedMotion: "reduce" });
    await page.addInitScript(() => {
      localStorage.setItem("df2.theme", "light");
    });
  });

  test("public landing and sign-in", async ({ page }) => {
    await page.goto("/");
    await expect(page.getByRole("heading", { level: 1 }).first()).toBeVisible();
    await expectNoBlockingViolations(page, "landing");
    await page.goto("/#/transfer");
    await expect(page.locator("#login-email")).toBeVisible();
    await expectNoBlockingViolations(page, "sign-in");
  });

  test("signed-in shell: skip link and main landmark", async ({ page }) => {
    await enterWorkspace(page);
    await page.keyboard.press("Tab");
    const skip = page.getByRole("link", { name: "Skip to main content" });
    await expect(skip).toBeFocused();
    await expect(skip).toBeInViewport();
    await page.keyboard.press("Enter");
    await expect(page.locator("main#datawrap-main-content")).toBeFocused();
    await expect(page.locator("main")).toHaveCount(1);
  });

  for (const screen of SCREENS) {
    test(`${screen.label} has no critical or serious violations`, async ({ page }) => {
      await enterWorkspace(page);
      await page.evaluate((hash) => {
        window.location.hash = `#/${hash}`;
      }, screen.hash);
      await expect(page.locator(".df2-route-loading")).toHaveCount(0, { timeout: 30_000 });
      // Let the first data load settle so loaded states, not spinners, are checked.
      await page.waitForLoadState("networkidle").catch(() => {});
      await expectNoBlockingViolations(page, screen.label);
    });
  }

  test("dialogs: new connection has no violations and gives focus back", async ({ page }) => {
    await enterWorkspace(page);
    await page.evaluate(() => {
      window.location.hash = "#/connectors";
    });
    const trigger = page.getByRole("button", { name: /New connection|Add connection/ }).first();
    await expect(trigger).toBeVisible();
    await trigger.focus();
    await page.keyboard.press("Enter");
    await expect(page.getByRole("dialog")).toBeVisible();
    await expectNoBlockingViolations(page, "new connection dialog");
    await page.keyboard.press("Escape");
    await expect(page.getByRole("dialog")).toHaveCount(0);
    await expect(trigger).toBeFocused();
  });
});
