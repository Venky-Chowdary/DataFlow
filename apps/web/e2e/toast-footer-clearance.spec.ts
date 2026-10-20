import { expect, test, type Locator, type Page } from "@playwright/test";
import { installHermeticStubs } from "./hermetic";
import { openStudio } from "./support";

/**
 * D-BP-11: a toast must never cover the sticky footer action bar, a primary
 * action, or the step's content card — the "Source profiled" toast painted
 * over "Continue to Transform", "Preflight passed" covered the contract row
 * on Validate, and the toast stack covered the bottom-right of the Map
 * empty-state card. Geometry, not styling.
 *
 * Every visible .dt-toast is measured — .first() can resolve to a toast that
 * is registered but not yet rendered (boundingBox() returns null), which is
 * how this spec used to false-fail.
 */

function intersects(
  a: { x: number; y: number; width: number; height: number },
  b: { x: number; y: number; width: number; height: number },
): boolean {
  return (
    a.x < b.x + b.width &&
    a.x + a.width > b.x &&
    a.y < b.y + b.height &&
    a.y + a.height > b.y
  );
}

async function visibleBoxes(
  locator: Locator,
): Promise<{ x: number; y: number; width: number; height: number }[]> {
  const boxes: { x: number; y: number; width: number; height: number }[] = [];
  const n = await locator.count();
  for (let i = 0; i < n; i++) {
    const el = locator.nth(i);
    if (!(await el.isVisible().catch(() => false))) continue;
    const box = await el.boundingBox();
    if (box) boxes.push(box);
  }
  return boxes;
}

async function assertToastClearance(
  page: Page,
  label: string,
  opts: { requireToast?: boolean } = {},
) {
  const requireToast = opts.requireToast ?? true;
  const toasts = page.locator(".dt-toast:visible");
  if (requireToast) {
    await expect(toasts.first(), `a toast must be visible on ${label}`).toBeVisible();
  }
  const toastBoxes = await visibleBoxes(toasts);
  if (requireToast) {
    expect(
      toastBoxes.length,
      `at least one rendered toast must have a box on ${label}`,
    ).toBeGreaterThan(0);
  }

  // D-BP-20: docked toasts must live inside the header toast dock — never
  // over the step content. The content column also keeps the full width:
  // its right edge stays aligned to the stepper's right edge (+/-8px).
  // The shell-clear check runs on every leg, toast or not.
  const dock = page.locator(".df2-toast-dock").first();
  const stepPanel = page.locator(".df2-transfer-step-viewport:visible").first();
  const stepper = page.locator(".df2-transfer-studio-chrome").first();
  const [dockBox, panelBox, stepperBox] = await Promise.all([
    dock.boundingBox(),
    stepPanel.boundingBox(),
    stepper.boundingBox(),
  ]);
  const vw = page.viewportSize()?.width ?? 1440;
  if (dockBox && panelBox && stepperBox) {
    if (vw > 1280) {
      // >1280px: dock lives in the header chrome row — pills must stay inside it.
      for (const t of toastBoxes) {
        expect(
          t.y >= dockBox.y - 4 &&
            t.y + t.height <= dockBox.y + dockBox.height + 4,
          `docked toast ${JSON.stringify(t)} must stay within the dock row on ${label}`,
        ).toBe(true);
      }
    }
    // <=1280px: the dock drops below the chrome as an overlay strip by design
    // — the card/footer intersection checks below still apply.
    const edgeDelta = Math.abs(panelBox.x + panelBox.width - (stepperBox.x + stepperBox.width));
    expect(
      edgeDelta,
      `content right edge must align to the stepper right edge (+/-8px) on ${label} (delta=${edgeDelta.toFixed(1)})`,
    ).toBeLessThanOrEqual(8);
  }
  // D-BP-23: the header stepper must never crush — every step label fully
  // rendered (no clipping), labels never overlap each other or a toast,
  // and the route chip stays readable, at every width.
  const stepLabels = page.locator(
    ".df2-transfer-studio-chrome .df2-wizard-studio .df2-wizard-label:visible, " +
      ".df2-transfer-studio-chrome .df2-wizard-studio .df2-wizard-label-short:visible",
  );
  const labelCount = await stepLabels.count();
  const labelBoxes: { x: number; y: number; width: number; height: number }[] = [];
  for (let i = 0; i < labelCount; i++) {
    const el = stepLabels.nth(i);
    const clipped = await el.evaluate(
      (n) => n.scrollWidth > n.clientWidth + 1,
    );
    expect(
      clipped,
      `step label ${i} must not be clipped on ${label}`,
    ).toBe(false);
    const box = await el.boundingBox();
    if (box) labelBoxes.push(box);
  }
  const labelTexts = await stepLabels.allTextContents();
  for (let i = 0; i < labelBoxes.length; i++) {
    for (let j = i + 1; j < labelBoxes.length; j++) {
      expect(
        intersects(labelBoxes[i], labelBoxes[j]),
        `step labels '${labelTexts[i]}'/'${labelTexts[j]}' must not overlap on ${label}: ${JSON.stringify(labelBoxes[i])} vs ${JSON.stringify(labelBoxes[j])}`,
      ).toBe(false);
    }
    for (const t of toastBoxes) {
      expect(
        intersects(labelBoxes[i], t),
        `step label ${i} must not sit under a toast on ${label}`,
      ).toBe(false);
    }
  }
  const routeChip = page.locator(".df2-route-bar:visible").first();
  if (await routeChip.isVisible().catch(() => false)) {
    const clipped = await routeChip.evaluate(
      (n) => n.scrollWidth > n.clientWidth + 1,
    );
    const box = await routeChip.boundingBox();
    expect(
      !clipped && box !== null && box.width >= 80,
      `route chip must be readable on ${label}`,
    ).toBe(true);
    // D-BP-25: the container being wide enough is not the contract — the
    // source/destination NAME spans must render in full.
    const routeLabels = routeChip.locator(".df2-route-bar-label");
    const rlCount = await routeLabels.count();
    for (let i = 0; i < rlCount; i++) {
      const el = routeLabels.nth(i);
      if (!(await el.isVisible().catch(() => false))) continue;
      const labelClipped = await el.evaluate(
        (n) => n.scrollWidth > n.clientWidth + 1,
      );
      const txt = ((await el.textContent()) || "").trim();
      expect(
        labelClipped,
        `route-bar name '${txt}' must not be truncated on ${label}`,
      ).toBe(false);
    }
  }

  // D-BP-26: footer status words must not truncate mid-word at >=1024 —
  // the CSS drops secondary detail spans instead of ellipsizing.
  const vpw = page.viewportSize()?.width ?? 0;
  if (vpw >= 1024) {
    const statusSpans = page.locator(
      ".df2-validate-footer-status:visible > span, " +
        ".df2-run-footer-status:visible > span, " +
        ".df2-map-footer-status:visible > span, " +
        ".df2-wizard-footer:visible > .df2-label-hint",
    );
    const sCount = await statusSpans.count();
    for (let i = 0; i < sCount; i++) {
      const el = statusSpans.nth(i);
      if (!(await el.isVisible().catch(() => false))) continue;
      const clipped = await el.evaluate(
        (n) => n.scrollWidth > n.clientWidth + 1,
      );
      const txt = ((await el.textContent()) || "").trim().slice(0, 48);
      expect(
        clipped,
        `footer status '${txt}' must not be truncated on ${label}`,
      ).toBe(false);
    }
  }

  if (toastBoxes.length === 0) {
    return; // shell asserted; no live toast to intersect
  }

  const targets = page.locator(
    ".df2-wizard-footer, .df2-run-footer, .df2-transfer-step-panel .df2-btn-primary, " +
      ".df2-empty-state, .df2-map-empty-state, .df2-card, " +
      ".df2-contract-bind, .df2-vd-metric",
  );
  const targetBoxes = await visibleBoxes(targets);
  expect(
    targetBoxes.length,
    `a footer / primary action / card must be on screen on ${label}`,
  ).toBeGreaterThan(0);
  for (const t of toastBoxes) {
    for (const b of targetBoxes) {
      expect(
        intersects(t, b),
        `toast ${JSON.stringify(t)} must not overlap ${JSON.stringify(b)} on ${label}`,
      ).toBe(false);
    }
  }
}

async function loadSample(page: Page) {
  await page
    .getByRole("button", { name: /Load sample orders CSV/ })
    .click();
}

test.describe("toast/footer clearance (D-BP-11)", () => {
  test.beforeEach(async ({ page }) => {
    // Hermetic: every endpoint the Source→Validate walk hits is stubbed from
    // captured fixtures — this spec must pass on plain `vite dev` (no API).
    await installHermeticStubs(page);
    await page.addInitScript(() => {
      localStorage.removeItem("df.studioSchemaBrowser");
      localStorage.removeItem("df.studioCanvas");
      localStorage.setItem("df2.theme", "light");
    });
  });

  for (const width of [1440, 1280, 1024, 390]) {
    test(`no toast overlap with footer, primary actions, or cards @${width}px`, async ({
      page,
    }) => {
      await runClearanceWalk(page, width, "");
    });
  }
});

// D-BP-24: the same clearance contract holds in canvas mode — docked toasts
// must live inside the header band at every width there too.
test.describe("toast/footer clearance — canvas mode (D-BP-24)", () => {
  test.beforeEach(async ({ page }) => {
    await installHermeticStubs(page);
    await page.addInitScript(() => {
      localStorage.setItem("df.studioCanvas", "1");
      localStorage.setItem("df2.theme", "light");
    });
  });

  for (const width of [1440, 1280, 1024, 390]) {
    test(`canvas: no toast overlap @${width}px`, async ({ page }) => {
      await runClearanceWalk(page, width, "canvas ");
    });
  }
});

async function runClearanceWalk(page: Page, width: number, mode: string) {
      await page.setViewportSize({ width, height: 900 });
      await openStudio(page);

      // Source step: the sample-file load fires "Source profiled", which used
      // to cover "Continue to Transform" in the wizard footer.
      await loadSample(page);
      await assertToastClearance(page, `${mode}Source @${width}px`);

      // P1-1: walk every classic step and assert the shell stays clear on
      // each — not only Source/Validate. The Validate leg stays HARD (the
      // spec must actually reach it); Run is asserted when reachable —
      // preview mode cannot execute a transfer, so a missing Run button is
      // reported, not hidden.
      const clickIf = async (re: RegExp, ms = 2500) => {
        const b = page.getByRole("button", { name: re }).first();
        if (await b.isVisible().catch(() => false)) {
          await b.click().catch(() => {});
          await page.waitForTimeout(ms);
          return true;
        }
        return false;
      };
      if (await clickIf(/Continue to Destination/)) {
        await assertToastClearance(page, `${mode}Destination @${width}px`, {
          requireToast: false,
        });
      }
      const fileTab = page.getByRole("tab", { name: /File export|File/ }).first();
      if (await fileTab.isVisible().catch(() => false)) {
        await fileTab.click();
        await page.waitForTimeout(400);
      }
      if (await clickIf(/Continue to Review|Continue to Transform/)) {
        await assertToastClearance(page, `${mode}Transform/Review @${width}px`, {
          requireToast: false,
        });
      }
      await clickIf(/Review result/);
      if (await clickIf(/Continue without transforming/)) {
        await assertToastClearance(page, `${mode}Map @${width}px`, {
          requireToast: false,
        });
      }
      const toValidate = page
        .getByRole("button", { name: /Continue to Validate/ })
        .first();
      await toValidate.waitFor({ state: "visible", timeout: 15_000 });
      await toValidate.click();
      // Validate panel + the contract bind field + at least one metric card
      // must be on screen, and the Validate toasts must be live. The leg
      // waits for the preflight toast specifically so it cannot pass on a
      // stale toast by timing (D-BP-24).
      await expect(page.locator(".df2-contract-bind").first()).toBeVisible({
        timeout: 15_000,
      });
      await expect(page.locator(".df2-vd-metric").first()).toBeVisible();
      await expect(
        page.locator(".dt-toast:visible", { hasText: /preflight|validat/i }).first(),
        `a preflight/validate toast must be shown on ${mode}Validate @${width}px`,
      ).toBeVisible({ timeout: 10_000 });
      await assertToastClearance(page, `${mode}Validate step @${width}px`);

      // Run leg: reachable only when the preview can advance — assert when
      // it lands, record plainly when it cannot.
      const toRun = page
        .getByRole("button", { name: /Continue to Run|Run transfer|Execute/i })
        .first();
      if (
        await toRun.isVisible().catch(() => false)
      ) {
        await toRun.click().catch(() => {});
        await page.waitForTimeout(1500);
        await assertToastClearance(page, `${mode}Run @${width}px`, {
          requireToast: false,
        });
      } else {
        test.info().annotations.push({
          type: "note",
          description: `Run step not reachable in preview @${width}px — leg skipped honestly`,
        });
      }
}
