import { expect, test, type Page } from "@playwright/test";
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

interface SnapBox {
  x: number;
  y: number;
  width: number;
  height: number;
}

interface ClearanceSnap {
  dock: SnapBox | null;
  panel: SnapBox | null;
  stepper: SnapBox | null;
  toasts: SnapBox[];
  labels: { box: SnapBox; clipped: boolean; text: string }[];
  routeChip: { box: SnapBox; clipped: boolean } | null;
  routeLabels: { clipped: boolean; text: string }[];
  footerStatuses: { clipped: boolean; text: string }[];
  targets: SnapBox[];
}

// One DOM frame, one measurement. Bounding boxes used to be read through a
// dozen separate protocol round-trips — a toast exiting or the dock band
// reflowing between two calls mixed geometry from different frames and
// false-failed at the wrap widths. Everything the assertions touch is
// snapshotted in a single evaluate.
async function snapshotClearance(page: Page): Promise<ClearanceSnap> {
  return page.evaluate(() => {
    type B = { x: number; y: number; width: number; height: number };
    const rectOf = (el: Element): B => {
      const r = el.getBoundingClientRect();
      return { x: r.x, y: r.y, width: r.width, height: r.height };
    };
    const shown = (el: Element): boolean => {
      const cs = getComputedStyle(el);
      if (cs.display === "none" || cs.visibility === "hidden") return false;
      const r = el.getBoundingClientRect();
      return r.width > 0 && r.height > 0;
    };
    const firstBox = (sel: string): B | null => {
      const el = document.querySelector(sel);
      return el && shown(el) ? rectOf(el) : null;
    };
    // Overlap targets only: a bounding box reports layout geometry, not
    // painted pixels — inside an overflow-clipping ancestor, content beyond
    // the clip edge is not painted. Clip each target to its clipping
    // ancestors so overlap measures visible pixels. Toasts must NOT be
    // clipped this way: a toast clipped by the dock's overflow is exactly
    // the D-BP-27 defect the containment check asserts on its unclipped box.
    const painted = (el: Element): B | null => {
      const r = el.getBoundingClientRect();
      let left = r.left, top = r.top, right = r.right, bottom = r.bottom;
      let cur = el.parentElement;
      while (cur) {
        const cs = getComputedStyle(cur);
        if (
          [cs.overflowX, cs.overflowY].some(
            (o) => o === "hidden" || o === "auto" || o === "scroll" || o === "clip",
          )
        ) {
          const cr = cur.getBoundingClientRect();
          const bl = parseFloat(cs.borderLeftWidth) || 0;
          const bt = parseFloat(cs.borderTopWidth) || 0;
          const br = parseFloat(cs.borderRightWidth) || 0;
          const bb = parseFloat(cs.borderBottomWidth) || 0;
          left = Math.max(left, cr.left + bl);
          top = Math.max(top, cr.top + bt);
          right = Math.min(right, cr.right - br);
          bottom = Math.min(bottom, cr.bottom - bb);
        }
        cur = cur.parentElement;
      }
      if (right <= left || bottom <= top) return null;
      return { x: left, y: top, width: right - left, height: bottom - top };
    };
    const clipped = (el: Element): boolean =>
      el.scrollWidth > el.clientWidth + 1;

    const labels = Array.from(
      document.querySelectorAll(
        ".df2-transfer-studio-chrome .df2-wizard-studio .df2-wizard-label, " +
          ".df2-transfer-studio-chrome .df2-wizard-studio .df2-wizard-label-short",
      ),
    )
      .filter(shown)
      .map((el) => ({
        box: rectOf(el),
        clipped: clipped(el),
        text: (el.textContent || "").trim(),
      }));

    const chip = document.querySelector(".df2-route-bar");
    const routeChip =
      chip && shown(chip) ? { box: rectOf(chip), clipped: clipped(chip) } : null;
    const routeLabels = chip
      ? Array.from(chip.querySelectorAll(".df2-route-bar-label"))
          .filter(shown)
          .map((el) => ({
            clipped: clipped(el),
            text: (el.textContent || "").trim(),
          }))
      : [];

    const footerStatuses = Array.from(
      document.querySelectorAll(
        ".df2-validate-footer-status > span, .df2-run-footer-status > span, " +
          ".df2-map-footer-status > span, .df2-wizard-footer > .df2-label-hint",
      ),
    )
      .filter(shown)
      .map((el) => ({
        clipped: clipped(el),
        text: (el.textContent || "").trim().slice(0, 48),
      }));

    const targets = Array.from(
      document.querySelectorAll(
        ".df2-wizard-footer, .df2-run-footer, " +
          ".df2-transfer-step-panel .df2-btn-primary, .df2-empty-state, " +
          ".df2-map-empty-state, .df2-card, .df2-contract-bind, .df2-vd-metric",
      ),
    )
      .filter(shown)
      .map(painted)
      .filter((b): b is B => b !== null); // same set as OVERLAP_TARGETS_SEL

    return {
      dock: firstBox(".df2-toast-dock"),
      panel: firstBox(".df2-transfer-step-viewport"),
      stepper: firstBox(".df2-transfer-studio-chrome"),
      toasts: Array.from(document.querySelectorAll(".dt-toast"))
        .filter(shown)
        .map(rectOf),
      labels,
      routeChip,
      routeLabels,
      footerStatuses,
      targets,
    };
  });
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
  // The toast's mount and the step's content land on different frames — a
  // snapshot right after the toast appears can precede the footer/cards.
  // Wait for a VISIBLE overlap target (the selector's first DOM match can be
  // a hidden control like "Apply read window") so the leg is rendered.
  await expect(
    page.locator(`${OVERLAP_TARGETS_SEL} >> visible=true`).first(),
    `a footer / primary action / card must render on ${label}`,
  ).toBeVisible({ timeout: 15_000 });
  const snap = await snapshotClearance(page);
  const { dock: dockBox, panel: panelBox, stepper: stepperBox } = snap;
  const toastBoxes = snap.toasts;
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
  const vw = page.viewportSize()?.width ?? 1440;
  if (dockBox && panelBox && stepperBox) {
    // D-BP-27: at EVERY width, each visible toast box must be fully inside
    // the dock band's box — left, right, top, and bottom. Partial visibility
    // (a toast clipped off the lane edge) is a defect, not a layout.
    for (const t of toastBoxes) {
      expect(
        t.x >= dockBox.x - 1 &&
          t.y >= dockBox.y - 1 &&
          t.x + t.width <= dockBox.x + dockBox.width + 1 &&
          t.y + t.height <= dockBox.y + dockBox.height + 1,
        `toast ${JSON.stringify(t)} must be fully inside the dock band ${JSON.stringify(dockBox)} on ${label}`,
      ).toBe(true);
    }
    const edgeDelta = Math.abs(panelBox.x + panelBox.width - (stepperBox.x + stepperBox.width));
    expect(
      edgeDelta,
      `content right edge must align to the stepper right edge (+/-8px) on ${label} (delta=${edgeDelta.toFixed(1)})`,
    ).toBeLessThanOrEqual(8);
  }
  // D-BP-23: the header stepper must never crush — every step label fully
  // rendered (no clipping), labels never overlap each other or a toast,
  // and the route chip stays readable, at every width.
  const labelBoxes = snap.labels.map((l) => l.box);
  const labelTexts = snap.labels.map((l) => l.text);
  for (let i = 0; i < snap.labels.length; i++) {
    expect(
      snap.labels[i].clipped,
      `step label ${i} must not be clipped on ${label}`,
    ).toBe(false);
  }
  // D-BP-27: at <=480 the dock lane stacks above the stepper — keep >=8px
  // clearance between the lane bottom and the first step pill's top.
  if (vw <= 480 && dockBox && labelBoxes.length > 0) {
    const stepsTop = Math.min(...labelBoxes.map((b) => b.y));
    const gap = stepsTop - (dockBox.y + dockBox.height);
    expect(
      gap >= 8,
      `docked toast lane must keep >=8px clearance above the stepper on ${label} (gap=${gap.toFixed(1)})`,
    ).toBe(true);
  }
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
  if (snap.routeChip) {
    expect(
      !snap.routeChip.clipped && snap.routeChip.box.width >= 80,
      `route chip must be readable on ${label}`,
    ).toBe(true);
    // D-BP-25: the container being wide enough is not the contract — the
    // source/destination NAME spans must render in full.
    for (const rl of snap.routeLabels) {
      expect(
        rl.clipped,
        `route-bar name '${rl.text}' must not be truncated on ${label}`,
      ).toBe(false);
    }
  }

  // D-BP-26: footer status words must not truncate mid-word at >=1024 —
  // the CSS drops secondary detail spans instead of ellipsizing.
  const vpw = page.viewportSize()?.width ?? 0;
  if (vpw >= 1024) {
    for (const s of snap.footerStatuses) {
      expect(
        s.clipped,
        `footer status '${s.text}' must not be truncated on ${label}`,
      ).toBe(false);
    }
  }

  if (toastBoxes.length === 0) {
    return; // shell asserted; no live toast to intersect
  }

  expect(
    snap.targets.length,
    `a footer / primary action / card must be on screen on ${label}`,
  ).toBeGreaterThan(0);
  for (const t of toastBoxes) {
    for (const b of snap.targets) {
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

// Geometry specs must not read mid-animation boxes: dt-toast-in slides a
// toast in with translateX(12px), and a measurement during that flight
// reports a box hanging past the dock edge — the intermittent 1280/1024
// false-failures. Freeze animation/transition to 0s for the whole run.
function freezeAnimations(page: Page) {
  return page.addInitScript(() => {
    const apply = () => {
      const style = document.createElement("style");
      style.textContent =
        "*,*::before,*::after{animation-duration:0s!important;" +
        "animation-delay:0s!important;transition-duration:0s!important;" +
        "transition-delay:0s!important}";
      (document.head || document.documentElement).appendChild(style);
    };
    if (document.head) apply();
    else document.addEventListener("DOMContentLoaded", apply, { once: true });
  });
}

const OVERLAP_TARGETS_SEL =
  ".df2-wizard-footer, .df2-run-footer, " +
  ".df2-transfer-step-panel .df2-btn-primary, .df2-empty-state, " +
  ".df2-map-empty-state, .df2-card, .df2-contract-bind, .df2-vd-metric";

test.describe("toast/footer clearance (D-BP-11)", () => {
  test.beforeEach(async ({ page }) => {
    // Hermetic: every endpoint the Source→Validate walk hits is stubbed from
    // captured fixtures — this spec must pass on plain `vite dev` (no API).
    await installHermeticStubs(page);
    await freezeAnimations(page);
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
    await freezeAnimations(page);
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
