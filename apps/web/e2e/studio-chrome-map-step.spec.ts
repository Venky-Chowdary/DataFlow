import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test, type Browser, type BrowserContext, type Page } from "@playwright/test";
import { fixture, stubMapStudio } from "./mapStepStubs";
import { openStudio, settleToasts } from "./support";

/**
 * Transfer Studio chrome with production-shaped data (W9): the Destination
 * mode tabs when this host refuses file exports, the Map step with a
 * false-friend pair, rows held for review and a 36-column destination, and
 * the stepper with very long source and destination names.
 *
 * Hermetic like the rest of the suite. The file name ends in map-step.spec.ts
 * so the CI "map-step" project runs it.
 */
const HERMETIC = join(dirname(fileURLToPath(import.meta.url)), "fixtures", "hermetic");
const hermetic = (name: string) => JSON.parse(readFileSync(join(HERMETIC, name), "utf8"));
const SAMPLE = join(dirname(fileURLToPath(import.meta.url)), "..", "public", "fixtures", "sample-orders.csv");
const json = (body: unknown) => ({ status: 200, contentType: "application/json", body: JSON.stringify(body) });

type View = { width: number; height: number };
const VIEWS: Record<string, View> = {
  w1920: { width: 1920, height: 1080 },
  w1440: { width: 1440, height: 900 },
  w1366: { width: 1366, height: 768 },
  w1280: { width: 1280, height: 720 },
  w1024: { width: 1024, height: 768 },
  w390: { width: 390, height: 844 },
};

const LONG_SOURCE = "Orders Postgres (demo) export of the quarterly order ledger 2026-Q3 final.csv";
const LONG_DEST = "QA Postgres Local — Catalog MongoDB Atlas (demo) analytics replica eu-west";
const LONG_TABLE = "mahi_qa_20261109_pr392_mongo_orders_catalog_snapshot_v2";

/** This host refuses file exports (the hosted cloud), as /transfer/capabilities says. */
async function fileExportRefused(page: Page): Promise<void> {
  const caps = hermetic("transfer_capabilities.json");
  caps.destination_file_formats_unavailable = ["csv", "json", "parquet"].map((id) => ({
    id,
    reason: "Not available on cloud",
    message: "File exports aren't available as a destination on the hosted cloud. Pick a database or warehouse.",
  }));
  await page.route("**/api/v1/transfer/capabilities", (route) => route.fulfill(json(caps)));
}

/**
 * The owner's Map: customer_email is a user≠customer false friend, three
 * rows are lossy and wait for a Risk Contract, and the destination table has
 * 36 columns (two NOT NULL with no source).
 */
async function busyMap(page: Page): Promise<void> {
  const map = hermetic("transfer_map.json");
  map.mappings = map.mappings.map((m: Record<string, unknown>) => {
    if (m.source === "customer_email") {
      return { ...m, review_kind: "entity_identity", requires_review: true, reasoning: "entity qualifier conflict: customer vs user" };
    }
    if (["order_amt", "order_date", "status"].includes(String(m.source))) {
      return { ...m, fidelity: "lossy_cast", conversion_class: "lossy", requires_risk_contract: true, requires_review: true };
    }
    return m;
  });
  await page.route("**/api/v1/transfer/map", (route) => route.fulfill(json(map)));
  const catalog = JSON.parse(fixture("destination_columns.json"));
  const nullable = catalog.columns[1];
  catalog.columns = [
    ...catalog.columns,
    ...Array.from({ length: 28 }, (_, i) => {
      const name = `warehouse_attribute_${String(i).padStart(2, "0")}_customer_lifetime`;
      return { ...nullable, name, match_key: name, ordinal: 8 + i };
    }),
  ];
  const intro = JSON.parse(fixture("transfer_introspect.json"));
  type Col = { name: string; type: string; nullable: boolean };
  intro.destination.columns = catalog.columns.map((c: Col) => c.name);
  intro.destination.schema = Object.fromEntries(catalog.columns.map((c: Col) => [c.name, c.type]));
  intro.destination.schema_nullability = Object.fromEntries(catalog.columns.map((c: Col) => [c.name, c.nullable]));
  await page.route("**/api/v1/transfer/introspect", (route) => route.fulfill(json(intro)));
  await page.route("**/api/v1/transfer/mapping/destination-columns", (route) => route.fulfill(json(catalog)));
}

/** A saved destination connection with a 70-character name. */
async function longDestinationName(page: Page): Promise<void> {
  const rename = (value: unknown): unknown => {
    if (Array.isArray(value)) return value.map(rename);
    if (value && typeof value === "object") {
      return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, v === "QA Postgres Local" ? LONG_DEST : rename(v)]));
    }
    return value;
  };
  const saved = rename(hermetic("connectors_saved.json"));
  await page.route("**/api/v1/connectors/saved*", (route) => route.fulfill(json(saved)));
}

interface Walk {
  view: View;
  theme?: "light" | "dark";
  setup?: Array<(page: Page) => Promise<void>>;
  long?: boolean;
  stopAt?: "destination" | "destination-picked" | "map";
}

async function walk(browser: Browser, opts: Walk): Promise<{ context: BrowserContext; page: Page }> {
  const context = await browser.newContext({ viewport: opts.view });
  await context.addInitScript((t) => localStorage.setItem("df2.theme", t), opts.theme ?? "light");
  const page = await context.newPage();
  await stubMapStudio(page);
  for (const setup of opts.setup ?? []) await setup(page);
  if (opts.long) await longDestinationName(page);
  await openStudio(page);
  await page.addStyleTag({ content: ".dt-toast, .dt-toast-more { display: none !important; }" });
  if (opts.long) {
    await page.locator('input[type="file"]').first().setInputFiles({
      name: LONG_SOURCE, mimeType: "text/csv", buffer: readFileSync(SAMPLE),
    });
  } else {
    await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
  }
  await page.getByRole("button", { name: /Continue to Destination/ }).click();
  if (opts.stopAt === "destination") {
    await page.waitForLoadState("networkidle");
    return { context, page };
  }
  await page.getByRole("radio", { name: /QA Postgres Local/ }).click();
  await page.locator("#dest-col").fill(opts.long ? LONG_TABLE : "orders_dest");
  await page.locator("#dest-col").press("Enter");
  await page.locator("#dest-col").blur();
  await page.waitForLoadState("networkidle");
  if (opts.stopAt === "destination-picked") return { context, page };
  const next = page.getByRole("button", { name: /Continue to Transform/ });
  await expect(next).toBeEnabled();
  await next.click();
  await page.getByRole("button", { name: /Continue without transforming/ }).click();
  await expect(page.getByRole("toolbar", { name: "Mapping actions" })).toBeVisible();
  await settleToasts(page);
  await page.waitForLoadState("networkidle");
  return { context, page };
}

/* ---- 1. Destination mode tabs ------------------------------------------- */

test.describe("Destination mode tabs when this host refuses file exports (W9)", () => {
  for (const key of ["w1920", "w1440", "w1366", "w1280", "w1024", "w390"]) {
    test(`${key}: short fixed labels, the refused tab is disabled, the reason is said once under the control`, async ({ browser }) => {
      const { context, page } = await walk(browser, { view: VIEWS[key], setup: [fileExportRefused], stopAt: "destination" });
      const tabs = page.getByRole("tablist", { name: "Destination mode" });
      await expect(tabs.getByRole("tab")).toHaveText(["Database", "File export"]);
      const fileTab = tabs.getByRole("tab", { name: "File export", exact: true });
      await expect(fileTab).toHaveAttribute("aria-disabled", "true");
      await expect(fileTab).toHaveAttribute("title", /Not available on cloud/);
      await fileTab.click({ force: true });
      await expect(fileTab).toHaveAttribute("aria-selected", "false");
      await expect(tabs.getByRole("tab", { name: "Database", exact: true })).toHaveAttribute("aria-selected", "true");

      // The reason, once, as a one-line hint under the control (and the tab's description).
      const hint = page.getByTestId("dest-mode-hint");
      await expect(hint).toBeVisible();
      await expect(hint).toContainText("Not available on cloud");
      await expect(page.locator(".df2-dest-step").getByText(/Not available on cloud/)).toHaveCount(1);
      const describedBy = await fileTab.getAttribute("aria-describedby");
      expect(describedBy && (await page.locator(`#${describedBy}`).count())).toBeTruthy();

      const geometry = await tabs.evaluate((list) => {
        const box = list.getBoundingClientRect();
        const items = [...list.querySelectorAll('[role="tab"]')].map((t) => {
          const r = t.getBoundingClientRect();
          return { text: (t as HTMLElement).innerText, l: r.left, r: r.right, t: r.top, b: r.bottom, cut: t.scrollWidth > t.clientWidth + 1 };
        });
        const hint = document.querySelector('[data-testid="dest-mode-hint"]') as HTMLElement | null;
        return {
          list: { l: box.left, r: box.right, t: box.top, b: box.bottom },
          items,
          hintLines: hint ? Math.round(hint.getBoundingClientRect().height / parseFloat(getComputedStyle(hint).lineHeight)) : 0,
          pageScrollsX: document.documentElement.scrollWidth > innerWidth + 1,
        };
      });
      for (const item of geometry.items) {
        expect(item.cut, `${key}: tab "${item.text}" is cut`).toBe(false);
        expect(item.l >= geometry.list.l - 1 && item.r <= geometry.list.r + 1, `${key}: tab "${item.text}" leaves its track`).toBe(true);
      }
      const [a, b] = geometry.items;
      expect(Math.min(a.r, b.r) - Math.max(a.l, b.l) <= 1 || Math.min(a.b, b.b) - Math.max(a.t, b.t) <= 1, `${key}: the tabs overlap`).toBe(true);
      expect(geometry.hintLines, `${key}: the hint is one line`).toBeLessThanOrEqual(key === "w390" ? 2 : 1);
      expect(geometry.pageScrollsX, `${key}: the page scrolls sideways`).toBe(false);
      await context.close();
    });
  }
});

/* ---- 2. Map step: one banner, one scroll region per pane ---------------- */

async function mapScrollReport(page: Page) {
  return page.evaluate(() => {
    const overflowY = (el: Element) => el.scrollHeight > el.clientHeight + 1;
    const scrolls = (el: Element) => /auto|scroll/.test(getComputedStyle(el).overflowY) && overflowY(el)
      && (el as HTMLElement).offsetParent !== null;
    const label = (el: Element) => `${el.tagName.toLowerCase()}.${String((el as HTMLElement).className).trim().split(/\s+/).slice(0, 2).join(".")}`;
    const step = document.querySelector(".df2-map-step-panel")!;
    const containers = [document.documentElement, document.querySelector("main"), step, step.querySelector(".df2-ms-scroll")]
      .filter((n): n is Element => Boolean(n))
      .filter((n) => overflowY(n) && (n === document.documentElement || /auto|scroll|hidden/.test(getComputedStyle(n).overflowY)))
      .map((n) => `${label(n)} ${n.scrollHeight}>${n.clientHeight}`);
    const panes = [step.querySelector(".df2-column-review-editor"), step.querySelector(".df2-ms-panel")]
      .filter((n): n is Element => Boolean(n));
    const perPane = panes.map((pane) => ({
      pane: label(pane),
      scrollers: [pane, ...pane.querySelectorAll("*")].filter(scrolls).map(label),
    }));
    // A vertical scrollbar is the strip between the client box and the border box.
    const underBars: string[] = [];
    const controls = 'button, input, select, [role="tab"], .df2-ms-status, .df2-ms-tag, .df2-ms-chip, .df2-ms-compat, .df2-badge';
    for (const scroller of [...step.querySelectorAll("*")].filter(scrolls)) {
      const r = scroller.getBoundingClientRect();
      const cs = getComputedStyle(scroller);
      const barLeft = r.left + parseFloat(cs.borderLeftWidth) + scroller.clientWidth;
      const barRight = r.right - parseFloat(cs.borderRightWidth);
      const strip = barRight - barLeft >= 1 ? { l: barLeft, r: barRight, t: r.top, b: r.bottom }
        // Overlay scrollbars take no layout space: they paint over the last 12 px.
        : { l: r.right - parseFloat(cs.borderRightWidth) - 12, r: r.right - parseFloat(cs.borderRightWidth), t: r.top, b: r.bottom };
      for (const c of scroller.querySelectorAll(controls)) {
        const b = c.getBoundingClientRect();
        if (b.width < 1 || b.height < 1) continue;
        const w = Math.min(b.right, strip.r) - Math.max(b.left, strip.l);
        const h = Math.min(b.bottom, strip.b) - Math.max(b.top, strip.t);
        if (w > 0.5 && h > 0.5) underBars.push(`${label(c)} "${(c as HTMLElement).innerText?.trim().slice(0, 30)}" under the scrollbar of ${label(scroller)}`);
      }
    }
    const primary = document.querySelector(".df2-map-footer .df2-btn-primary")!.getBoundingClientRect();
    const banners = [...step.querySelectorAll(".df2-map-issues, .df2-column-review-alert")].filter((b) => (b as HTMLElement).offsetParent !== null);
    return {
      overflowing: containers,
      perPane,
      underBars: underBars.slice(0, 10),
      primaryInView: primary.top >= 0 && primary.bottom <= innerHeight && primary.left >= 0 && primary.right <= innerWidth,
      banners: banners.map((b) => (b as HTMLElement).innerText.replace(/\s+/g, " ").trim()),
    };
  });
}

test.describe("Map step with two kinds of open work and a 36-column destination (W9)", () => {
  for (const key of ["w1440", "w1366", "w1280", "w1024"]) {
    test(`${key}: one banner, the step never scrolls, one scrollbar per pane, none over a control`, async ({ browser }) => {
      const { context, page } = await walk(browser, { view: VIEWS[key], setup: [busyMap] });
      const toggle = page.getByRole("toolbar", { name: "Mapping actions" }).getByRole("button", { name: "Show destination columns" });
      if (await toggle.isVisible()) await toggle.click();
      await expect(page.getByRole("complementary", { name: /Destination columns of/ })).toBeVisible();
      // The grid shows every row, so it has something to scroll.
      await page.getByRole("tablist", { name: "Filter columns" }).getByRole("tab", { name: /^All/ }).click();

      const report = await mapScrollReport(page);
      expect(report.banners, `${key}: one banner for everything that is open`).toHaveLength(1);
      const banner = report.banners[0];
      expect(banner).toContain("1 false-friend pair needs confirming");
      expect(banner).toContain("3 columns need review");
      expect(banner, `${key}: no "(s)" plurals`).not.toMatch(/\(s\)/);
      expect(report.overflowing, `${key}: the step or the page scrolls`).toEqual([]);
      for (const pane of report.perPane) {
        expect(pane.scrollers.length, `${key}: ${pane.pane} has ${pane.scrollers.join(", ")}`).toBeLessThanOrEqual(1);
      }
      expect(report.underBars, `${key}: a scrollbar covers a control`).toEqual([]);
      expect(report.primaryInView, `${key}: Continue to Validate is out of view`).toBe(true);
      await context.close();
    });
  }
});

/* ---- 3. Stepper with long names ----------------------------------------- */

async function stepperReport(page: Page) {
  return page.evaluate(() => {
    const chrome = document.querySelector(".df2-transfer-studio-chrome")!.getBoundingClientRect();
    const steps = [...document.querySelectorAll('nav[aria-label="Transfer steps"] .df2-wizard-step')].map((s) => {
      const r = s.getBoundingClientRect();
      return { name: s.getAttribute("aria-label"), top: Math.round(r.top), bottom: Math.round(r.bottom) };
    });
    const bar = document.querySelector(".df2-route-bar") as HTMLElement | null;
    const barBox = bar?.getBoundingClientRect();
    const labels = bar ? [...bar.querySelectorAll(".df2-route-bar-label")].map((l) => ({
      text: (l as HTMLElement).innerText,
      title: l.getAttribute("title"),
      truncated: l.scrollWidth > l.clientWidth + 1,
      ellipsis: getComputedStyle(l).textOverflow === "ellipsis",
    })) : [];
    const head = document.querySelector(".df2-map-step-head, .df2-transfer-step-panel");
    return {
      cardHeight: Math.round(chrome.height),
      steps,
      bar: barBox ? { top: Math.round(barBox.top), height: Math.round(barBox.height), name: bar!.getAttribute("aria-label") ?? "" } : null,
      stepsBottom: Math.max(...steps.map((s) => s.bottom)),
      labels,
      gapBelow: head ? Math.round(head.getBoundingClientRect().top - chrome.bottom) : null,
      pageScrollsX: document.documentElement.scrollWidth > innerWidth + 1,
    };
  });
}

test.describe("Stepper with very long source and destination names (W9)", () => {
  test("1440 baseline: the stepper card height with short names", async ({ browser }) => {
    const { context, page } = await walk(browser, { view: VIEWS.w1440 });
    const report = await stepperReport(page);
    // The single-row card was 66 px before the redesign; the budget is +8 px.
    expect(report.cardHeight, "stepper card height, short names").toBeLessThanOrEqual(74);
    await context.close();
  });

  for (const key of ["w1920", "w1440", "w1280", "w1024"]) {
    test(`${key}: six steps on one line, the route on one slim line under them`, async ({ browser }) => {
      const { context, page } = await walk(browser, { view: VIEWS[key], long: true });
      const report = await stepperReport(page);
      const tops = new Set(report.steps.map((s) => s.top));
      expect(report.steps, `${key}: six steps`).toHaveLength(6);
      expect(Math.max(...tops) - Math.min(...tops), `${key}: the steps wrap ${JSON.stringify(report.steps)}`).toBeLessThanOrEqual(2);
      expect(report.bar, `${key}: no route line`).not.toBeNull();
      expect(report.bar!.top, `${key}: the route line sits under the steps`).toBeGreaterThanOrEqual(report.stepsBottom - 1);
      expect(report.bar!.height, `${key}: the route line is one slim line`).toBeLessThanOrEqual(28);
      for (const label of report.labels) {
        expect(label.ellipsis, `${key}: "${label.text}" does not end in an ellipsis`).toBe(true);
        expect(label.title, `${key}: "${label.text}" has no full-value tooltip`).toBeTruthy();
      }
      expect(report.labels.map((l) => l.title).join(" "), `${key}: tooltips`).toContain(LONG_SOURCE);
      expect(report.labels.map((l) => l.title).join(" "), `${key}: tooltips`).toContain(LONG_DEST);
      expect(report.bar!.name, `${key}: the route's accessible name`).toContain(LONG_SOURCE);
      expect(report.bar!.name, `${key}: the route's accessible name`).toContain(LONG_TABLE);
      if (key === "w1440") expect(report.cardHeight, "stepper card height, long names").toBeLessThanOrEqual(74);
      expect(report.gapBelow!, `${key}: band under the stepper`).toBeLessThanOrEqual(16);
      expect(report.pageScrollsX).toBe(false);
      await context.close();
    });
  }

  test("1440: no destination yet reads 'Choose destination' on the same slim line", async ({ browser }) => {
    const { context, page } = await walk(browser, { view: VIEWS.w1440, long: true, stopAt: "destination" });
    const report = await stepperReport(page);
    expect(new Set(report.steps.map((s) => s.top)).size).toBe(1);
    expect(report.bar!.height).toBeLessThanOrEqual(28);
    await expect(page.locator(".df2-route-bar")).toContainText("Choose destination");
    expect(report.cardHeight).toBeLessThanOrEqual(74);
    await context.close();
  });

  test("390: the compact rail keeps all six steps on one row", async ({ browser }) => {
    const { context, page } = await walk(browser, { view: VIEWS.w390, long: true });
    const report = await stepperReport(page);
    expect(new Set(report.steps.map((s) => s.top)).size, JSON.stringify(report.steps)).toBe(1);
    expect(report.pageScrollsX).toBe(false);
    for (const label of report.labels) expect(label.title).toBeTruthy();
    await context.close();
  });
});

/* ---- W9 round 2: one step bar on every step; a header that does not repeat
   the route; the panel's table name on one line ------------------------- */

/** The destination table does not exist yet, so the walk can reach Validate. */
async function newTable(page: Page): Promise<void> {
  const intro = JSON.parse(fixture("transfer_introspect.json"));
  intro.destination = { ...intro.destination, table_exists: false, columns: [], schema: {}, schema_nullability: {},
    objects: [{ name: "customers", type: "table" }] };
  const catalog = { ...JSON.parse(fixture("destination_columns.json")), table_exists: false, columns: [] };
  await page.route("**/api/v1/transfer/introspect", (route) => route.fulfill(json(intro)));
  await page.route("**/api/v1/transfer/mapping/destination-columns", (route) => route.fulfill(json(catalog)));
}

async function stepLabelLefts(page: Page): Promise<Record<string, number>> {
  return page.evaluate(() => {
    const out: Record<string, number> = {};
    for (const step of document.querySelectorAll('nav[aria-label="Transfer steps"] .df2-wizard-step')) {
      const label = [...step.querySelectorAll(".df2-wizard-label, .df2-wizard-label-short")]
        .find((l) => getComputedStyle(l).display !== "none")!;
      out[step.getAttribute("aria-label") ?? "?"] = label.getBoundingClientRect().left;
    }
    return out;
  });
}

test.describe("One step bar on every step (W9 round 2)", () => {
  for (const key of ["w1440", "w1366", "w1024"]) {
    test(`${key}: every step label keeps its x position from Source to Validate, toasts included`, async ({ browser }) => {
      const context = await browser.newContext({ viewport: VIEWS[key] });
      await context.addInitScript(() => localStorage.setItem("df2.theme", "light"));
      const page = await context.newPage();
      await stubMapStudio(page);
      await newTable(page);
      await openStudio(page);
      // Toasts stay on: one arriving must not move the steps.
      const seen: Array<[string, Record<string, number>]> = [];
      const record = async (where: string) => {
        await page.waitForLoadState("networkidle");
        seen.push([where, await stepLabelLefts(page)]);
      };
      await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
      await expect(page.getByRole("button", { name: /Continue to Destination/ })).toBeEnabled();
      await record("Source");
      await page.getByRole("button", { name: /Continue to Destination/ }).click();
      await expect(page.getByRole("radio", { name: /QA Postgres Local/ })).toBeVisible();
      await record("Destination");
      await page.getByRole("radio", { name: /QA Postgres Local/ }).click();
      await page.locator("#dest-col").fill("orders_new");
      await page.locator("#dest-col").press("Enter");
      await page.locator("#dest-col").blur();
      await page.waitForLoadState("networkidle");
      await page.getByRole("button", { name: /Continue to Transform/ }).click();
      await expect(page.getByRole("button", { name: /Continue without transforming/ })).toBeVisible();
      await record("Transform");
      await page.getByRole("button", { name: /Continue without transforming/ }).click();
      await expect(page.getByRole("toolbar", { name: "Mapping actions" })).toBeVisible();
      await record("Map");
      await page.getByRole("button", { name: /Approve eligible/ }).click();
      await page.getByRole("button", { name: /Continue to Validate/ }).click();
      await expect(page.locator('[aria-current="step"]')).toHaveAttribute("aria-label", "5. Validate");
      await record("Validate");

      const [first, base] = seen[0];
      for (const [where, lefts] of seen.slice(1)) {
        for (const [name, x] of Object.entries(lefts)) {
          expect(Math.abs(x - base[name]), `${key}: "${name}" moved ${(x - base[name]).toFixed(1)}px from ${first} to ${where}`)
            .toBeLessThanOrEqual(1);
        }
      }
      await context.close();
    });
  }
});

async function headerReport(page: Page) {
  return page.evaluate(() => {
    const head = document.querySelector(".df2-map-step-head")!;
    const title = head.querySelector(".df2-card-title");
    const copy = head.querySelector(".df2-map-step-head-copy")!.getBoundingClientRect();
    const toolbar = head.querySelector(".df2-ms-toolbar")!.getBoundingClientRect();
    const route = head.querySelector(".df2-map-route") as HTMLElement | null;
    const lineHeight = route ? parseFloat(getComputedStyle(route).lineHeight) : 0;
    const titleBox = title && getComputedStyle(title).display !== "none" ? title.getBoundingClientRect() : copy;
    return {
      height: Math.round(head.getBoundingClientRect().height),
      toolbarTop: Math.round(toolbar.top),
      titleTop: Math.round(titleBox.top),
      routeText: route?.innerText ?? "",
      routeTitle: route?.getAttribute("title") ?? "",
      routeLines: route ? Math.round(route.getBoundingClientRect().height / lineHeight) : 0,
      panelSub: (() => {
        const sub = document.querySelector(".df2-ms-panel-sub") as HTMLElement | null;
        if (!sub) return null;
        return {
          lines: Math.round(sub.getBoundingClientRect().height / parseFloat(getComputedStyle(sub).lineHeight)),
          title: sub.getAttribute("title") ?? "",
          ellipsis: getComputedStyle(sub).textOverflow === "ellipsis",
        };
      })(),
    };
  });
}

test.describe("Map header and panel with very long names (W9 round 2)", () => {
  for (const key of ["w1440", "w1366", "w1024"]) {
    test(`${key}: the header says only table facts on one line; the toolbar stays on the title row`, async ({ browser }) => {
      const short = await walk(browser, { view: VIEWS[key] });
      const shortReport = await headerReport(short.page);
      await short.context.close();

      const { context, page } = await walk(browser, { view: VIEWS[key], long: true });
      const toggle = page.getByRole("toolbar", { name: "Mapping actions" }).getByRole("button", { name: "Show destination columns" });
      if (await toggle.isVisible()) await toggle.click();
      const report = await headerReport(page);
      expect(report.routeText, `${key}: the header repeats the route`).not.toContain("Orders Postgres");
      expect(report.routeText).not.toContain(LONG_TABLE);
      expect(report.routeText).toMatch(/existing table · 8 columns · /);
      expect(report.routeTitle, `${key}: the full route stays in the title`).toContain(LONG_SOURCE);
      expect(report.routeLines, `${key}: header subtitle lines`).toBe(1);
      expect(Math.abs(report.toolbarTop - report.titleTop), `${key}: the toolbar wrapped under the subtitle`).toBeLessThanOrEqual(4);
      expect(Math.abs(report.height - shortReport.height), `${key}: header ${report.height}px vs ${shortReport.height}px with short names`)
        .toBeLessThanOrEqual(4);
      expect(report.panelSub, `${key}: the panel's table name`).not.toBeNull();
      expect(report.panelSub!.lines, `${key}: the panel's table name wraps`).toBe(1);
      expect(report.panelSub!.ellipsis).toBe(true);
      expect(report.panelSub!.title).toContain(LONG_TABLE);
      await expect(page.getByRole("complementary", { name: new RegExp(LONG_TABLE) })).toBeVisible();
      await context.close();
    });
  }

  test("390: the header subtitle is at most two lines", async ({ browser }) => {
    const { context, page } = await walk(browser, { view: VIEWS.w390, long: true });
    const report = await headerReport(page);
    expect(report.routeLines).toBeLessThanOrEqual(2);
    expect(report.routeText).not.toContain("Orders Postgres");
    await context.close();
  });
});
