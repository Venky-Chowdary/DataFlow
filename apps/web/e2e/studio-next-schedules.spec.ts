import { execFileSync } from "node:child_process";
import { expect, test, type Locator, type Page } from "@playwright/test";
import { expectDisjoint } from "./layout";

const WIDTHS = [
  { width: 768, height: 1024 },
  { width: 1280, height: 800 },
  { width: 1366, height: 768 },
  { width: 1440, height: 900 },
  { width: 1920, height: 1080 },
] as const;

const schedule = {
  id: "sched-orders",
  name: "Nightly orders",
  source_connector_id: "src-1",
  source_table: "orders",
  dest_connector_id: "dst-1",
  dest_table: "warehouse_orders",
  interval: "daily",
  cadence_label: "Daily",
  cron: "",
  timezone: "UTC",
  sync_mode: "incremental",
  validation_mode: "strict",
  schema_policy: "manual_review",
  backfill_new_fields: false,
  cursor_column: "updated_at",
  primary_key: "id",
  cursor_value: "",
  workspace_id: "ws",
  max_retries: 2,
  retry_backoff_seconds: 30,
  notify_on_failure: true,
  notify_on_success: false,
  enabled: true,
  last_run_at: "2026-10-14T02:00:00Z",
  next_run_at: "2026-10-15T02:00:00Z",
  last_job_id: "job-parent-11",
  last_status: "completed",
  run_count: 4,
  running: false,
  created_at: "2026-10-01T00:00:00Z",
  contract_id: "contract-orders",
};

async function readBody(response: { ok: () => boolean; status: () => number; text: () => Promise<string> }): Promise<{ ok: boolean; status: number; text: string; json: Record<string, unknown> }> {
  const text = await response.text();
  let json: Record<string, unknown> = {};
  try {
    json = JSON.parse(text) as Record<string, unknown>;
  } catch {
    json = {};
  }
  return { ok: response.ok(), status: response.status(), text, json };
}

async function enterSchedules(page: Page): Promise<void> {
  await page.goto("/#/schedules");
  const preview = page.locator("button", { hasText: "Preview workspace (UI only)" });
  await expect(preview).toBeAttached();
  await preview.evaluate((node: HTMLButtonElement) => node.click());
  await expect(page.getByRole("heading", { name: "Schedules", exact: true })).toBeVisible();
}

async function visibleControls(root: Locator): Promise<Locator[]> {
  const controls = root.locator("button, summary, a, input, .df2-badge");
  const count = await controls.count();
  const visible: Locator[] = [];
  for (let index = 0; index < count; index += 1) {
    const control = controls.nth(index);
    if (await control.isVisible()) visible.push(control);
  }
  return visible;
}

async function expectControlsDisjoint(page: Page, controls: Locator[]): Promise<void> {
  for (let left = 0; left < controls.length; left += 1) {
    for (let right = left + 1; right < controls.length; right += 1) {
      await expectDisjoint(page, controls[left], controls[right]);
    }
  }
}

test.describe("studio schedules geometry", () => {
  test.beforeEach(async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.setItem("df.studioCanvas", "1");
      localStorage.setItem("df2.theme", "light");
    });
    await page.route("**/api/v1/workspace/settings", async (route) => {
      if (route.request().method() !== "GET") {
        await route.continue();
        return;
      }
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          org_name: "Datawrap",
          timezone: "UTC",
          retention_days: 90,
          studio_schema_browser: false,
          studio_canvas: true,
        }),
      });
    });
    await page.route("**/api/v1/schedules/**", async (route) => {
      const url = route.request().url();
      const method = route.request().method();
      if (method === "GET" && /\/history/.test(url)) {
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            runs: [
              {
                job_id: "job-parent-11",
                status: "completed",
                attempt: 0,
                started_at: "2026-10-14T02:00:00Z",
                records_transferred: 4,
              },
            ],
          }),
        });
        return;
      }
      if (method === "GET" && /\/schedules\/?$/.test(url.split("?")[0])) {
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify([schedule]),
        });
        return;
      }
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(schedule),
      });
    });
  });

  test("studio schedules stay disjoint at 768, 1280, 1366, 1440, and 1920 and Classic UI restores the pipeline row", async ({ page }) => {
    await enterSchedules(page);
    await expect(page.getByTestId("schedules-studio")).toBeVisible();
    await expect(page.getByRole("button", { name: "Classic UI" })).toBeVisible();
    await expect(page.getByText("Nightly orders")).toBeVisible();
    const row = page.getByTestId("sched-row-sched-orders");
    for (const size of WIDTHS) {
      await page.setViewportSize(size);
      await expect(row).toBeVisible();
      const shell = page.getByTestId("schedules-studio");
      const overflow = await shell.evaluate((node) => node.scrollWidth <= node.clientWidth + 1);
      expect(overflow, `horizontal overflow at ${size.width}`).toBe(true);
      await expectControlsDisjoint(page, await visibleControls(page.locator(".df2-sched-studio-toolbar")));
      await expectControlsDisjoint(page, await visibleControls(row));
      const box = await row.boundingBox();
      expect(box, "studio schedule row").toBeTruthy();
      expect(box!.x).toBeGreaterThanOrEqual(-1);
      expect(box!.x + box!.width).toBeLessThanOrEqual(size.width + 1);
      if (size.width === 1440 || size.width === 768) {
        await page.screenshot({
          path: `/opt/cursor/artifacts/schedules-studio-${size.width}.png`,
          fullPage: false,
        });
      }
    }
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.getByRole("button", { name: "Classic UI" }).click();
    await expect(page.locator(".df2-pipeline-row").first()).toBeVisible();
    await expect(page.getByTestId("schedules-studio")).toHaveCount(0);
    await expect(page.getByRole("button", { name: "Sync now" }).first()).toBeVisible();
  });
});

test.describe("studio schedules live API", () => {
  test("studio schedules create, enable, disable, run, and open the job on the local API", async ({ page }) => {
    const api = process.env.DATAFLOW_E2E_API || "http://127.0.0.1:8011";
    test.setTimeout(180_000);
    const stamp = Date.now();
    const name = `Nightly orders ${stamp}`;
    const sourcePath = `/tmp/df-sched-src-${stamp}.db`;
    const destPath = `/tmp/df-sched-dst-${stamp}.db`;
    execFileSync("python3", ["-c", `
import sqlite3
src = sqlite3.connect(${JSON.stringify(sourcePath)})
src.execute("create table orders (id integer primary key, email text, status text, amount real, ordered_on text)")
src.executemany(
    "insert into orders values (?,?,?,?,?)",
    [
        (1001, "alex@example.com", "open", 249.90, "2026-10-01"),
        (1002, "priya@example.com", "open", 89.50, "2026-10-01"),
        (1003, "sam@example.com", "held", 12.00, "2026-10-02"),
        (1004, "lee@example.com", "open", 40.00, "2026-10-02"),
        (1005, "nina@example.com", "open", 8.25, "2026-10-03"),
    ],
)
src.commit()
src.close()
sqlite3.connect(${JSON.stringify(destPath)}).close()
`]);

    const health = await readBody(await page.request.get(`${api}/api/v1/workspace/settings`));
    expect(health.ok, health.text).toBeTruthy();
    const previous = health.json;
    const turnedOn = await readBody(await page.request.patch(`${api}/api/v1/workspace/settings`, {
      data: { ...previous, studio_canvas: true, studio_schema_browser: false },
    }));
    expect(turnedOn.ok, turnedOn.text).toBeTruthy();

    const source = await readBody(await page.request.post(`${api}/api/v1/connectors/`, {
      data: {
        name: `Orders source ${stamp}`,
        type: "sqlite",
        host: "localhost",
        port: 0,
        database: sourcePath,
        role: "source",
      },
    }));
    expect(source.ok, source.text).toBeTruthy();
    const sourceId = String(source.json.id || "");
    const dest = await readBody(await page.request.post(`${api}/api/v1/connectors/`, {
      data: {
        name: `Orders dest ${stamp}`,
        type: "sqlite",
        host: "localhost",
        port: 0,
        database: destPath,
        role: "destination",
      },
    }));
    expect(dest.ok, dest.text).toBeTruthy();
    const destId = String(dest.json.id || "");
    expect(sourceId && destId).toBeTruthy();

    await page.addInitScript((base) => {
      localStorage.setItem("df.studioCanvas", "1");
      localStorage.setItem("df2.theme", "light");
      (window as unknown as { DATAWRAP_API_BASE: string }).DATAWRAP_API_BASE = base;
    }, `${api}/api/v1`);
    await enterSchedules(page);
    await expect(page.getByTestId("schedules-studio")).toBeVisible();

    await page.getByRole("button", { name: "New schedule" }).click();
    await page.locator("#sched-name").fill(name);
    await page.locator("#sched-src").selectOption(sourceId);
    await page.locator("#sched-dst").selectOption(destId);
    await page.locator("#sched-src-table").fill("orders");
    await page.locator("#sched-dst-table").fill("warehouse_orders");
    const created = page.waitForResponse(
      (response) =>
        response.request().method() === "POST"
        && /\/api\/v1\/schedules\/?$/.test(response.url().split("?")[0]),
    );
    await page.getByRole("button", { name: "Save schedule" }).click();
    const createdBody = await readBody(await created);
    expect(createdBody.ok, createdBody.text).toBeTruthy();
    const scheduleId = String(createdBody.json.id || "");
    expect(scheduleId).not.toBe("");

    const mapped = await readBody(await page.request.patch(`${api}/api/v1/schedules/${scheduleId}`, {
      data: {
        mappings: [
          { source: "id", target: "id", confidence: 1 },
          { source: "email", target: "email", confidence: 1 },
          { source: "status", target: "status", confidence: 1 },
          { source: "amount", target: "amount", confidence: 1 },
          { source: "ordered_on", target: "ordered_on", confidence: 1 },
        ],
        primary_key: "id",
        sync_mode: "full_refresh_overwrite",
      },
    }));
    expect(mapped.ok, mapped.text).toBeTruthy();

    await page.goto("/#/schedules");
    await expect(page.getByRole("heading", { name: "Schedules", exact: true })).toBeVisible();
    const row = page.getByTestId(`sched-row-${scheduleId}`);
    await expect(row).toBeVisible();
    await expect(row.getByText(name)).toBeVisible();

    const enable = page.waitForResponse(
      (response) => response.request().method() === "PATCH" && response.url().includes(scheduleId),
    );
    await row.getByRole("button", { name: "Enable" }).click();
    const enabled = await readBody(await enable);
    expect(enabled.ok, enabled.text).toBeTruthy();
    expect(enabled.json.enabled).toBe(true);
    await expect(row.getByRole("button", { name: "Pause" })).toBeVisible();

    const disable = page.waitForResponse(
      (response) => response.request().method() === "PATCH" && response.url().includes(scheduleId),
    );
    await row.getByRole("button", { name: "Pause" }).click();
    const paused = await readBody(await disable);
    expect(paused.ok, paused.text).toBeTruthy();
    expect(paused.json.enabled).toBe(false);

    const history = page.waitForResponse(
      (response) => response.request().method() === "GET" && response.url().includes(`/${scheduleId}/history`),
    );
    await row.getByRole("button", { name: "History" }).click();
    const historyResponse = await readBody(await history);
    expect(historyResponse.ok, historyResponse.text).toBeTruthy();

    let nativeDialog = false;
    page.on("dialog", async (dialog) => {
      nativeDialog = true;
      await dialog.dismiss();
    });
    const sync = page.waitForResponse(
      (response) => response.request().method() === "POST" && response.url().includes(`/${scheduleId}/sync`),
    );
    await row.getByRole("button", { name: "Sync now" }).click();
    const syncResponse = await readBody(await sync);
    expect(syncResponse.status, syncResponse.text).toBeLessThan(500);
    if (syncResponse.ok) {
      const dialog = page.getByRole("dialog", { name: "Sync now" });
      await expect(dialog).toBeVisible();
      await dialog.getByRole("button", { name: "Cancel" }).click();
      await expect(dialog).toBeHidden();
    } else {
      await expect(page.locator(".dt-toast").last()).toBeVisible();
    }
    expect(nativeDialog).toBe(false);

    const run = page.waitForResponse(
      (response) => response.request().method() === "POST" && response.url().includes(`/${scheduleId}/run`),
    );
    await row.getByRole("button", { name: "Run now" }).click();
    const runResponse = await readBody(await run);
    expect(runResponse.ok, runResponse.text).toBeTruthy();
    const jobId = String(runResponse.json.job_id || "");
    expect(jobId).not.toBe("");
    await expect(page).toHaveURL(/#\/jobs/);
    await expect(page.locator(`#job-item-${jobId}`)).toBeVisible();

    const restore = await readBody(await page.request.patch(`${api}/api/v1/workspace/settings`, {
      data: { ...previous, studio_canvas: previous.studio_canvas === true },
    }));
    expect(restore.ok, restore.text).toBeTruthy();
  });
});
