import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import type { Page } from "@playwright/test";

/**
 * Hermetic studio stubs — the spec must pass on a plain `vite dev` server
 * with NO API running. Every endpoint the Source→Validate walk hits is
 * fulfilled from a captured real response in ./fixtures/hermetic
 * (bodies recorded from a live preview session; contracts trimmed to 3).
 * A catch-all 404 keeps any unlisted API call honest instead of hanging.
 */
const FIXTURE_DIR = join(dirname(fileURLToPath(import.meta.url)), "fixtures", "hermetic");

function fixture(name: string): string {
  return readFileSync(join(FIXTURE_DIR, name), "utf8");
}

const json = (body: string, status = 200) => ({
  status,
  contentType: "application/json",
  body,
});

export async function installHermeticStubs(page: Page): Promise<void> {
  // Playwright matches routes in reverse registration order — the catch-all
  // goes FIRST so the specific stubs below always win.
  await page.route("**/api/**", async (route) => {
    await route.fulfill(json('{"detail":"not stubbed"}', 404));
  });
  const stubs: Array<[string, string, number]> = [
    ["**/api/v1/auth/sso/providers", "auth_sso_providers.json", 200],
    ["**/api/v1/auth/me", "__401__", 401],
    ["**/api/v1/team/workspaces", "team_workspaces.json", 200],
    ["**/api/v1/workspace/settings", "workspace_settings.json", 200],
    ["**/api/v1/transfer/capabilities", "transfer_capabilities.json", 200],
    ["**/api/v1/connectors/upload", "connectors_upload.json", 200],
    ["**/api/v1/schedules*", "schedules.json", 200],
    ["**/api/v1/connectors/jobs*", "connectors_jobs.json", 200],
    ["**/api/v1/connectors/saved*", "connectors_saved.json", 200],
    ["**/api/v1/transfer/analyze", "transfer_analyze.json", 200],
    ["**/api/v1/shape/catalog*", "shape_catalog.json", 200],
    ["**/api/v1/transform-library/*", "transform_library.json", 200],
    ["**/api/v1/shape/profile", "shape_profile.json", 200],
    ["**/api/v1/shape/preview", "shape_preview.json", 200],
    ["**/api/v1/ai/analyze/enhanced", "ai_analyze_enhanced.json", 200],
    ["**/api/v1/transfer/map", "transfer_map.json", 200],
    ["**/api/v1/preflight/preview-cells", "preflight_preview_cells.json", 200],
    ["**/api/v1/contracts*", "contracts.json", 200],
    ["**/api/v1/transfer/*/proof-pack*", "__404__", 404],
  ];
  for (const [pattern, name, status] of stubs) {
    await page.route(pattern, async (route) => {
      const body =
        name === "__401__"
          ? '{"detail":"Not authenticated"}'
          : name === "__404__"
            ? '{"detail":"No proof pack yet"}'
            : fixture(name);
      await route.fulfill(json(body, status));
    });
  }
  // Plan-scoped routes (the UI reuses the id POST /transfer/plans returns).
  await page.route("**/api/v1/transfer/plans/*/mappings*", async (route) => {
    await route.fulfill(json(fixture("plan_mappings.json")));
  });
  await page.route("**/api/v1/transfer/plans/*/preflight*", async (route) => {
    const name =
      route.request().method() === "POST" ? "plan_preflight_post.json" : "plan_preflight_get.json";
    await route.fulfill(json(fixture(name), route.request().method() === "POST" ? 202 : 200));
  });
  await page.route("**/api/v1/transfer/plans/*/approve*", async (route) => {
    await route.fulfill(json(fixture("plan_approve.json")));
  });
  await page.route("**/api/v1/transfer/plans", async (route) => {
    if (route.request().method() !== "POST") {
      await route.fulfill(json("[]"));
      return;
    }
    await route.fulfill(json(fixture("transfer_plans.json")));
  });
}
