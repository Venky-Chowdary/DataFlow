import { expect, test, type Page } from "@playwright/test";
import { installHermeticStubs } from "./hermetic";
import { openStudio } from "./support";

async function schemaReviewHarness(page: Page, origin: string, fixture: string) {
  const failures: string[] = [];
  const blocked: string[] = [];
  page.on("pageerror", error => failures.push(error.message));
  await page.route("**/*", async route => {
    const url = new URL(route.request().url());
    if (url.origin !== origin || url.pathname.startsWith("/api/")) {
      blocked.push(url.pathname);
      return route.abort();
    }
    if (url.pathname !== "/__qa_schema_review") return route.continue();
    await route.fulfill({ contentType: "text/html", body: `<!doctype html>
      <html><head><meta name="viewport" content="width=device-width, initial-scale=1">
      <link rel="stylesheet" href="/src/styles/marketing-base.css?direct">
      <link rel="stylesheet" href="/src/styles/app-styles.css?direct"></head>
      <body><main id="root" class="df2-app" style="display:block;min-height:100vh;padding:16px"></main>
      <script type="module">
        import RefreshRuntime from '/@react-refresh';
        RefreshRuntime.injectIntoGlobalHook(window);
        window.$RefreshReg$ = () => {};
        window.$RefreshSig$ = () => (type) => type;
        window.__vite_plugin_react_preamble_installed__ = true;
        const React = (await import('/node_modules/.vite/deps/react.js')).default;
        const { createRoot } = (await import('/node_modules/.vite/deps/react-dom_client.js')).default;
        const { SchemaDriftDialog, schemaDriftReportFromDetails } = await import('/src/components/SchemaDriftDialog.tsx');
        const { ValidateDashboard } = await import('/src/components/transfer/ValidateDashboard.tsx');
        const { ToastProvider } = await import('/src/components/Toast.tsx');
        const { ConfirmProvider } = await import('/src/components/ui/ConfirmDialog.tsx');
        const { Dialog } = await import('/src/components/ui/Dialog.tsx');
        const { StructurePreview } = await import('/src/components/ui/StructurePreview.tsx');
        const element = React.createElement;
        const fixture = new URL(location.href).searchParams.get('fixture');
        const hard = new URL(location.href).searchParams.get('hard');
        const additive = Array.from({ length: 18 }, (_, index) => ({
          kind: 'add_column', column: 'enterprise_customer_account_' + 'unbroken_identifier_'.repeat(5) + index,
          new_type: 'ARRAY<STRUCT<' + 'very_long_nested_type_definition_'.repeat(4) + '>>'
        }));
        const details = { remediation_kind: 'acknowledge_schema_drift', ack_required: true,
          schema_evolution: { action: 'manual_review', additive, hard_breaking: [], should_pause: false, compatibility: 'backward' } };
        if (hard === 'nested') details.schema_evolution.hard_breaking = [{ kind: 'type_change', column: 'customer_id' }];
        if (hard === 'pause') details.schema_evolution.should_pause = true;
        if (hard === 'none') details.schema_evolution.compatibility = 'none';
        if (hard === 'evolution') details.schema_evolution.hard_breaking = [
          { kind: 'type_change', column: 'amount', old_type: 'DECIMAL(18,2)', new_type: 'TEXT' },
          { kind: 'key_change', column: 'customer_id' }
        ];
        const message = 'Synthetic schema drift requires operator review';
        const report = schemaDriftReportFromDetails(details, message);
        const preflight = { passed: false, passed_count: 0, total_gates: 1, readiness_score: 0,
          run_id: 'isolated-schema-review', gates: [{ id: 'schema_drift', status: 'block', message, details }],
          blockers: [{ id: 'schema_drift', message, details }] };
        function Fixture() {
          const [open, setOpen] = React.useState(false);
          const [counts, setCounts] = React.useState({ acknowledge: 0, approve: 0, reject: 0, remap: 0 });
          const record = key => setCounts(previous => ({ ...previous, [key]: previous[key] + 1 }));
          return element(React.Fragment, null,
            element('output', { 'data-testid': 'decisions' }, JSON.stringify(counts)),
            fixture === 'dashboard' ? element(ToastProvider, null, element(ConfirmProvider, null,
              element(ValidateDashboard, { preflight, running: false,
                onAcknowledgeSchemaDrift: () => record('acknowledge'), onReviewMappings: () => record('remap') }))) :
              element(React.Fragment, null,
                element('button', { onClick: () => setOpen(true) }, 'Open isolated review'),
                fixture === 'keyboard' ? element(Dialog, { open, onClose: () => setOpen(false), title: 'Structure keyboard review', size: 'lg' },
                  element(StructurePreview, { columns: Array.from({ length: 30 }, (_, index) => 'field_' + index),
                    rows: [], expandable: false, showFieldStrip: true })) :
                  element(SchemaDriftDialog, { open, report, onClose: () => setOpen(false),
                    onAcknowledge: () => { record('acknowledge'); setOpen(false); },
                    onApproveAdditive: () => record('approve'), onReject: () => record('reject'),
                    onRemap: () => { record('remap'); setOpen(false); } })));
        }
        createRoot(document.getElementById('root')).render(element(Fixture));
      </script></body></html>` });
  });
  await page.goto(`${origin}/__qa_schema_review?${fixture}`);
  await expect.poll(async () => {
    if (failures.length) throw new Error(failures.join("\n"));
    return page.getByTestId("decisions").count();
  }).toBe(1);
  return { failures, blocked };
}

test.describe("isolated schema review", () => {
  for (const viewport of [{ width: 1440, height: 1000 }, { width: 390, height: 844 }]) {
    for (const scenario of ["dashboard", "layout", "hard", "keyboard", "dashboard-hard-evolution"]) {
      test(`${scenario} ${viewport.width}x${viewport.height}`, async ({ playwright, baseURL }, testInfo) => {
        const browser = await playwright.chromium.launch({ channel: process.env.QA_BROWSER_CHANNEL ?? "msedge" });
        const page = await browser.newPage({ viewport });
        const origin = new URL(baseURL ?? "http://127.0.0.1:5173").origin;
        const zero = { acknowledge: 0, approve: 0, reject: 0, remap: 0 };
        const decisions = () => page.getByTestId("decisions").evaluate(node => JSON.parse(node.textContent || "{}"));
        try {
          const evidence = await schemaReviewHarness(page, origin, scenario === "dashboard-hard-evolution"
            ? "fixture=dashboard&hard=evolution" : `fixture=${scenario === "hard" ? "dialog" : scenario}`);
          const dialog = page.getByRole("dialog");
          const trigger = page.getByRole("button", { name: scenario === "dashboard" ? "Review schema change" : "Open isolated review", exact: true });
          if (scenario === "dashboard") {
            for (const dismiss of ["Cancel", "Close", "Escape"]) {
              await trigger.click();
              await expect(dialog).toBeVisible();
              expect(await decisions()).toEqual(zero);
              if (dismiss === "Escape") await page.keyboard.press("Escape");
              else await dialog.getByRole("button", { name: dismiss, exact: true }).click();
              await expect(dialog).toHaveCount(0);
              await expect(trigger).toBeFocused();
              expect(await decisions()).toEqual(zero);
            }
            await trigger.click();
            await page.screenshot({ path: testInfo.outputPath(`schema-dashboard-${viewport.width}x${viewport.height}.png`), animations: "disabled" });
            await dialog.getByRole("button", { name: "Acknowledge for this run", exact: true }).click();
            await expect(dialog).toHaveCount(0);
            expect(await decisions()).toEqual({ ...zero, acknowledge: 1 });
            await trigger.click();
            await dialog.getByRole("button", { name: "Open remapping", exact: true }).click();
            expect(await decisions()).toEqual({ ...zero, acknowledge: 1, remap: 1 });
          } else if (scenario === "dashboard-hard-evolution") {
            const review = page.getByRole("button", { name: "Review breaking change", exact: true });
            await expect(review).toBeVisible();
            await expect(page.locator("details[open]")).toHaveCount(0);
            await expect(page.getByRole("button", { name: "Review schema change", exact: true })).toHaveCount(0);
            expect(await decisions()).toEqual(zero);
            await review.click();
            await expect(dialog).toBeVisible();
            await expect(dialog).toBeFocused();
            await expect(dialog.getByRole("status")).toContainText("Mapping review required");
            await expect(dialog.getByRole("button", { name: /Acknowledge for this run|Approve additive/ })).toHaveCount(0);
            expect(await decisions()).toEqual(zero);
            await page.keyboard.press("Tab");
            await expect(dialog.getByRole("button", { name: "Close", exact: true })).toBeFocused();
            await page.keyboard.press("Shift+Tab");
            await expect(dialog.getByRole("button", { name: "Open remapping", exact: true })).toBeFocused();
            const geometry = () => dialog.evaluate(panel => {
              const header = panel.querySelector(".df2-modal-header")!.getBoundingClientRect();
              const footer = panel.querySelector(".df2-modal-footer")!.getBoundingClientRect();
              const body = panel.querySelector(".df2-modal-body")!;
              const bodyBox = body.getBoundingClientRect();
              const panelBox = panel.getBoundingClientRect();
              return { headerTop: header.top, footerTop: footer.top, scrollTop: body.scrollTop,
                overflow: Math.max(panel.scrollWidth - panel.clientWidth, body.scrollWidth - body.clientWidth, document.documentElement.scrollWidth - innerWidth),
                contained: panelBox.left >= 0 && panelBox.right <= innerWidth + 1 && panelBox.top >= 0 && panelBox.bottom <= innerHeight + 1,
                separated: header.bottom <= bodyBox.top + 1 && bodyBox.bottom <= footer.top + 1 };
            });
            await dialog.locator(".df2-modal-body").evaluate(body => { body.scrollTop = 0; });
            const before = await geometry();
            await page.screenshot({ path: testInfo.outputPath(`schema-dashboard-hard-top-${viewport.width}x${viewport.height}.png`), animations: "disabled" });
            for (const field of ["amount", "customer_id"]) {
              const row = dialog.locator("tbody tr").filter({ has: page.getByText(field, { exact: true }) });
              await row.scrollIntoViewIfNeeded();
              await expect(row.locator('td[data-label="Field"]')).toHaveText(field);
              await expect(row.locator('td[data-label="Field"]')).toBeVisible();
              await expect(row.locator('td[data-label="Change"]')).toHaveText(field === "amount" ? "type change" : "key change");
              for (const label of ["Current", "Proposed"]) {
                await expect(row.locator(`td[data-label="${label}"]`)).toBeVisible();
              }
              if (field === "amount") {
                await expect(row.locator('td[data-label="Current"]')).toHaveText("DECIMAL(18,2)");
                await expect(row.locator('td[data-label="Proposed"]')).toHaveText("TEXT");
              }
            }
            await dialog.locator(".df2-modal-body").evaluate(body => { body.scrollTop = body.scrollHeight; });
            const after = await geometry();
            expect(after.scrollTop).toBeGreaterThan(0);
            expect(after.headerTop).toBeCloseTo(before.headerTop, 0);
            expect(after.footerTop).toBeCloseTo(before.footerTop, 0);
            for (const snapshot of [before, after]) {
              expect(snapshot.overflow).toBeLessThanOrEqual(1);
              expect(snapshot.contained).toBe(true);
              expect(snapshot.separated).toBe(true);
            }
            await expect(dialog.getByRole("button", { name: "Open remapping", exact: true })).toBeInViewport();
            await page.screenshot({ path: testInfo.outputPath(`schema-dashboard-hard-bottom-${viewport.width}x${viewport.height}.png`), animations: "disabled" });
            await page.keyboard.press("Escape");
            await expect(dialog).toHaveCount(0);
            await expect(review).toBeFocused();
            expect(await decisions()).toEqual(zero);
            await review.click();
            await dialog.getByRole("button", { name: "Open remapping", exact: true }).click();
            await expect(dialog).toHaveCount(0);
            expect(await decisions()).toEqual({ ...zero, remap: 1 });
            await expect(page.locator("details[open]")).toHaveCount(0);
          } else if (scenario === "layout") {
            await trigger.click();
            await expect(dialog.locator("tbody tr")).toHaveCount(18);
            await expect(dialog.locator("tbody tr").first()).toContainText("very_long_nested_type_definition");
            await expect(dialog.getByRole("button", { name: "Approve additive", exact: true })).toHaveCount(0);
            await page.screenshot({ path: testInfo.outputPath(`schema-dialog-top-${viewport.width}x${viewport.height}.png`), animations: "disabled" });
            const geometry = () => dialog.evaluate(panel => {
              const header = panel.querySelector(".df2-modal-header")!.getBoundingClientRect();
              const footer = panel.querySelector(".df2-modal-footer")!.getBoundingClientRect();
              const body = panel.querySelector(".df2-modal-body")!;
              const bodyBox = body.getBoundingClientRect();
              const panelBox = panel.getBoundingClientRect();
              const clipped = Array.from(panel.querySelectorAll("td, td code, button, .df2-drift-kind")).filter(node => node.scrollWidth > node.clientWidth + 1).map(node => node.textContent);
              const overlaps = Array.from(panel.querySelectorAll(".df2-drift-actions button")).some((button, index, buttons) => buttons.slice(index + 1).some(other => {
                const first = button.getBoundingClientRect(), second = other.getBoundingClientRect();
                return Math.min(first.right, second.right) - Math.max(first.left, second.left) > 1
                  && Math.min(first.bottom, second.bottom) - Math.max(first.top, second.top) > 1;
              }));
              const code = panel.querySelector("tbody td:nth-child(2) code")!;
              const range = document.createRange();
              range.selectNodeContents(code);
              const proposedRange = document.createRange();
              proposedRange.selectNodeContents(panel.querySelector('tbody td[data-label="Proposed"] code')!);
              return { headerTop: header.top, footerTop: footer.top, bodyHeight: body.clientHeight,
                scrollHeight: body.scrollHeight, scrollTop: body.scrollTop, clipped, overlaps,
                wrappedLines: range.getClientRects().length, proposedLines: proposedRange.getClientRects().length,
                overflow: Math.max(panel.scrollWidth - panel.clientWidth, body.scrollWidth - body.clientWidth, document.documentElement.scrollWidth - innerWidth),
                contained: panelBox.left >= 0 && panelBox.right <= innerWidth + 1 && panelBox.top >= 0 && panelBox.bottom <= innerHeight + 1,
                separated: header.bottom <= bodyBox.top + 1 && bodyBox.bottom <= footer.top + 1 };
            });
            const before = await geometry();
            await dialog.locator(".df2-modal-body").evaluate(body => { body.scrollTop = body.scrollHeight; });
            const after = await geometry();
            await page.screenshot({ path: testInfo.outputPath(`schema-dialog-bottom-${viewport.width}x${viewport.height}.png`), animations: "disabled" });
            expect(before.scrollHeight).toBeGreaterThan(before.bodyHeight);
            expect(after.scrollTop).toBeGreaterThan(0);
            expect(after.headerTop).toBeCloseTo(before.headerTop, 0);
            expect(after.footerTop).toBeCloseTo(before.footerTop, 0);
            for (const snapshot of [before, after]) {
              expect(snapshot.overflow).toBeLessThanOrEqual(1);
              expect(snapshot.clipped).toEqual([]);
              expect(snapshot.overlaps).toBe(false);
              expect(snapshot.contained).toBe(true);
              expect(snapshot.separated).toBe(true);
              expect(snapshot.wrappedLines).toBeGreaterThan(1);
              expect(snapshot.proposedLines).toBeGreaterThan(1);
            }
            await dialog.getByRole("button", { name: "Cancel", exact: true }).click();
            expect(await decisions()).toEqual(zero);
          } else if (scenario === "hard") {
            for (const hard of ["nested", "pause", "none"]) {
              await page.goto(`${origin}/__qa_schema_review?fixture=dialog&hard=${hard}`);
              await trigger.click();
              await expect(dialog.getByRole("status")).toContainText("Mapping review required");
              await expect(dialog.getByRole("button", { name: /Acknowledge for this run|Approve additive/ })).toHaveCount(0);
              await page.screenshot({ path: testInfo.outputPath(`schema-hard-${hard}-${viewport.width}x${viewport.height}.png`), animations: "disabled" });
              await dialog.getByRole("button", { name: "Cancel", exact: true }).click();
              expect(await decisions()).toEqual(zero);
              await page.goto(`${origin}/__qa_schema_review?fixture=dashboard&hard=${hard}`);
              await expect(page.getByTestId("decisions")).toBeVisible();
              await expect(page.getByRole("button", { name: "Review schema change", exact: true })).toHaveCount(0);
              await expect(page.getByRole("button", { name: /Acknowledge for this run|Approve additive/ })).toHaveCount(0);
              expect(await decisions()).toEqual(zero);
            }
          } else {
            await trigger.click();
            await expect(dialog).toBeFocused();
            const close = dialog.getByRole("button", { name: "Close", exact: true });
            const summary = dialog.locator("summary");
            await expect(dialog.locator("details")).not.toHaveAttribute("open");
            await expect(dialog.getByRole("button", { name: "+6 more", exact: true })).toBeHidden();
            await page.keyboard.press("Tab");
            await expect(close).toBeFocused();
            await page.keyboard.press("Tab");
            await expect(summary).toBeFocused();
            await page.keyboard.press("Tab");
            await expect(close).toBeFocused();
            await page.keyboard.press("Shift+Tab");
            await expect(summary).toBeFocused();
            await page.screenshot({ path: testInfo.outputPath(`schema-closed-summary-${viewport.width}x${viewport.height}.png`), animations: "disabled" });
            await page.keyboard.press("Escape");
            await expect(dialog).toHaveCount(0);
            await expect(trigger).toBeFocused();
            expect(await decisions()).toEqual(zero);
          }
          expect(evidence.failures).toEqual([]);
          expect(evidence.blocked.filter(path => path !== "/api/v1/preflight/explain")).toEqual([]);
        } finally {
          await browser.close();
        }
      });
    }
  }
});

test.describe("isolated real-component entity icons", () => {
  for (const viewport of [{ width: 1440, height: 1000 }, { width: 390, height: 844 }]) {
    test(`entity icons and schema rows ${viewport.width}x${viewport.height}`, async ({ playwright, baseURL }, testInfo) => {
      const browser = await playwright.chromium.launch({ channel: process.env.QA_BROWSER_CHANNEL ?? "msedge" });
      const page = await browser.newPage({ viewport });
      const origin = new URL(baseURL ?? "http://127.0.0.1:5173").origin;
      const failures: string[] = [];
      page.on("pageerror", (error) => failures.push(error.message));
      try {
        await page.route("**/*", async (route) => {
          const url = new URL(route.request().url());
          if (url.origin !== origin || url.pathname.startsWith("/api/")) return route.abort();
          if (url.pathname !== "/__qa_entity_icons") return route.continue();
          await route.fulfill({ contentType: "text/html", body: `<!doctype html>
            <html><head><meta name="viewport" content="width=device-width, initial-scale=1">
            <link rel="stylesheet" href="/src/styles/marketing-base.css?direct">
            <link rel="stylesheet" href="/src/styles/app-styles.css?direct"></head>
            <body><main id="root" class="df2-app" style="display:block;min-height:100vh;padding:16px"></main>
            <script type="module">
              import RefreshRuntime from '/@react-refresh';
              RefreshRuntime.injectIntoGlobalHook(window);
              window.$RefreshReg$ = () => {};
              window.$RefreshSig$ = () => (type) => type;
              window.__vite_plugin_react_preamble_installed__ = true;
              const React = (await import('/node_modules/.vite/deps/react.js')).default;
              const { createRoot } = (await import('/node_modules/.vite/deps/react-dom_client.js')).default;
              const { DtIcon } = await import('/src/components/DtIcon.tsx');
              const { StructurePreview } = await import('/src/components/ui/StructurePreview.tsx');
              const { SchemaBrowser } = await import('/src/components/query/SchemaBrowser.tsx');
              const element = React.createElement;
              const names = ['cloud', 'database', 'file', 'folder', 'folder-open', 'table'];
              const objects = [
                { name: 'orders', type: 'table', columns: [{ name: 'id', type: 'integer', primaryKey: true, nullable: false }] },
                { name: 'enterprise_customer_account_archive', type: 'view', columns: [{ name: 'name', type: 'text' }] },
                { name: 'events', type: 'collection', columns: [{ name: 'payload', type: 'json' }] }
              ];
              const noop = () => {};
              createRoot(document.getElementById('root')).render(element(React.Fragment, null,
                element('h1', { style: { fontSize: 18 } }, 'Isolated QA fixture: production components, synthetic data'),
                element('section', { 'data-testid': 'icon-matrix', style: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: 12, marginBottom: 20 } },
                  ...names.flatMap(name => [16, 20, 24].flatMap(size => ['nav', 'product'].map(chrome =>
                    element('div', { key: name + size + chrome, 'data-icon-size': size, 'data-icon-name': name, 'data-chrome': chrome, style: { display: 'flex', gap: 6, alignItems: 'center', fontSize: 11, minHeight: 28 } },
                      element(DtIcon, { name, size, chrome }), element('span', { style: { minWidth: 0, flex: 1, overflowWrap: 'anywhere' } }, name + ' ' + size + ' ' + chrome)))))),
                element('section', { 'data-testid': 'preview' }, element(StructurePreview, {
                  title: 'Orders', columns: ['id', 'name'], schema: { id: 'integer', name: 'string' },
                  rows: [{ id: 1, name: 'QA customer' }, { id: 2, name: 'Synthetic account' }], allowJson: true, expandable: false
                })),
                ...['studio', 'query'].map(density => element('section', { key: density, 'data-testid': 'schema-' + density, style: { marginTop: 20 } },
                  element(SchemaBrowser, { objects, density, connected: true, onExpand: noop, onRefresh: noop, onInsert: noop, onPreview: noop, onUseObject: noop })))
              ));
            </script></body></html>` });
        });
        await page.goto(`${origin}/__qa_entity_icons`);
        await expect.poll(async () => {
          if (failures.length) throw new Error(failures.join("\n"));
          return page.getByTestId("icon-matrix").locator("svg").count();
        }).toBe(36);
        await page.evaluate(() => document.fonts.ready);
        await page.screenshot({ path: testInfo.outputPath(`entity-icons-${viewport.width}x${viewport.height}.png`), fullPage: true });
        const matrix = page.getByTestId("icon-matrix");
        for (const name of ["cloud", "database", "file", "folder", "folder-open", "table"]) {
          for (const icon of await matrix.locator(`[data-icon-name="${name}"] svg`).all()) {
            await expect(icon).toHaveClass(/lucide/);
          }
        }
        const sizes = await matrix.locator("svg").evaluateAll((icons) => icons.map((icon) => {
          const box = icon.getBoundingClientRect();
          const drawing = (icon as SVGSVGElement).getBBox();
          return { requested: Number(icon.parentElement?.closest("[data-icon-size]")?.getAttribute("data-icon-size")), width: box.width, height: box.height, inkWidth: drawing.width, inkHeight: drawing.height };
        }));
        for (const size of sizes) {
          expect(size.width).toBe(size.requested);
          expect(size.height).toBe(size.requested);
          expect(size.inkWidth).toBeGreaterThan(0);
          expect(size.inkHeight).toBeGreaterThan(0);
        }
        const preview = page.getByTestId("preview");
        await expect(preview.getByRole("tab", { name: "Table", exact: true }).locator("svg")).toHaveClass(/lucide-table/);
        await preview.getByRole("tab", { name: "JSON", exact: true }).click();
        await expect(preview.getByLabel("Sample documents")).toBeVisible();
        await preview.getByRole("tab", { name: "Table", exact: true }).click();
        await expect(preview.locator("tbody tr")).toHaveCount(2);
        for (const density of ["studio", "query"]) {
          const schema = page.getByTestId(`schema-${density}`);
          await expect(schema.locator(".df2-qw-node-toggle svg.lucide-table-2")).toHaveCount(2);
          await schema.locator(".df2-qw-node-toggle").first().click();
          await expect(schema.locator(".df2-qw-col").first()).toContainText("NOT NULL");
          await schema.getByRole("textbox", { name: "Filter schema" }).fill("events");
          await expect(schema.locator(".df2-qw-node-row")).toHaveCount(1);
          await schema.getByRole("textbox", { name: "Filter schema" }).fill("");
        }
        const layout = await page.evaluate(() => {
          const overlaps: string[] = [];
          for (const row of document.querySelectorAll(".df2-qw-node-row, .df2-qw-col, .df2-structure-preview-head")) {
            const children = Array.from(row.children).map(child => child.getBoundingClientRect());
            for (let index = 1; index < children.length; index++) {
              const previous = children[index - 1], current = children[index];
              if (Math.min(previous.right, current.right) - Math.max(previous.left, current.left) > 1 && Math.min(previous.bottom, current.bottom) - Math.max(previous.top, current.top) > 1) overlaps.push(row.className);
            }
          }
          for (const toggle of document.querySelectorAll(".df2-qw-node-toggle")) {
            const name = toggle.querySelector(".df2-qw-node-name")!.getBoundingClientRect();
            for (const svg of toggle.querySelectorAll("svg")) {
              if (svg.getBoundingClientRect().right > name.left + 1) overlaps.push("schema icon/name");
            }
          }
          return { overflow: document.documentElement.scrollWidth - innerWidth, overlaps };
        });
        await page.screenshot({ path: testInfo.outputPath(`entity-icons-${viewport.width}x${viewport.height}.png`), fullPage: true });
        expect(failures).toEqual([]);
        expect(layout.overflow).toBeLessThanOrEqual(1);
        expect(layout.overlaps).toEqual([]);
        const painted = await matrix.locator("svg").evaluateAll(async icons => {
          const counts: number[] = [];
          for (const icon of icons) {
            const clone = icon.cloneNode(true) as SVGSVGElement;
            clone.setAttribute("xmlns", "http://www.w3.org/2000/svg");
            clone.setAttribute("stroke", getComputedStyle(icon).color);
            const image = new Image();
            image.src = 'data:image/svg+xml;base64,' + btoa(new XMLSerializer().serializeToString(clone));
            await image.decode();
            const canvas = document.createElement("canvas");
            canvas.width = image.width; canvas.height = image.height;
            const context = canvas.getContext("2d")!;
            context.drawImage(image, 0, 0);
            const pixels = context.getImageData(0, 0, canvas.width, canvas.height).data;
            counts.push(Array.from(pixels).filter((value, index) => index % 4 === 3 && value > 0).length);
          }
          return counts;
        });
        expect(painted.every(count => count > 8)).toBeTruthy();
        expect(await matrix.locator("svg").evaluateAll(icons => icons.map(icon => icon.getBoundingClientRect().width))).toEqual(sizes.map(size => size.width));
      } finally {
        await browser.close();
      }
    });
  }
});

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
