import { expect, test, type Page } from "@playwright/test";
import { installHermeticStubs } from "./hermetic";
import { openStudio } from "./support";

async function gate8ProofHarness(page: Page, origin: string) {
  const moduleResponse = await page.request.get(`${origin}/src/components/transfer/Gate8ProofCard.tsx`);
  expect(moduleResponse.ok()).toBe(true);
  const toastImport = (await moduleResponse.text()).match(/import\s*\{\s*useToast\s*\}\s*from\s*["']([^"']+)["']/);
  expect(toastImport, "Use the card's actual Vite Toast context").not.toBeNull();
  const hash = "0123456789abcdef".repeat(4);
  const longError = "Signature digest mismatch: " + "unsigned_customer_contract_requires_operator_review_".repeat(6);
  const reports = {
    full: { passed: true, phase: "post_write", coverage: "full_checksum", assurance_level: "full_checksum",
      source_checksum_provenance: "independent_source_reread", source_checksum: hash, target_checksum: hash,
      checksum_match: true, source_rows: 1250, target_rows: 1250, migration_proven: true },
    failed: { passed: false, phase: "post_write_failed", coverage: "full_checksum",
      source_checksum_provenance: "independent_source_reread", source_checksum: hash, target_checksum: "f".repeat(64),
      checksum_match: false, source_rows: 1250, target_rows: 1249, migration_proven: false,
      message: "Independent destination read-back failed; migration remains unproven." },
    sample: { passed: true, phase: "post_write_sample_verified", coverage: "sample",
      source_checksum_provenance: "write_pass", source_checksum: hash, source_rows: 1250, target_rows: 1250,
      migration_proven: false, sample_compare: { passed: true, compared: 12, alignment: "primary_key", mismatches: [] } },
    writer: { passed: true, phase: "post_write_writer_ack", coverage: "writer_ack",
      source_checksum_provenance: "write_pass", source_checksum: hash, source_rows: 1250, target_rows: 1250,
      migration_proven: false, message: "Writer acknowledged rows; independent read-back not available." },
  };
  const summaries = Object.fromEntries(Object.entries(reports).map(([key, report]) => [`qa-proof-${key}`, {
    job_id: `qa-proof-${key}`, assurance: { migration_proven: report.migration_proven,
      claim_level: key === "full" ? "full_checksum" : "unproven" },
    accepted_risks: key === "full" ? [] : [{ reason: "Independent population proof unavailable" }],
    proof_incomplete_reasons: key === "full" ? [] : ["Migration is not proven by a file signature"],
    hashes: { ddl_hash: hash, mapping_hash: hash }, connector_versions_honesty: "Synthetic QA versions, not live certification",
    rollback_plan: { strategy: "operator_review", executable: false },
  }]));
  const evidence = { gets: [] as string[], posts: [] as Record<string, unknown>[], fulfilled: 0,
    blocked: [] as string[], failures: [] as string[] };
  const pending: Array<(answer: { body: unknown; status: number }) => void> = [];
  page.on("pageerror", error => evidence.failures.push(error.message));
  await page.route("**/*", async route => {
    const request = route.request();
    const url = new URL(request.url());
    const job = Object.keys(summaries).find(id => url.pathname.endsWith(`/transfer/${id}/proof-pack`));
    if (job && request.method() === "GET") {
      evidence.gets.push(url.pathname);
      return route.fulfill({ contentType: "application/json", body: JSON.stringify(summaries[job]) });
    }
    if (url.pathname.endsWith("/transfer/proof-pack/verify") && request.method() === "POST") {
      evidence.posts.push(request.postDataJSON());
      const answer = await new Promise<{ body: unknown; status: number }>(resolve => pending.push(resolve));
      await route.fulfill({ status: answer.status, contentType: "application/json", body: JSON.stringify(answer.body) });
      evidence.fulfilled++;
      return;
    }
    if (url.origin !== origin || url.pathname.startsWith("/api/")) {
      evidence.blocked.push(`${request.method()} ${url.pathname}`);
      return route.abort();
    }
    if (url.pathname !== "/__qa_gate8_proof") return route.continue();
    return route.fulfill({ contentType: "text/html", body: `<!doctype html>
      <html style="height:auto;overflow:auto"><head><meta name="viewport" content="width=device-width, initial-scale=1">
      <link rel="stylesheet" href="/src/styles/marketing-base.css?direct">
      <link rel="stylesheet" href="/src/styles/app-styles.css?direct"></head>
      <body style="height:auto;overflow:auto"><main id="root" class="df2-app" style="display:block;height:auto;max-height:none;overflow:visible;min-height:100vh;padding:16px"></main>
      <script type="module">
        import RefreshRuntime from '/@react-refresh';
        RefreshRuntime.injectIntoGlobalHook(window);
        window.$RefreshReg$ = () => {};
        window.$RefreshSig$ = () => (type) => type;
        window.__vite_plugin_react_preamble_installed__ = true;
        const React = (await import('/node_modules/.vite/deps/react.js')).default;
        const { createRoot } = (await import('/node_modules/.vite/deps/react-dom_client.js')).default;
        const { ToastProvider, useToast } = await import(${JSON.stringify(toastImport![1])});
        const { Gate8ProofCard } = await import('/src/components/transfer/Gate8ProofCard.tsx');
        const element = React.createElement;
        const reports = ${JSON.stringify(reports)};
        function Fixture() {
          const [kind, setKind] = React.useState('full');
          const { toast } = useToast();
          React.useEffect(() => {
            const update = event => setKind(event.detail);
            window.addEventListener('qa-proof-job', update);
            return () => window.removeEventListener('qa-proof-job', update);
          }, []);
          return element(React.Fragment, null,
            element('header', { 'data-testid': 'proof-header' },
              element('h1', null, 'Migration evidence'),
              element('div', { className: 'df2-toast-dock', 'data-df2-toast-dock': '' }),
              element('button', { onClick: () => toast({ title: 'Synthetic evidence review',
                message: 'File integrity does not establish migration correctness.', tone: 'warning' }) }, 'Show QA notification')),
            element(Gate8ProofCard, { report: reports[kind], jobId: 'qa-proof-' + kind }),
            element('footer', { 'data-testid': 'proof-footer' }, 'Migration evidence review complete'));
        }
        createRoot(document.getElementById('root')).render(element(ToastProvider, null, element(Fixture)));
      </script></body></html>` });
  });
  await page.goto(`${origin}/__qa_gate8_proof`);
  await expect(page.getByRole("region", { name: "Gate-8 reconciliation", exact: true })).toBeVisible();
  await expect(page.getByLabel("Signed proof pack honesty")).toContainText("true");
  return { reports, hash, longError, evidence,
    changeJob: async (kind: keyof typeof reports) => {
      const count = evidence.gets.length;
      await page.evaluate(kind => window.dispatchEvent(new CustomEvent("qa-proof-job", { detail: kind })), kind);
      await expect.poll(() => evidence.gets.length).toBe(count + 1);
      await expect(page.getByLabel("Signed proof pack honesty")).toContainText(kind === "full" ? "true" : "false");
    },
    answer: async (body: unknown, status = 200) => {
      expect(pending).toHaveLength(1);
      const count = evidence.fulfilled;
      pending.shift()!({ body, status });
      await expect.poll(() => evidence.fulfilled).toBe(count + 1);
    } };
}

test.describe("isolated Gate8 proof", () => {
  for (const viewport of [{ width: 1440, height: 1000 }, { width: 390, height: 844 }]) {
    for (const scenario of ["full and failed signature isolation", "sample and writer acknowledgement",
      "invalid file and signature feedback", "delayed verification job lifecycle"]) {
      test(`Gate8 ${scenario} ${viewport.width}x${viewport.height}`, async ({ playwright, baseURL }, testInfo) => {
        const browser = await playwright.chromium.launch({ channel: process.env.QA_BROWSER_CHANNEL ?? "msedge" });
        const page = await browser.newPage({ viewport });
        try {
          const harness = await gate8ProofHarness(page, new URL(baseURL ?? "http://127.0.0.1:5173").origin);
          const card = page.getByRole("region", { name: "Gate-8 reconciliation", exact: true });
          const feedback = card.locator(".df2-gate8-proof-verify");
          const input = card.locator('input[type="file"]');
          const badge = card.locator(".df2-gate8-proof-badge").first();
          const resolveColorToken = async (token: string) => page.evaluate(token => {
            const probe = document.createElement("span");
            probe.style.backgroundColor = `var(${token})`;
            document.body.append(probe);
            const color = getComputedStyle(probe).backgroundColor;
            probe.remove();
            return color;
          }, token);
          const expectNeutralPendingCard = async () => {
            await expect(card).toHaveClass(/\bis-pending\b/);
            const background = await card.evaluate(panel => getComputedStyle(panel).backgroundColor);
            expect(background).toBe(await resolveColorToken("--df-surface-subtle"));
            expect(background).not.toBe(await resolveColorToken("--df-success-bg"));
          };
          const expectNeutralValidSignature = async () => {
            const color = await feedback.locator("strong").evaluate(label => getComputedStyle(label).color);
            expect(color).toBe(await resolveColorToken("--df-text-primary"));
            expect(color).not.toBe(await resolveColorToken("--df-success"));
          };
          const start = async (job: string) => {
            const count = harness.evidence.posts.length;
            await expect(input).toBeEnabled();
            const inputId = await input.getAttribute("id");
            expect(inputId).toBeTruthy();
            await expect(card.locator("label").filter({ hasText: "Verify proof pack" })).toHaveAttribute("for", inputId!);
            await input.setInputFiles({ name: "synthetic-signed-proof.json", mimeType: "application/json",
              buffer: Buffer.from(JSON.stringify({ job_id: job, signature: "synthetic-not-a-real-HMAC" })) });
            await expect.poll(() => harness.evidence.posts.length).toBe(count + 1);
            expect(harness.evidence.posts.at(-1)).toEqual({ pack: { job_id: job, signature: "synthetic-not-a-real-HMAC" } });
            await expect(input).toBeDisabled();
            await expect(feedback).toHaveAttribute("role", "status");
            await expect(feedback).toContainText("Signature verification in progress");
          };
          const inspect = async (name: string) => {
            await page.evaluate(() => document.fonts.ready);
            await page.getByRole("button", { name: "Show QA notification", exact: true }).click();
            await expect(page.locator("[data-df2-toast-dock] .dt-toast")).toBeVisible();
            await page.getByTestId("proof-footer").scrollIntoViewIfNeeded();
            const geometry = await card.evaluate(panel => {
              const bounds = panel.getBoundingClientRect();
              const actions = panel.querySelector(".df2-gate8-proof-next-actions")!.getBoundingClientRect();
              const next = panel.querySelector(".df2-gate8-proof-next")!.getBoundingClientRect();
              const result = panel.querySelector<HTMLElement>(".df2-gate8-proof-verify");
              const resultBox = result?.getBoundingClientRect();
              const style = result && getComputedStyle(result);
              const panelStyle = getComputedStyle(panel);
              const footer = document.querySelector('[data-testid="proof-footer"]')!.getBoundingClientRect();
              const header = document.querySelector('[data-testid="proof-header"]')!.getBoundingClientRect();
              const intersects = (first: DOMRect, second: DOMRect) =>
                Math.min(first.right, second.right) - Math.max(first.left, second.left) > 1
                && Math.min(first.bottom, second.bottom) - Math.max(first.top, second.top) > 1;
              const controls = Array.from(panel.querySelectorAll<HTMLElement>(".df2-gate8-proof-next-actions .df2-btn"));
              const contentWidth = bounds.width - parseFloat(panelStyle.paddingLeft) - parseFloat(panelStyle.paddingRight)
                - parseFloat(panelStyle.borderLeftWidth) - parseFloat(panelStyle.borderRightWidth);
              return { overflow: Math.max(document.documentElement.scrollWidth - innerWidth, panel.scrollWidth - panel.clientWidth),
                cardFits: bounds.left >= 0 && bounds.right <= innerWidth + 1,
                separated: header.bottom <= bounds.top + 1 && bounds.bottom <= footer.top + 1,
                footerVisible: footer.top >= 0 && footer.bottom <= innerHeight + 1,
                controlsFit: controls.every(control => {
                  const box = control.getBoundingClientRect();
                  return box.left >= bounds.left && box.right <= bounds.right + 1 && control.scrollWidth <= control.clientWidth + 1;
                }),
                controlsOverlap: controls.some((control, index) => controls.slice(index + 1).some(other =>
                  intersects(control.getBoundingClientRect(), other.getBoundingClientRect()))),
                feedback: result && resultBox && style ? { belowActions: resultBox.top >= Math.max(next.bottom, actions.bottom) - 1,
                  fullWidth: Math.abs(resultBox.width - contentWidth) <= 1,
                  fits: result.scrollWidth <= result.clientWidth + 1,
                  visible: resultBox.top >= 0 && resultBox.bottom <= innerHeight + 1,
                  display: style.display, wrap: style.overflowWrap, border: style.borderTopWidth,
                  fontSize: style.fontSize, text: result.textContent, bounds: resultBox.toJSON() } : null,
                toasts: Array.from(document.querySelectorAll(".dt-toast")).map(toast => {
                  const box = toast.getBoundingClientRect();
                  return { clearCard: !intersects(box, bounds), clearFooter: !intersects(box, footer),
                    clearFeedback: !resultBox || !intersects(box, resultBox) };
                }) };
            });
            expect(geometry.overflow).toBeLessThanOrEqual(1);
            expect(geometry).toMatchObject({ cardFits: true, separated: true, footerVisible: true,
              controlsFit: true, controlsOverlap: false });
            if (geometry.feedback) expect(geometry.feedback).toMatchObject({ belowActions: true, fullWidth: true,
              fits: true, visible: true, display: "grid", wrap: "anywhere", border: "1px", fontSize: "12px" });
            expect(geometry.toasts).toHaveLength(1);
            for (const toast of geometry.toasts) expect(toast).toEqual({ clearCard: true, clearFooter: true, clearFeedback: true });
            await testInfo.attach(`gate8-${name}-geometry`, { body: JSON.stringify(geometry, null, 2), contentType: "application/json" });
            for (const capture of ["end", "start"]) {
              const path = testInfo.outputPath(`gate8-${name}-${capture}-${viewport.width}.png`);
              if (capture === "start") await card.locator(".df2-gate8-proof-head").scrollIntoViewIfNeeded();
              await page.screenshot({ path, animations: "disabled" });
              await testInfo.attach(`gate8-${name}-${capture}`, { path, contentType: "image/png" });
            }
          };
          const limited = async (label: string) => {
            await expect(badge).toHaveText(label);
            await expect(card).not.toHaveClass(/\bis-pass\b/);
            await expect(card.getByRole("heading")).not.toHaveText("Source and destination match");
            await expect(card.getByLabel("Signed proof pack honesty")).toContainText("false");
          };
          if (scenario === "full and failed signature isolation") {
            await expect(badge).toHaveText("Verified");
            await expect(card).toHaveClass(/\bis-pass\b/);
            await expect(card.getByRole("heading")).toHaveText("Source and destination match");
            await start("qa-proof-full");
            await harness.answer({ ok: true, errors: [], content_sha256: harness.hash });
            await expect(feedback).toContainText(harness.hash);
            await expect(feedback).toContainText("Pack integrity only");
            await inspect("full-valid");
            await harness.changeJob("failed");
            await limited("Failed");
            const dataText = await card.locator(".df2-gate8-proof-grid").textContent();
            await start("qa-proof-failed");
            await harness.answer({ ok: true, errors: [], content_sha256: harness.hash });
            await expect(feedback).toContainText("Signature valid");
            await limited("Failed");
            await expect(card.getByRole("heading")).toHaveText("Reconciliation did not verify");
            expect(await card.locator(".df2-gate8-proof-grid").textContent()).toBe(dataText);
            await inspect("failed-valid-signature");
          } else if (scenario === "sample and writer acknowledgement") {
            for (const [kind, label] of [["sample", "Sample only"], ["writer", "Writer ack"]] as const) {
              await harness.changeJob(kind);
              await limited(label);
              await expectNeutralPendingCard();
              await start(`qa-proof-${kind}`);
              await harness.answer({ ok: true, errors: [], content_sha256: harness.hash });
              await expect(feedback).toContainText("Signature valid");
              await expectNeutralValidSignature();
              await limited(label);
              await expect(card).toContainText(kind === "sample" ? "not population / migration_proven" : "independent read-back not available");
              await inspect(`${kind}-valid-signature`);
            }
          } else if (scenario === "invalid file and signature feedback") {
            await harness.changeJob("failed");
            await input.setInputFiles({ name: "invalid.json", mimeType: "application/json", buffer: Buffer.from("not-json") });
            await expect(feedback).toHaveAttribute("role", "alert");
            await expect(feedback).toContainText("Signature verification failed");
            await expect(feedback).toContainText("Pack integrity not verified");
            expect(harness.evidence.posts).toHaveLength(0);
            await expect(input).toBeEnabled();
            await inspect("invalid-file");
            await start("qa-proof-failed");
            await inspect("verification-pending");
            await harness.answer({ ok: false, errors: [harness.longError, "No trusted signer matches the exported pack"] });
            await expect(feedback).toHaveAttribute("role", "alert");
            await expect(feedback).toContainText(harness.longError);
            await limited("Failed");
            await inspect("signature-failure");
            await start("qa-proof-failed");
            await harness.answer({ detail: harness.longError }, 422);
            await expect(feedback).toContainText(harness.longError);
            await expect(input).toBeEnabled();
            await inspect("verification-http-failure");
          } else {
            await harness.changeJob("failed");
            await start("qa-proof-failed");
            await harness.changeJob("sample");
            await expect(feedback).toHaveCount(0);
            await expect(input).toBeEnabled();
            const response = page.waitForResponse(response => response.url().endsWith("/transfer/proof-pack/verify"));
            await harness.answer({ ok: true, errors: [], content_sha256: "old-job-hash-must-not-appear" });
            await (await response).finished();
            await page.evaluate(() => new Promise<void>(resolve => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))));
            await expect(feedback).toHaveCount(0);
            await expect(card).not.toContainText("old-job-hash-must-not-appear");
            await limited("Sample only");
            await inspect("stale-result-ignored");
            await start("qa-proof-sample");
            await harness.answer({ ok: true, errors: [], content_sha256: harness.hash });
            await expect(feedback).toContainText(harness.hash);
            await limited("Sample only");
            await inspect("current-result-accepted");
          }
          await expect(input).toBeEnabled();
          expect(harness.evidence.blocked).toEqual([]);
          expect(harness.evidence.failures).toEqual([]);
          expect(harness.evidence.gets.length).toBeGreaterThanOrEqual(2);
          expect(harness.evidence.fulfilled).toBe(harness.evidence.posts.length);
          await testInfo.attach("gate8-api-evidence", { body: JSON.stringify(harness.evidence, null, 2), contentType: "application/json" });
        } finally {
          await browser.close();
        }
      });
    }
  }
});

async function repairProposalHarness(page: Page, origin: string) {
  const token = "customer_contract_identifier_requires_operator_review_".repeat(5);
  const mappings = [{ source: token, destination: "amount", target_type: "INTEGER" }];
  const proven = { kind: "change_target_type", column: token, to_type: "DECIMAL(20,2)", apply_proven: true };
  const guidance = { kind: "future_action", column: token, destination: token,
    label: "Review destination contract before execution. " + token };
  const proposal = { id: "qa-proposal-" + token, job_id: "qa-job-" + token, status: "proposed",
    source: "Synthetic preflight", confidence: "Unverified", auto_applicable: true,
    summary: "Review mapping changes; no destination write has occurred.", actions: [proven, guidance] };
  const updated = [{ ...mappings[0], target_type: "DECIMAL(20,2)" }];
  const verified = { ...proposal, status: "applied", apply_result: { applied: true, mappings: updated } };
  const evidence = { posts: [] as Record<string, unknown>[], fulfilled: 0,
    blocked: [] as string[], failures: [] as string[] };
  const pending: Array<(answer: { body: unknown; status: number }) => void> = [];
  page.on("pageerror", error => evidence.failures.push(error.message));
  await page.route("**/*", async route => {
    const request = route.request();
    const url = new URL(request.url());
    if (url.pathname === `/api/v1/repair/proposals/${encodeURIComponent(proposal.id)}/decide`
      && request.method() === "POST") {
      evidence.posts.push(request.postDataJSON());
      const answer = await new Promise<{ body: unknown; status: number }>(resolve => pending.push(resolve));
      await route.fulfill({ status: answer.status, contentType: "application/json", body: JSON.stringify(answer.body) });
      evidence.fulfilled++;
      return;
    }
    if (url.origin !== origin || url.pathname.startsWith("/api/")) {
      evidence.blocked.push(`${request.method()} ${url.pathname}`);
      return route.abort();
    }
    if (url.pathname !== "/__qa_repair_proposal") return route.continue();
    return route.fulfill({ contentType: "text/html", body: `<!doctype html>
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
        const { RepairProposalDrawer } = await import('/src/components/transfer/RepairProposalDrawer.tsx');
        const element = React.createElement;
        function Fixture() {
          const [open, setOpen] = React.useState(true);
          const [proposal, setProposal] = React.useState(${JSON.stringify(proposal)});
          const [mappings, setMappings] = React.useState(${JSON.stringify(mappings)});
          const [failApplied, setFailApplied] = React.useState(false);
          const [events, setEvents] = React.useState({ applied: [], decided: [], closed: 0, identity: 0 });
          React.useEffect(() => {
            const update = event => {
              if ('open' in event.detail) setOpen(event.detail.open);
              if ('proposal' in event.detail) setProposal(event.detail.proposal);
              if ('mappings' in event.detail) setMappings(event.detail.mappings);
              if ('failApplied' in event.detail) setFailApplied(event.detail.failApplied);
            };
            window.addEventListener('qa-repair-parent', update);
            return () => window.removeEventListener('qa-repair-parent', update);
          }, []);
          return element(React.Fragment, null,
            element('output', { 'data-testid': 'repair-events' }, JSON.stringify(events)),
            element(RepairProposalDrawer, { open, proposal, mappings,
              onClose: () => { setEvents(previous => ({ ...previous, closed: previous.closed + 1 })); setOpen(false); },
              onApplied: (updated, value) => {
                if (failApplied) throw new Error('Synthetic parent callback failed. Mapping changes were not accepted.');
                setEvents(previous => ({ ...previous, applied: [...previous.applied, { updated, id: value.id }] }));
              },
              onDecided: value => setEvents(previous => ({ ...previous, decided: [...previous.decided, value.id] })),
              onOpenIdentitySettings: () => setEvents(previous => ({ ...previous, identity: previous.identity + 1 })) }));
        }
        createRoot(document.getElementById('root')).render(element(Fixture));
      </script></body></html>` });
  });
  await page.goto(`${origin}/__qa_repair_proposal`);
  await expect.poll(async () => {
    if (evidence.failures.length) throw new Error(evidence.failures.join("\n"));
    return page.getByTestId("repair-events").count();
  }).toBe(1);
  await expect(page.getByRole("dialog")).toBeVisible();
  return { proposal, proven, guidance, mappings, updated, verified, evidence,
    configure: async (detail: Record<string, unknown>) => {
      await page.evaluate(detail => window.dispatchEvent(new CustomEvent("qa-repair-parent", { detail })), detail);
    },
    answer: async (body: unknown, status = 200) => {
      expect(pending.length, "A decision POST must be pending before answering").toBe(1);
      const fulfilled = evidence.fulfilled;
      pending.shift()!({ body, status });
      await expect.poll(() => evidence.fulfilled).toBe(fulfilled + 1);
    } };
}

test.describe("isolated repair proposal", () => {
  for (const viewport of [{ width: 1440, height: 1000 }, { width: 390, height: 844 }]) {
    for (const scenario of ["labels and long identifiers", "manual gates and no mappings",
      "pending dismissal and partial mixed apply", "failure and response lifecycle guards"]) {
      test(`repair ${scenario} ${viewport.width}x${viewport.height}`, async ({ playwright, baseURL }, testInfo) => {
        const browser = await playwright.chromium.launch({ channel: process.env.QA_BROWSER_CHANNEL ?? "msedge" });
        const page = await browser.newPage({ viewport });
        try {
          const harness = await repairProposalHarness(page, new URL(baseURL ?? "http://127.0.0.1:5173").origin);
          const dialog = page.getByRole("dialog");
          const apply = () => dialog.getByRole("button", { name: /^Approve & apply/ });
          const events = async () => JSON.parse((await page.getByTestId("repair-events").textContent())!);
          const unchanged = async () => expect(await events()).toEqual({ applied: [], decided: [], closed: 0, identity: 0 });
          const inspectError = async (name: string) => {
            const alert = dialog.getByRole("alert");
            await expect(dialog).toHaveAccessibleName("Repair proposal");
            await expect(alert).toBeVisible();
            await expect(alert).toBeFocused();
            await expect(alert).toBeInViewport({ ratio: 1 });
            await expect.poll(async () => alert.evaluate(node => {
              const box = node.getBoundingClientRect();
              const body = node.closest(".df2-drawer-body")!.getBoundingClientRect();
              return box.width > 0 && box.height > 0 && box.left >= Math.max(0, body.left) - 1
                && box.right <= Math.min(innerWidth, body.right) + 1
                && box.top >= Math.max(0, body.top) - 1
                && box.bottom <= Math.min(innerHeight, body.bottom) + 1;
            }), "Focused error must be fully inside the viewport and unclipped drawer body").toBe(true);
            const geometry = await alert.evaluate(node => {
              const box = node.getBoundingClientRect();
              const body = node.closest(".df2-drawer-body")!;
              return { viewport: { width: innerWidth, height: innerHeight },
                alert: box.toJSON(), body: body.getBoundingClientRect().toJSON(),
                scrollTop: body.scrollTop, focused: document.activeElement === node, text: node.textContent };
            });
            await expect(dialog.locator(".df2-drawer-header")).toBeInViewport({ ratio: 1 });
            await expect(dialog.locator(".df2-drawer-footer")).toBeInViewport({ ratio: 1 });
            await testInfo.attach(`repair-${name}-alert-geometry`, {
              body: JSON.stringify(geometry, null, 2), contentType: "application/json" });
            const path = testInfo.outputPath(`repair-${name}-focused-alert-${viewport.width}.png`);
            await page.screenshot({ path, animations: "disabled" });
            await testInfo.attach(`repair-${name}-focused-alert`, { path, contentType: "image/png" });
          };
          const inspect = async (name: string) => {
            await page.evaluate(() => document.fonts.ready);
            const body = dialog.locator(".df2-drawer-body");
            await body.evaluate(node => { node.scrollTop = node.scrollHeight; });
            await expect(dialog.locator(".df2-drawer-header")).toBeInViewport({ ratio: 1 });
            await expect(dialog.locator(".df2-drawer-footer")).toBeInViewport({ ratio: 1 });
            const geometry = await dialog.evaluate(panel => {
              const box = panel.getBoundingClientRect();
              const header = panel.querySelector(".df2-drawer-header")!.getBoundingClientRect();
              const body = panel.querySelector(".df2-drawer-body")!;
              const bodyBox = body.getBoundingClientRect();
              const footer = panel.querySelector(".df2-drawer-footer")!.getBoundingClientRect();
              const buttons = Array.from(panel.querySelectorAll(".df2-drawer-footer button"));
              const buttonBoxes = buttons.map(button => button.getBoundingClientRect());
              const rows = Array.from(panel.querySelectorAll(".df2-repair-metadata > div, .df2-repair-action-details > div"));
              return { width: innerWidth, overflow: Math.max(document.documentElement.scrollWidth - innerWidth,
                panel.scrollWidth - panel.clientWidth, body.scrollWidth - body.clientWidth),
                contained: box.left >= 0 && box.right <= innerWidth + 1 && box.top >= 0 && box.bottom <= innerHeight + 1,
                separated: header.bottom <= bodyBox.top + 1 && bodyBox.bottom <= footer.top + 1,
                pageEnd: body.scrollTop + body.clientHeight >= body.scrollHeight - 1,
                footerClearance: footer.top - bodyBox.bottom,
                buttons: buttons.map((button, index) => ({ text: button.textContent,
                  fits: button.scrollWidth <= button.clientWidth + 1,
                  contained: buttonBoxes[index].left >= box.left && buttonBoxes[index].right <= box.right + 1
                    && buttonBoxes[index].top >= footer.top && buttonBoxes[index].bottom <= footer.bottom + 1 })),
                overlap: buttonBoxes.some((first, index) => buttonBoxes.slice(index + 1).some(second =>
                  Math.min(first.right, second.right) - Math.max(first.left, second.left) > 1
                  && Math.min(first.bottom, second.bottom) - Math.max(first.top, second.top) > 1)),
                labels: rows.map(row => {
                  const label = row.querySelector("dt")!.getBoundingClientRect();
                  const value = row.querySelector("dd")!;
                  const valueBox = value.getBoundingClientRect();
                  return { aligned: Math.abs(label.left - valueBox.left) <= 1, below: valueBox.top >= label.bottom - 1,
                    fits: value.scrollWidth <= value.clientWidth + 1, text: value.textContent };
                }) };
            });
            expect(geometry.overflow).toBeLessThanOrEqual(1);
            expect(geometry.contained).toBe(true);
            expect(geometry.separated).toBe(true);
            expect(geometry.pageEnd).toBe(true);
            expect(geometry.overlap).toBe(false);
            for (const button of geometry.buttons) expect(button).toMatchObject({ fits: true, contained: true });
            expect(geometry.labels.length).toBeGreaterThan(0);
            for (const label of geometry.labels) expect(label).toMatchObject({ aligned: true, below: true, fits: true });
            await testInfo.attach(`repair-${name}-geometry`, { body: JSON.stringify(geometry, null, 2), contentType: "application/json" });
            for (const position of ["end", "start"]) {
              if (position === "start") await body.evaluate(node => { node.scrollTop = 0; });
              const path = testInfo.outputPath(`repair-${name}-${position}-${viewport.width}.png`);
              await page.screenshot({ path, animations: "disabled" });
              await testInfo.attach(`repair-${name}-${position}`, { path, contentType: "image/png" });
            }
          };
          if (scenario === "labels and long identifiers") {
            await expect(dialog.locator(".df2-repair-status")).toHaveText("Statusproposed");
            await expect(dialog.locator(".df2-repair-metadata")).toContainText(harness.proposal.id);
            await expect(dialog.locator(".df2-repair-metadata")).toContainText(harness.proposal.job_id);
            await expect(dialog.getByRole("heading", { name: "Proposed mapping changes (1)", exact: true })).toBeVisible();
            const actions = dialog.locator(".df2-repair-actions li");
            await expect(actions.nth(0)).toContainText("Change target type");
            await expect(actions.nth(0)).toContainText("Proven against scanned values");
            await expect(actions.nth(1)).toContainText("Manual step. Not applied by this proposal.");
            await expect(actions.nth(1).locator("dd").last()).toHaveText("Unverified");
            await expect(dialog).toContainText("this flag is not validation evidence");
            await expect(dialog).not.toContainText("Each type was proven");
            await expect(apply()).toBeEnabled();
            await inspect("labels");
            await unchanged();
            expect(harness.evidence.posts).toHaveLength(0);
          } else if (scenario === "manual gates and no mappings") {
            await harness.configure({ proposal: { ...harness.proposal, actions: [
              { ...harness.proven, requires_ddl: true }, { ...harness.proven, apply_proven: false }, harness.guidance] } });
            await expect(apply()).toHaveCount(0);
            await expect(dialog.getByRole("heading", { name: "Manual steps (3)", exact: true })).toBeVisible();
            await expect(dialog).toContainText("Not proven");
            await inspect("live-ddl-unproven");
            await unchanged();
            await harness.configure({ proposal: harness.proposal, mappings: [] });
            await expect(apply()).toHaveCount(0);
            await expect(dialog.getByRole("button", { name: "Continue in Validate", exact: true })).toBeEnabled();
            await inspect("no-mappings");
            await dialog.getByRole("button", { name: "Continue in Validate", exact: true }).click();
            await expect(dialog).toHaveCount(0);
            expect(await events()).toEqual({ applied: [], decided: [harness.proposal.id], closed: 1, identity: 0 });
            expect(harness.evidence.posts).toHaveLength(0);
          } else if (scenario === "pending dismissal and partial mixed apply") {
            await harness.configure({ proposal: { ...harness.proposal, diagnosis: { root_cause: "duplicate_identity_keys" },
              actions: [harness.proven, { kind: "fix_source_keys", column: "id", mapping_applyable: false }, harness.guidance] } });
            await expect(dialog.getByRole("status").filter({ hasText: "Root cause:" })).toContainText("cannot dedupe rows");
            await expect(apply()).toBeEnabled();
            await inspect("mixed-guidance");
            await apply().evaluate(button => { (button as HTMLButtonElement).click(); (button as HTMLButtonElement).click(); });
            await expect.poll(() => harness.evidence.posts.length).toBe(1);
            await expect(dialog.locator('.df2-drawer-footer button[aria-busy="true"][data-loading="true"]')).toBeDisabled();
            await page.keyboard.press("Escape");
            await dialog.getByRole("button", { name: "Close", exact: true }).click();
            await page.locator(".df2-drawer-overlay").click({ position: { x: 2, y: 2 } });
            await expect(dialog).toBeVisible();
            await unchanged();
            await expect(dialog.getByRole("button", { name: "Cancel", exact: true })).toBeDisabled();
            await expect(dialog.getByRole("button", { name: "Reject", exact: true })).toBeDisabled();
            await expect(dialog.getByRole("button", { name: "Review identity separately", exact: true })).toBeDisabled();
            await inspect("pending");
            expect(harness.evidence.posts[0]).toEqual({ approve: true, mappings: harness.mappings });
            await harness.answer({ ...harness.verified, apply_result: { ...harness.verified.apply_result,
              mutated_count: 1, skipped_actions: [{ kind: "fix_source_keys", reason: "Manual identity repair required" }] } });
            await expect(dialog).toHaveCount(0);
            expect(await events()).toEqual({ applied: [{ updated: harness.updated, id: harness.proposal.id }],
              decided: [], closed: 1, identity: 0 });
            await harness.configure({ open: true });
            await expect(dialog).toBeVisible();
            await expect(dialog).toContainText("cannot dedupe rows");
            await expect(dialog).not.toContainText("duplicates resolved");
            expect((await events()).applied).toHaveLength(1);
            expect(harness.evidence.posts).toHaveLength(1);
          } else {
            for (const failure of ["http", "proposal-id", "job-id", "empty-mappings", "unverified-result"]) {
              await apply().click();
              await expect.poll(() => harness.evidence.posts.length).toBe(harness.evidence.fulfilled + 1);
              await harness.answer(failure === "http" ? { detail: "Synthetic repair refused. Reload proposal and re-run Validate before retrying." }
                : failure === "proposal-id" ? { ...harness.verified, id: "different-proposal" }
                  : failure === "job-id" ? { ...harness.verified, job_id: "different-job" }
                    : { ...harness.verified, apply_result: { applied: failure === "unverified-result" ? "true" : true,
                      mappings: failure === "empty-mappings" ? [] : harness.updated } }, failure === "http" ? 422 : 200);
              await expect(dialog.getByRole("alert")).toContainText(failure === "http" ? "re-run Validate"
                : failure.endsWith("-id") ? "Reload before applying changes" : "cannot auto-fix the blocker");
              await expect(apply()).toBeEnabled();
              await unchanged();
              if (failure === "http" || failure === "empty-mappings" || failure === "unverified-result") {
                await inspectError(failure);
              }
              await inspect(failure);
            }
            await apply().click();
            await expect.poll(() => harness.evidence.posts.length).toBe(6);
            await harness.configure({ open: false });
            await expect(dialog).toHaveCount(0);
            await harness.configure({ open: true });
            await expect(dialog).toBeVisible();
            await harness.answer(harness.verified);
            await expect(apply()).toBeEnabled();
            await unchanged();
            await expect(dialog.getByRole("alert")).toHaveCount(0);
            await apply().click();
            await expect.poll(() => harness.evidence.posts.length).toBe(7);
            await harness.answer(harness.verified);
            await expect(dialog).toHaveCount(0);
            expect(await events()).toEqual({ applied: [{ updated: harness.updated, id: harness.proposal.id }],
              decided: [], closed: 1, identity: 0 });
            await harness.configure({ open: true, failApplied: true });
            await expect(dialog).toBeVisible();
            const beforeCallback = await events();
            await apply().click();
            await expect.poll(() => harness.evidence.posts.length).toBe(8);
            await harness.answer(harness.verified);
            await expect(dialog.getByRole("alert")).toContainText("Synthetic parent callback failed");
            await expect(dialog).toBeVisible();
            await expect(apply()).toBeEnabled();
            expect(await events()).toEqual(beforeCallback);
            await inspectError("callback-error");
            await inspect("callback-error");
          }
          expect(harness.evidence.failures).toEqual([]);
          expect(harness.evidence.blocked).toEqual([]);
          await testInfo.attach("repair-api-evidence", { body: JSON.stringify(harness.evidence, null, 2), contentType: "application/json" });
        } finally {
          await browser.close();
        }
      });
    }
  }
});

async function quarantineRecoveryHarness(page: Page, origin: string) {
  const moduleResponse = await page.request.get(`${origin}/src/components/transfer/QuarantinePanel.tsx`);
  expect(moduleResponse.ok()).toBe(true);
  const toastImport = (await moduleResponse.text()).match(/import\s*\{\s*useToast\s*\}\s*from\s*["']([^"']+)["']/);
  expect(toastImport, "Resolve the same Vite Toast context used by QuarantinePanel").not.toBeNull();
  const finding = { row: 2, column: "amount", target: "amount", value: "bad",
    reason: "Invalid integer: " + "stored_customer_value_requires_operator_review_".repeat(8),
    values: { id: "2", amount: "bad" }, retry_status: "open" };
  const open = { quarantine: [finding], source: "dlq", rejected_rows: 1, issue_count: 1,
    quarantine_durable: true, dest_dlq_durable: true, rows_rolled_back: 7,
    replay_block_reason: null,
    quarantine_closure: { verdict: "open", open_count: 1, promoted_count: 0 } };
  const closed = { ...open, quarantine: [{ ...finding, retry_status: "promoted" }],
    quarantine_closure: { verdict: "closed", open_count: 0, promoted_count: 1 } };
  const verified = { success: true, job_id: "qa-child", rows_written: 1, rejected: 0,
    reconciliation: { passed: true, phase: "readback" }, quarantine_closure: closed.quarantine_closure };
  let ledger: Record<string, unknown> = open;
  let result: Record<string, unknown> = verified;
  let holdReplay = false;
  let holdGet = false;
  let releaseReplay = () => {};
  let releaseGet = () => {};
  const evidence = { gets: 0, posts: [] as Record<string, unknown>[], proposals: [] as Record<string, unknown>[],
    events: [] as string[], blocked: [] as string[], failures: [] as string[] };
  page.on("pageerror", error => evidence.failures.push(error.message));
  await page.route("**/*", async route => {
    const request = route.request();
    const url = new URL(request.url());
    const json = (body: unknown) => route.fulfill({ contentType: "application/json", body: JSON.stringify(body) });
    if (url.pathname === "/api/v1/connectors/jobs/qa-recovery/quarantine" && request.method() === "GET") {
      evidence.gets++;
      evidence.events.push("GET requested");
      if (holdGet) await new Promise<void>(resolve => { releaseGet = resolve; });
      evidence.events.push("GET fulfilled");
      return json(ledger);
    }
    if (url.pathname === "/api/v1/connectors/jobs/qa-recovery/quarantine/replay" && request.method() === "POST") {
      evidence.posts.push(request.postDataJSON());
      evidence.events.push("POST requested");
      if (holdReplay) await new Promise<void>(resolve => { releaseReplay = resolve; });
      evidence.events.push("POST fulfilled");
      return json(result);
    }
    if (url.pathname === "/api/v1/repair/propose/quarantine" && request.method() === "POST") {
      evidence.proposals.push(request.postDataJSON());
      return route.fulfill({ status: 422, contentType: "application/json", body: JSON.stringify({ detail: "Synthetic proposal refused" }) });
    }
    if (url.pathname.startsWith("/api/")) {
      evidence.blocked.push(`${request.method()} ${url.pathname}`);
      return route.abort();
    }
    if (url.origin !== origin) return route.abort();
    if (url.pathname !== "/__qa_quarantine_recovery") return route.continue();
    return route.fulfill({ contentType: "text/html", body: `<!doctype html>
      <html style="height:auto;overflow:auto"><head><meta name="viewport" content="width=device-width, initial-scale=1">
      <link rel="stylesheet" href="/src/styles/marketing-base.css?direct">
      <link rel="stylesheet" href="/src/styles/app-styles.css?direct"></head>
      <body style="height:auto;overflow:auto"><main id="root" class="df2-app" style="display:block;height:auto;max-height:none;overflow:visible;min-height:100vh;padding:16px"></main>
      <script type="module">
        import RefreshRuntime from '/@react-refresh';
        RefreshRuntime.injectIntoGlobalHook(window);
        window.$RefreshReg$ = () => {};
        window.$RefreshSig$ = () => (type) => type;
        window.__vite_plugin_react_preamble_installed__ = true;
        const React = (await import('/node_modules/.vite/deps/react.js')).default;
        const { createRoot } = (await import('/node_modules/.vite/deps/react-dom_client.js')).default;
        const { ToastProvider } = await import(${JSON.stringify(toastImport![1])});
        const { QuarantinePanel } = await import('/src/components/transfer/QuarantinePanel.tsx');
        const element = React.createElement;
        function Fixture() {
          const [completed, setCompleted] = React.useState([]);
          return element(ToastProvider, null,
            element('header', { 'data-testid': 'recovery-header' }, element('h1', null, 'Quarantine recovery')),
            element('output', { 'data-testid': 'recovery-completed' }, JSON.stringify(completed)),
            element(QuarantinePanel, { jobId: 'qa-recovery', initiallyOpen: true,
              onReplayComplete: child => setCompleted(previous => [...previous, child]) }),
            element('footer', { 'data-testid': 'recovery-footer' }, 'Recovery ledger review'));
        }
        createRoot(document.getElementById('root')).render(element(Fixture));
      </script></body></html>` });
  });
  await page.goto(`${origin}/__qa_quarantine_recovery`);
  await expect.poll(async () => {
    if (evidence.failures.length) throw new Error(evidence.failures.join("\n"));
    return page.getByTestId("recovery-completed").count();
  }).toBe(1);
  await expect(page.getByRole("button", { name: "Replay quarantined rows", exact: true })).toBeEnabled();
  return { open, closed, verified, evidence,
    setLedger: (value: Record<string, unknown>) => { ledger = value; },
    setResult: (value: Record<string, unknown>) => { result = value; },
    hold: () => { holdReplay = true; holdGet = true; },
    releaseReplay: () => { holdReplay = false; releaseReplay(); },
    releaseGet: () => { holdGet = false; releaseGet(); } };
}

test.describe("isolated quarantine recovery", () => {
  for (const viewport of [{ width: 1440, height: 1000 }, { width: 390, height: 844 }]) {
    for (const scenario of ["state and proof gates", "long reasons and value editor"]) {
      test(`recovery ${scenario} ${viewport.width}x${viewport.height}`, async ({ playwright, baseURL }, testInfo) => {
        const browser = await playwright.chromium.launch({ channel: process.env.QA_BROWSER_CHANNEL ?? "msedge" });
        const page = await browser.newPage({ viewport });
        try {
          const harness = await quarantineRecoveryHarness(page, new URL(baseURL ?? "http://127.0.0.1:5173").origin);
          const replay = () => page.getByRole("button", { name: "Replay quarantined rows", exact: true });
          const completed = page.getByTestId("recovery-completed");
          const next = page.getByRole("region", { name: "Next remediation step", exact: true });
          const review = page.getByRole("alert").filter({ hasText: "Replay requires review" });
          const proof = page.locator(".df2-quarantine-replay-result");
          const refresh = async (ledger: Record<string, unknown>) => {
            harness.setLedger(ledger);
            const count = harness.evidence.gets;
            await page.getByRole("button", { name: "Refresh", exact: true }).click();
            await expect.poll(() => harness.evidence.gets).toBe(count + 1);
            await expect(page.getByRole("button", { name: "Refresh", exact: true })).toBeEnabled();
          };
          const screenshot = async (name: string) => {
            const path = testInfo.outputPath(`recovery-${name}-${viewport.width}x${viewport.height}.png`);
            await page.screenshot({ path, fullPage: true, animations: "disabled" });
            await testInfo.attach(`recovery-${name}`, { path, contentType: "image/png" });
            if (name === "missing-proof" || name === "verified-closed") {
              const resultPath = testInfo.outputPath(`recovery-${name}-result-${viewport.width}x${viewport.height}.png`);
              await proof.screenshot({ path: resultPath, animations: "disabled" });
              await testInfo.attach(`recovery-${name}-result`, { path: resultPath, contentType: "image/png" });
            }
          };
          const toastClearance = async (name: string) => {
            await expect(page.locator(".df2-quarantine-notifications .dt-toast").first()).toBeVisible();
            const geometry = await page.getByRole("region", { name: "Quarantine table", exact: true }).evaluate(region => {
              const findings = region.getBoundingClientRect();
              return Array.from(document.querySelectorAll(".df2-quarantine-notifications .dt-toast")).map(toast => {
                const bounds = toast.getBoundingClientRect();
                return { bottom: bounds.bottom, findingsTop: findings.top,
                  nonintersection: bounds.bottom <= findings.top || bounds.top >= findings.bottom
                    || bounds.right <= findings.left || bounds.left >= findings.right };
              });
            });
            expect(geometry.length).toBeGreaterThan(0);
            for (const toast of geometry) expect(toast.nonintersection).toBe(true);
            await testInfo.attach(`recovery-toast-clearance-${name}`, {
              body: JSON.stringify(geometry, null, 2), contentType: "application/json" });
          };
          await expect(completed).toHaveText("[]");
          await expect(page.getByLabel("Quarantine counts")).toContainText("1 row quarantined");
          await expect(page.getByLabel("Quarantine counts")).toContainText("7 rows rolled back with the refused batch");
          if (scenario === "state and proof gates") {
            await refresh({ ...harness.open, quarantine_durable: false, quarantine_dlq_error: "Synthetic durable store unavailable" });
            await expect(next.getByRole("alert")).toContainText("not durably written");
            await expect(replay()).toHaveCount(0);
            await refresh({ ...harness.open, dest_dlq_durable: false,
              dest_dlq: { skipped: true, reason: "Synthetic control-plane-only destination", error: "Synthetic SQL DLQ refused" } });
            await expect(next.getByRole("alert")).toContainText("control plane only");
            await expect(replay()).toBeEnabled();
            await refresh(harness.closed);
            await expect(replay()).toHaveCount(0);
            await expect(next).toContainText("parent transfer's checksum");
            await expect(next).toContainText("not");
            await screenshot("ledger-closed");
            for (const failure of ["diverging", "missing-ledger", "missing-proof"]) {
              await refresh(harness.open);
              harness.setResult(failure === "missing-proof" ? { ...harness.verified, reconciliation: {} }
                : failure === "diverging" ? { ...harness.verified, quarantine_closure: { verdict: "diverging", open_count: 1 } }
                  : harness.verified);
              harness.setLedger(failure === "missing-ledger" ? { ...harness.open, quarantine_closure: undefined }
                : failure === "diverging" ? { ...harness.open, quarantine_closure: { verdict: "diverging", open_count: 1, next_action: "Synthetic destination ledger mismatch" } }
                  : harness.closed);
              harness.hold();
              const postCount = harness.evidence.posts.length;
              const getCount = harness.evidence.gets;
              await replay().evaluate(button => { (button as HTMLButtonElement).click(); (button as HTMLButtonElement).click(); });
              await expect.poll(() => harness.evidence.posts.length).toBe(postCount + 1);
              await expect(page.getByRole("button", { name: "Promoting", exact: false })).toBeDisabled();
              expect(harness.evidence.gets).toBe(getCount);
              await expect(completed).toHaveText("[]");
              harness.releaseReplay();
              await expect.poll(() => harness.evidence.gets).toBe(getCount + 1);
              await expect(completed).toHaveText("[]");
              if (failure !== "missing-ledger") await expect(page.locator(".dt-toast--warning").last())
                .toContainText(failure === "missing-proof" ? "proof not verified" : "ledger requires review");
              harness.releaseGet();
              await expect(page.getByRole("button", { name: "Refresh", exact: true })).toBeEnabled();
              await expect(completed).toHaveText("[]");
              expect(harness.evidence.posts.length).toBe(postCount + 1);
              if (failure !== "missing-proof") await expect(review)
                .toContainText(failure === "diverging" ? "ledger mismatch" : "ledger could not be verified");
              await expect(proof).toContainText(failure === "missing-proof" ? "Child proof unverified" : "Child proof verified");
              await expect(proof).toHaveClass(/\bis-review\b/);
              await expect(proof).not.toHaveClass(/\bis-verified\b/);
              await toastClearance(failure);
              if (failure === "missing-proof") {
                await expect(review).toContainText("Child replay proof failed or is unavailable");
                await expect(page.locator(".dt-toast--success")).toHaveCount(0);
                await expect(proof.locator('svg polyline[points="20,6 9,17 4,12"]')).toHaveCount(0);
                await expect(proof.locator("svg")).toHaveCount(1);
                await screenshot("missing-proof");
                await refresh(harness.closed);
                await expect(review).toContainText("Child replay proof failed or is unavailable");
                await expect(proof).toContainText("Child proof unverified");
                await expect(proof).toHaveClass(/\bis-review\b/);
              } else if (failure === "missing-ledger") {
                await screenshot("missing-ledger");
                await refresh(harness.open);
                await expect(review).toHaveCount(0);
                await expect(proof).toContainText("Child proof verified");
                await expect(completed).toHaveText("[]");
                await screenshot("manual-refresh-cleared");
              }
            }
          } else {
            const layout = await page.locator(".df2-quarantine-panel").evaluate(panel => {
              const copy = panel.querySelector(".df2-quarantine-next-copy")!;
              const heading = getComputedStyle(copy.querySelector(":scope > strong")!);
              const inline = Array.from(copy.querySelectorAll("p strong")).map(node => {
                const style = getComputedStyle(node);
                return { display: style.display, transform: style.textTransform, margin: style.marginBottom };
              });
              const header = document.querySelector('[data-testid="recovery-header"]')!.getBoundingClientRect();
              const footer = document.querySelector('[data-testid="recovery-footer"]')!.getBoundingClientRect();
              const box = panel.getBoundingClientRect();
              const summary = panel.querySelector(".df2-quarantine-summary")!.getBoundingClientRect();
              const chips = Array.from(panel.querySelectorAll(".df2-quarantine-summary-chip")).map(chip => {
                const bounds = chip.getBoundingClientRect();
                const style = getComputedStyle(chip);
                return { contained: bounds.left >= summary.left && bounds.right <= summary.right
                    && bounds.top >= summary.top && bounds.bottom <= summary.bottom
                    && bounds.left >= 0 && bounds.right <= innerWidth,
                  scrollWidth: chip.scrollWidth, clientWidth: chip.clientWidth,
                  whiteSpace: style.whiteSpace, radius: style.borderRadius };
              });
              const actions = Array.from(panel.querySelectorAll(".df2-quarantine-next-actions button"));
              const overlaps = actions.some((button, index) => actions.slice(index + 1).some(other => {
                const first = button.getBoundingClientRect(), second = other.getBoundingClientRect();
                return Math.min(first.right, second.right) - Math.max(first.left, second.left) > 1
                  && Math.min(first.bottom, second.bottom) - Math.max(first.top, second.top) > 1;
              }));
              return { overflow: document.documentElement.scrollWidth - innerWidth,
                separated: header.bottom <= box.top + 1 && box.bottom <= footer.top + 1, overlaps,
                heading: { display: heading.display, transform: heading.textTransform }, inline, chips };
            });
            expect(layout.overflow).toBeLessThanOrEqual(1);
            expect(layout.separated).toBe(true);
            expect(layout.overlaps).toBe(false);
            expect(layout.heading).toEqual({ display: "block", transform: "uppercase" });
            expect(layout.inline.length).toBeGreaterThan(0);
            for (const style of layout.inline) expect(style).toEqual({ display: "inline", transform: "none", margin: "0px" });
            expect(layout.chips.length).toBeGreaterThan(0);
            for (const chip of layout.chips) {
              expect(chip.contained).toBe(true);
              expect(chip.scrollWidth).toBeLessThanOrEqual(chip.clientWidth);
              expect(chip.whiteSpace).toBe("normal");
              expect(chip.radius).toBe("4px");
            }
            await testInfo.attach("recovery-layout-evidence", { body: JSON.stringify(layout, null, 2), contentType: "application/json" });
            await screenshot("long-reasons");
            const footer = page.getByTestId("recovery-footer");
            await footer.scrollIntoViewIfNeeded();
            await expect(footer).toBeInViewport({ ratio: 1 });
            const clearance = await footer.evaluate(node => {
              const bounds = node.getBoundingClientRect();
              const panel = document.querySelector(".df2-quarantine-panel")!.getBoundingClientRect();
              return { contained: bounds.top >= 0 && bounds.bottom <= innerHeight,
                separated: panel.bottom <= bounds.top + 1,
                atPageEnd: scrollY + innerHeight >= document.documentElement.scrollHeight - 1 };
            });
            expect(clearance).toEqual({ contained: true, separated: true, atPageEnd: true });
            const footerPath = testInfo.outputPath(`recovery-page-end-${viewport.width}x${viewport.height}.png`);
            await page.screenshot({ path: footerPath, animations: "disabled" });
            await testInfo.attach("recovery-page-end", { path: footerPath, contentType: "image/png" });
            const edit = page.getByRole("button", { name: "Edit", exact: true });
            await edit.click();
            const dialog = page.getByRole("dialog", { name: "Edit quarantined value" });
            const input = dialog.getByLabel("Value", { exact: true });
            const sqlNull = dialog.getByRole("checkbox", { name: /SQL NULL/ });
            await expect(input).toHaveValue("bad");
            await sqlNull.check();
            await expect(input).toBeDisabled();
            await expect(input).toHaveValue("");
            await sqlNull.uncheck();
            await expect(input).toBeEnabled();
            await input.fill("42");
            await sqlNull.check();
            const geometry = await dialog.evaluate(panel => {
              const box = panel.getBoundingClientRect();
              const header = panel.querySelector(".df2-modal-header")!.getBoundingClientRect();
              const body = panel.querySelector(".df2-modal-body")!.getBoundingClientRect();
              const footer = panel.querySelector(".df2-modal-footer")!.getBoundingClientRect();
              return { overflow: panel.scrollWidth - panel.clientWidth,
                contained: box.left >= 0 && box.right <= innerWidth + 1 && box.top >= 0 && box.bottom <= innerHeight + 1,
                separated: header.bottom <= body.top + 1 && body.bottom <= footer.top + 1 };
            });
            expect(geometry).toEqual({ overflow: 0, contained: true, separated: true });
            await expect(dialog.locator(".df2-modal-header")).toBeInViewport();
            await expect(dialog.locator(".df2-modal-footer")).toBeInViewport();
            await screenshot("editor-null");
            await dialog.getByRole("button", { name: "Save", exact: true }).click();
            await expect(dialog).toHaveCount(0);
            await expect(edit).toBeFocused();
            await expect(page.locator(".df2-quarantine-value")).toHaveText("NULL");
            await edit.click();
            await expect(sqlNull).toBeChecked();
            await sqlNull.uncheck();
            await input.fill("unsaved");
            await page.keyboard.press("Escape");
            await expect(dialog).toHaveCount(0);
            await expect(edit).toBeFocused();
            await expect(page.locator(".df2-quarantine-value")).toHaveText("NULL");
            await page.getByRole("button", { name: "Propose repair", exact: true }).click();
            await expect.poll(() => harness.evidence.proposals.length).toBe(1);
            await expect(page.locator(".dt-toast--error").last()).toContainText("Synthetic proposal refused");
            expect(harness.evidence.proposals[0]).toMatchObject({ job_id: "qa-recovery", rejected_details: [{ column: "amount", value: null }] });
            await toastClearance("proposal-error");
            await page.locator(".dt-toast--error").last().getByRole("button", { name: "View notification details" }).click();
            const notificationDialog = page.getByRole("dialog", { name: "Notification details", exact: true });
            await expect(notificationDialog).toBeVisible();
            await expect(notificationDialog).toContainText("Synthetic proposal refused");
            await expect(page.locator(".dt-toast-host")).toBeHidden();
            await screenshot("notification-review");
            await page.keyboard.press("Escape");
            await expect(notificationDialog).toHaveCount(0);
            await toastClearance("proposal-error-after-review");
          }
          if (scenario === "state and proof gates") await refresh(harness.open);
          harness.setResult(harness.verified);
          harness.setLedger(harness.closed);
          harness.hold();
          const postsBefore = harness.evidence.posts.length;
          const getsBefore = harness.evidence.gets;
          await replay().click();
          await expect.poll(() => harness.evidence.posts.length).toBe(postsBefore + 1);
          await expect(completed).toHaveText("[]");
          harness.releaseReplay();
          await expect.poll(() => harness.evidence.gets).toBe(getsBefore + 1);
          await expect(completed).toHaveText("[]");
          harness.releaseGet();
          await expect(completed).toHaveText('["qa-child"]');
          await expect(proof).toContainText("Child proof verified");
          await expect(proof).toHaveClass(/\bis-verified\b/);
          await expect(proof).not.toHaveClass(/\bis-review\b/);
          await expect(review).toHaveCount(0);
          await expect(proof.locator('svg polyline[points="20,6 9,17 4,12"]')).toHaveCount(1);
          await expect(replay()).toHaveCount(0);
          expect(harness.evidence.posts.length).toBe(postsBefore + 1);
          expect(harness.evidence.posts.at(-1)).toMatchObject({ rows: [{ column: "amount",
            value: scenario === "state and proof gates" ? "bad" : null,
            values: { id: "2", amount: scenario === "state and proof gates" ? "bad" : null } }] });
          expect(harness.evidence.events.slice(-4)).toEqual(["POST requested", "POST fulfilled", "GET requested", "GET fulfilled"]);
          expect(harness.evidence.blocked).toEqual([]);
          expect(harness.evidence.failures).toEqual([]);
          await toastClearance("verified-closed");
          await screenshot("verified-closed");
          await testInfo.attach("recovery-api-evidence", { body: JSON.stringify(harness.evidence, null, 2), contentType: "application/json" });
        } finally {
          await browser.close();
        }
      });
    }
  }

  for (const viewport of [{ width: 1440, height: 1000 }, { width: 390, height: 844 }]) {
    test(`recovery partial sample refusal ${viewport.width}x${viewport.height}`, async ({ playwright, baseURL }, testInfo) => {
      const browser = await playwright.chromium.launch({ channel: process.env.QA_BROWSER_CHANNEL ?? "msedge" });
      const page = await browser.newPage({ viewport });
      try {
        const harness = await quarantineRecoveryHarness(page, new URL(baseURL ?? "http://127.0.0.1:5173").origin);
        const partial = { ...harness.open, replay_block_reason: "Quarantine sample is incomplete (1 of 2 rejects)." };
        harness.setLedger(partial);
        const getCount = harness.evidence.gets;
        await page.getByRole("button", { name: "Refresh", exact: true }).click();
        await expect.poll(() => harness.evidence.gets).toBe(getCount + 1);
        await expect(page.getByRole("alert")).toContainText("Quarantine sample is incomplete");
        await expect(page.getByRole("button", { name: "Replay quarantined rows", exact: true })).toHaveCount(0);
        const layout = await page.evaluate(() => {
          const warning = document.querySelector(".df2-quarantine-next-copy [role=alert]")?.getBoundingClientRect();
          const footer = document.querySelector("[data-testid=recovery-footer]")?.getBoundingClientRect();
          return warning && footer ? {
            warningLeft: warning.left,
            warningRight: warning.right,
            warningBottom: warning.bottom,
            footerTop: footer.top,
            viewportWidth: window.innerWidth,
          } : null;
        });
        expect(layout).not.toBeNull();
        expect(layout!.warningLeft).toBeGreaterThanOrEqual(0);
        expect(layout!.warningRight).toBeLessThanOrEqual(layout!.viewportWidth);
        expect(layout!.warningBottom).toBeLessThanOrEqual(layout!.footerTop);
        expect(harness.evidence.posts).toHaveLength(0);
        expect(harness.evidence.failures).toEqual([]);
        await testInfo.attach("partial-sample-layout", { body: JSON.stringify(layout), contentType: "application/json" });
      } finally {
        await browser.close();
      }
    });
  }
});

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

async function pendingFeatureHarness(page: Page, origin: string, fixture: string) {
  const failures: string[] = [];
  const blocked: string[] = [];
  page.on("pageerror", error => failures.push(error.message));
  await page.route("**/*", async route => {
    const url = new URL(route.request().url());
    if (url.origin !== origin || url.pathname.startsWith("/api/")) {
      blocked.push(url.pathname);
      return route.abort();
    }
    if (url.pathname !== "/__qa_pending_features") return route.continue();
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
        const { DestinationAdvancedDrawer } = await import('/src/components/transfer/DestinationAdvancedDrawer.tsx');
        const { ToastProvider, useToast } = await import('/src/components/Toast.tsx');
        const constants = await import('/src/lib/transferConstants.ts');
        const element = React.createElement;
        const params = new URL(location.href).searchParams;
        const noop = () => {};
        function renderDestinationAdvancedDrawer() {
          const [key, setKey] = React.useState('order_id');
          const [modes, setModes] = React.useState([]);
          const [open, setOpen] = React.useState(true);
          return element(React.Fragment, null,
            element('output', { 'data-testid': 'mode-changes' }, JSON.stringify(modes)),
            element(DestinationAdvancedDrawer, {
              open, onClose: () => setOpen(false), syncModes: constants.SYNC_MODES,
              schemaPolicies: constants.SCHEMA_POLICIES, validationModes: constants.VALIDATION_MODES,
              dateLocales: constants.DATE_LOCALES, numberLocales: constants.NUMBER_LOCALES,
              syncMode: 'full_refresh_append', schemaPolicy: 'manual_review', validationMode: 'strict',
              dateLocale: '', numberLocale: '', backfillNewFields: false,
              streamNames: ['sample-orders'], streamFields: {}, defaultCursor: '', defaultPrimaryKey: key,
              sourceColumns: ['order_id', 'status'], sourceSchema: { order_id: 'INTEGER', status: 'TEXT' },
              syncModeLabel: 'Full append', schemaPolicyLabel: 'Manual approval', requiresCursor: false,
              requiresPrimaryKey: false, streamNeedsReview: false,
              onSyncModeChange: mode => setModes(previous => [...previous, mode]),
              onSchemaPolicyChange: noop, onValidationModeChange: noop, onDateLocaleChange: noop,
              onNumberLocaleChange: noop, onBackfillChange: noop, onStreamCursorChange: noop,
              onStreamCursorSemanticsChange: noop, onStreamPrimaryKeyChange: (_stream, value) => setKey(value),
              syncHonestyLine: constants.syncModeHonestyLine('full_refresh_append',
                params.get('exists') === 'unknown' ? null : params.get('exists') === 'true', params.get('db') || 'mysql'),
            }));
        }
        function ToastFixture() {
          const { toast } = useToast();
          const emit = () => {
            const count = params.get('fixture') === 'toast-single' ? 1 : 3;
            for (let index = 1; index <= count; index++) toast({ tone: 'warning',
              title: 'Synthetic warning ' + index,
              message: ('Order identity requires explicit operator review before any write. ' +
                'Inspect the full schema policy and destination constraints. ').repeat(8) + 'END-WARNING-' + index });
          };
          return element('section', { className: 'df2-page-transfer-studio' },
            element('header', { className: 'df2-transfer-studio-chrome' },
              element('div', { className: 'df2-transfer-studio-chrome-row' },
                element('div', { className: 'df2-wizard-studio' },
                  element('div', { className: 'df2-wizard-track' }, 'Source  Destination  Transform  Map  Validate  Run')),
                element('div', { className: 'df2-route-bar' }, 'Postgres source  →  MySQL destination  ·  5.1k rows'),
                element('div', { className: 'df2-toast-dock', 'data-df2-toast-dock': '' }))),
            element('button', { onClick: emit }, 'Emit synthetic warnings'),
            element('button', null, 'Stable focus target'));
        }
        createRoot(document.getElementById('root')).render(params.get('fixture') === 'drawer'
          ? element(renderDestinationAdvancedDrawer)
          : element(ToastProvider, null, element(ToastFixture)));
      </script></body></html>` });
  });
  await page.goto(`${origin}/__qa_pending_features?${fixture}`);
  await expect.poll(async () => {
    if (failures.length) throw new Error(failures.join("\n"));
    return page.locator('#root > *').count();
  }).toBeGreaterThan(0);
  return { failures, blocked };
}

test.describe("pending feature browser QA", () => {
  for (const viewport of [{ width: 1440, height: 1000 }, { width: 390, height: 844 }]) {
    test(`append identity and conditional MySQL caption ${viewport.width}x${viewport.height}`, async ({ playwright, baseURL }, testInfo) => {
      const browser = await playwright.chromium.launch({ channel: process.env.QA_BROWSER_CHANNEL ?? "msedge" });
      const page = await browser.newPage({ viewport });
      const origin = new URL(baseURL ?? "http://127.0.0.1:5173").origin;
      try {
        const evidence = await pendingFeatureHarness(page, origin, "fixture=drawer&db=mysql&exists=false");
        const dialog = page.getByRole("dialog");
        await expect(dialog).toBeVisible();
        await expect(dialog).toContainText("plain append creates no PRIMARY KEY");
        await page.evaluate(() => document.fonts.ready);
        await page.screenshot({ path: testInfo.outputPath(`identity-caption-${viewport.width}.png`), animations: "disabled" });
        const identity = dialog.getByRole("combobox", { name: "Identity key for sample-orders", exact: true });
        const cell = dialog.locator('td[data-label="Identity key"]');
        const hint = cell.locator(".df2-label-hint");
        await identity.scrollIntoViewIfNeeded();
        await expect(identity).toHaveValue("order_id");
        await expect(identity).toBeEnabled();
        await expect(hint).toHaveText("Identity metadata; Validate checks the effective write policy.");
        await expect(dialog.locator('td[data-label="Cursor"]')).not.toContainText("Identity metadata");
        await expect(dialog.getByRole("combobox", { name: "Cursor field for sample-orders", exact: true })).toBeDisabled();
        const geometry = await cell.evaluate(node => {
          const select = node.querySelector("select")!.getBoundingClientRect();
          const hintNode = node.querySelector(".df2-label-hint")!;
          const hintBox = hintNode.getBoundingClientRect();
          const panel = node.closest('[role="dialog"]')!;
          const panelBox = panel.getBoundingClientRect();
          const body = panel.querySelector(".df2-drawer-body")!;
          return { below: hintBox.top >= select.bottom - 1, hintWidth: hintBox.width,
            hintLeft: hintBox.left, selectLeft: select.left,
            clipped: hintNode.scrollWidth > hintNode.clientWidth + 1,
            contained: panelBox.left >= 0 && panelBox.right <= innerWidth + 1,
            overflow: Math.max(document.documentElement.scrollWidth - innerWidth,
              panel.scrollWidth - panel.clientWidth, body.scrollWidth - body.clientWidth) };
        });
        expect(geometry.below).toBe(true);
        expect(geometry.hintLeft).toBeGreaterThanOrEqual(geometry.selectLeft);
        expect(geometry.hintWidth).toBeGreaterThan(0);
        expect(geometry.clipped).toBe(false);
        expect(geometry.contained).toBe(true);
        expect(geometry.overflow).toBeLessThanOrEqual(1);
        await page.screenshot({ path: testInfo.outputPath(`identity-row-${viewport.width}.png`), animations: "disabled" });
        await identity.selectOption("status");
        await expect(identity).toHaveValue("status");
        await expect(dialog.locator('td[data-label="Mode"]')).toHaveText("Full append");
        await expect(page.getByTestId("mode-changes")).toHaveText("[]");
        for (const query of ["db=mysql&exists=true", "db=mysql&exists=unknown", "db=postgresql&exists=false"]) {
          await page.goto(`${origin}/__qa_pending_features?fixture=drawer&${query}`);
          await expect(dialog).toBeVisible();
          await expect(dialog).not.toContainText("plain append creates no PRIMARY KEY");
        }
        expect(evidence.failures).toEqual([]);
        expect(evidence.blocked).toEqual([]);
      } finally {
        await browser.close();
      }
    });

    test(`long warning preview review and expiry focus ${viewport.width}x${viewport.height}`, async ({ playwright, baseURL }, testInfo) => {
      const browser = await playwright.chromium.launch({ channel: process.env.QA_BROWSER_CHANNEL ?? "msedge" });
      const page = await browser.newPage({ viewport });
      const origin = new URL(baseURL ?? "http://127.0.0.1:5173").origin;
      try {
        await page.clock.install();
        const evidence = await pendingFeatureHarness(page, origin, "fixture=toast");
        await page.getByRole("button", { name: "Emit synthetic warnings", exact: true }).click();
        const toasts = page.locator(".dt-toast");
        const more = page.getByRole("button", { name: "Review 1 more notifications", exact: true, includeHidden: true });
        await expect(toasts).toHaveCount(2);
        await expect(more).toBeVisible();
        await expect(page.locator(".df2-toast-dock .dt-toast")).toHaveCount(2);
        await page.evaluate(() => document.fonts.ready);
        const preview = await toasts.evaluateAll(nodes => nodes.map(node => {
          const title = node.querySelector(".dt-toast-title")!;
          const titleStyle = getComputedStyle(title);
          const titleBox = title.getBoundingClientRect();
          const message = node.querySelector(".dt-toast-message")!;
          const style = getComputedStyle(message);
          const box = message.getBoundingClientRect();
          const toastBox = node.getBoundingClientRect();
          const bodyBox = node.querySelector(".dt-toast-body")!.getBoundingClientRect();
          const buttons = Array.from(node.querySelectorAll("button")).map(button => button.getBoundingClientRect());
          return { width: toastBox.width, titleWidth: titleBox.width, messageWidth: box.width,
            titleWhiteSpace: titleStyle.whiteSpace, titleHeight: titleBox.height,
            titleLineHeight: parseFloat(titleStyle.lineHeight), titleOverflow: titleStyle.textOverflow,
            whiteSpace: style.whiteSpace, height: box.height, lineHeight: parseFloat(style.lineHeight),
            contained: toastBox.left >= 0 && toastBox.right <= innerWidth + 1,
            controlsClear: buttons.every(button => button.left >= bodyBox.right - 1),
            clipped: buttons.some(button => button.right > toastBox.right + 1) };
        }));
        for (const snapshot of preview) {
          expect(snapshot.width).toBeGreaterThanOrEqual(180);
          expect(snapshot.titleWidth).toBeGreaterThan(20);
          expect(snapshot.messageWidth).toBeGreaterThan(20);
          expect(snapshot.titleWhiteSpace).toBe("nowrap");
          expect(snapshot.titleHeight).toBeLessThanOrEqual(snapshot.titleLineHeight + 2);
          expect(snapshot.titleOverflow).toBe("ellipsis");
          expect(snapshot.whiteSpace).toBe("nowrap");
          expect(snapshot.height).toBeLessThanOrEqual(snapshot.lineHeight + 2);
          expect(snapshot.contained).toBe(true);
          expect(snapshot.controlsClear).toBe(true);
          expect(snapshot.clipped).toBe(false);
        }
        expect(await page.evaluate(() => document.documentElement.scrollWidth - innerWidth)).toBeLessThanOrEqual(1);
        await page.screenshot({ path: testInfo.outputPath(`toast-preview-${viewport.width}.png`), animations: "disabled" });
        const details = page.getByRole("button", { name: "View notification details", exact: true }).last();
        await details.click();
        const dialog = page.getByRole("dialog", { name: "Notification details", exact: true });
        await expect(dialog).toBeFocused();
        const host = page.locator(".dt-toast-host");
        await expect(host).toHaveCSS("display", "none");
        await expect(host).toHaveAttribute("inert", "");
        await expect(dialog.locator(".dt-toast-review-item")).toHaveCount(1);
        await expect(dialog).toContainText("END-WARNING-3");
        const fullMessage = (index: number) => ("Order identity requires explicit operator review before any write. " +
          "Inspect the full schema policy and destination constraints. ").repeat(8) + `END-WARNING-${index}`;
        await expect(dialog.getByText(fullMessage(3), { exact: true })).toHaveCount(1);
        await page.keyboard.press("Escape");
        await expect(dialog).toHaveCount(0);
        await expect(host).toBeVisible();
        await expect(host).not.toHaveAttribute("inert", "");
        await expect(details).toBeFocused();
        await more.click();
        await expect(dialog).toBeFocused();
        await expect(dialog.locator(".dt-toast-review-item")).toHaveCount(3);
        for (const index of [1, 2, 3]) {
          await expect(dialog).toContainText(`END-WARNING-${index}`);
          await expect(dialog.getByText(fullMessage(index), { exact: true })).toHaveCount(1);
        }
        await page.clock.fastForward(11000);
        await expect(toasts).toHaveCount(2);
        await expect(more).toHaveCount(1);
        await expect(more).toBeHidden();
        await expect(host).toHaveCSS("display", "none");
        await expect(host).toHaveAttribute("inert", "");
        await expect(dialog).toBeVisible();
        const reviewGeometry = await dialog.evaluate(panel => {
          const body = panel.querySelector(".df2-modal-body")!;
          const box = panel.getBoundingClientRect();
          const header = panel.querySelector(".df2-modal-header")!.getBoundingClientRect();
          const bodyBox = body.getBoundingClientRect();
          return { contained: box.left >= 0 && box.right <= innerWidth + 1 && box.top >= 0 && box.bottom <= innerHeight + 1,
            separated: header.bottom <= bodyBox.top + 1,
            clipped: Array.from(panel.querySelectorAll("h3, p, button")).some(node => node.scrollWidth > node.clientWidth + 1),
            overflow: Math.max(panel.scrollWidth - panel.clientWidth, body.scrollWidth - body.clientWidth) };
        });
        expect(reviewGeometry.contained).toBe(true);
        expect(reviewGeometry.separated).toBe(true);
        expect(reviewGeometry.clipped).toBe(false);
        expect(reviewGeometry.overflow).toBeLessThanOrEqual(1);
        await page.screenshot({ path: testInfo.outputPath(`toast-review-top-${viewport.width}.png`), animations: "disabled" });
        await dialog.locator(".df2-modal-body").evaluate(body => { body.scrollTop = body.scrollHeight; });
        await expect(dialog.getByText(/END-WARNING-1$/)).toBeInViewport();
        await page.screenshot({ path: testInfo.outputPath(`toast-review-bottom-${viewport.width}.png`), animations: "disabled" });
        const close = dialog.getByRole("button", { name: "Close", exact: true });
        const closeReceivesPointer = await close.evaluate(button => {
          const box = button.getBoundingClientRect();
          return button.contains(document.elementFromPoint(box.left + box.width / 2, box.top + box.height / 2));
        });
        await page.keyboard.press("Tab");
        await expect(close).toBeFocused();
        await page.keyboard.press("Enter");
        await expect(dialog).toHaveCount(0);
        await expect(host).toBeVisible();
        await expect(host).not.toHaveAttribute("inert", "");
        await expect(more).toBeVisible();
        await expect(more).toBeFocused();
        await page.screenshot({ path: testInfo.outputPath(`toast-restored-${viewport.width}.png`), animations: "disabled" });
        const stable = page.getByRole("button", { name: "Stable focus target", exact: true });
        await stable.focus();
        await page.clock.fastForward(10001);
        await expect(toasts).toHaveCount(0);
        await expect(more).toHaveCount(0);
        await expect(stable).toBeFocused();
        await page.screenshot({ path: testInfo.outputPath(`toast-expired-${viewport.width}.png`), animations: "disabled" });
        expect(evidence.failures).toEqual([]);
        expect(evidence.blocked).toEqual([]);
        expect(closeReceivesPointer, "Toast preview must not cover the notification review Close control").toBe(true);
      } finally {
        await browser.close();
      }
    });
  }

  test("single Transfer Studio toast has readable width", async ({ playwright, baseURL }) => {
    const browser = await playwright.chromium.launch({ channel: process.env.QA_BROWSER_CHANNEL ?? "msedge" });
    const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
    const origin = new URL(baseURL ?? "http://127.0.0.1:5173").origin;
    try {
      await pendingFeatureHarness(page, origin, "fixture=toast-single");
      await page.getByRole("button", { name: "Emit synthetic warnings", exact: true }).click();
      const toast = page.locator(".df2-toast-dock .dt-toast");
      await expect(toast).toHaveCount(1);
      const geometry = await toast.evaluate(node => ({
        width: node.getBoundingClientRect().width,
        titleWidth: node.querySelector(".dt-toast-title")!.getBoundingClientRect().width,
        messageWidth: node.querySelector(".dt-toast-message")!.getBoundingClientRect().width,
      }));
      expect(geometry.width).toBeGreaterThanOrEqual(180);
      expect(geometry.titleWidth).toBeGreaterThan(20);
      expect(geometry.messageWidth).toBeGreaterThan(20);
    } finally {
      await browser.close();
    }
  });
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

test.describe("Jobs detail route and incremental snapshot layout", () => {
  for (const viewport of [
    { width: 1440, height: 1000 },
    { width: 768, height: 900 },
    { width: 390, height: 844 },
  ]) {
    test(`long endpoints and aligned snapshot fields ${viewport.width}x${viewport.height}`, async ({ playwright, baseURL }, testInfo) => {
      const browser = await playwright.chromium.launch({ channel: process.env.QA_BROWSER_CHANNEL ?? "msedge" });
      const page = await browser.newPage({ viewport });
      const origin = new URL(baseURL ?? "http://127.0.0.1:5173").origin;
      const failures: string[] = [];
      page.on("pageerror", (error) => failures.push(error.message));
      try {
        await page.route("**/*", async (route) => {
          const url = new URL(route.request().url());
          if (url.pathname.startsWith("/api/")) {
            const body = url.pathname === "/api/v1/connectors/jobs/qa-long-route"
              ? {
                  _id: "qa-long-route", name: "qa_my_seed → defaultdb.mahi_qa_20261106_1902_schedule",
                  source_type: "postgresql",
                  source_name: "enterprise_primary_warehouse_source_cluster_identifier_region_replica_2026",
                  destination_type: "snowflake",
                  destination_database: "regional_analytics_destination_database_customer_archive_2026",
                  destination_collection: "transactional_orders_versioned_destination_collection",
                  sync_mode: "cdc", cdc_plugin: "postgresql", status: "completed",
                  records_processed: 1200, created_at: "2026-11-08T00:00:00Z",
                }
              : { signals: [], context: {} };
            return route.fulfill({ contentType: "application/json", body: JSON.stringify(body) });
          }
          if (url.origin !== origin || url.pathname !== "/__qa_job_snapshot_layout") return route.continue();
          return route.fulfill({ contentType: "text/html", body: `<!doctype html>
            <html><head><meta name="viewport" content="width=device-width, initial-scale=1">
            <link rel="stylesheet" href="/src/styles/marketing-base.css?direct">
            <link rel="stylesheet" href="/src/styles/app-styles.css?direct"></head>
            <body><main id="root" class="df2-app" style="display:block;min-height:100vh;padding:12px"></main>
            <script type="module">
              import RefreshRuntime from '/@react-refresh';
              RefreshRuntime.injectIntoGlobalHook(window);
              window.$RefreshReg$ = () => {};
              window.$RefreshSig$ = (type) => type;
              window.__vite_plugin_react_preamble_installed__ = true;
              const React = (await import('/node_modules/.vite/deps/react.js')).default;
              const { createRoot } = (await import('/node_modules/.vite/deps/react-dom_client.js')).default;
              const { JobsPage } = await import('/src/pages/JobsPage.tsx');
              const { ToastProvider } = await import('/src/components/Toast.tsx');
              const { ConfirmProvider } = await import('/src/components/ui/ConfirmDialog.tsx');
              const source = 'enterprise_primary_warehouse_source_cluster_identifier_region_replica_2026';
              const database = 'regional_analytics_destination_database_customer_archive_2026';
              const table = 'transactional_orders_versioned_destination_collection';
              const jobs = [{ _id: 'qa-long-route', name: 'qa_my_seed → defaultdb.mahi_qa_20261106_1902_schedule',
                source_type: 'postgresql', source_name: source,
                destination_type: 'snowflake', destination_database: database,
                destination_collection: table, sync_mode: 'cdc', cdc_plugin: { enabled: true },
                status: 'completed', records_processed: 1200, created_at: '2026-11-08T00:00:00Z' }];
              const element = React.createElement;
              createRoot(document.getElementById('root')).render(
                element(ToastProvider, null, element(ConfirmProvider, null,
                  element(JobsPage, { jobs, initialJobId: 'qa-long-route' })))
              );
            </script></body></html>` });
        });
        await page.goto(`${origin}/__qa_job_snapshot_layout`);
        await expect(page.locator(".df2-jobs-v3-summary-route")).toBeVisible();
        const snapshotPanel = page.getByRole("region", { name: "CDC incremental snapshot" });
        await expect(snapshotPanel).toBeVisible();
        await snapshotPanel.scrollIntoViewIfNeeded();
        await expect.poll(() => failures).toEqual([]);
        const layout = await page.evaluate(() => {
          const route = document.querySelector<HTMLElement>(".df2-jobs-v3-summary-route")!;
          const names = Array.from(route.querySelectorAll<HTMLElement>(".df2-jobs-v3-summary-endpoint > strong"));
          const titleLine = document.querySelector<HTMLElement>(".df2-jobs-v3-summary-title-line")!;
          const jobTitle = titleLine.querySelector<HTMLElement>(".df2-job-name-editor-title")!;
          const status = titleLine.querySelector<HTMLElement>(".df2-jobs-v3-summary-ids")!;
          const titleBox = jobTitle.getBoundingClientRect();
          const statusBox = status.getBoundingClientRect();
          const controls = Array.from(document.querySelectorAll<HTMLElement>(".df2-cdc-snapshot-form .df2-cdc-snapshot-control"));
          return {
            viewportWidth: window.innerWidth,
            documentWidth: Math.max(document.documentElement.scrollWidth, document.body.scrollWidth),
            titleLineWidth: titleLine.getBoundingClientRect().width,
            titleLineScrollWidth: titleLine.scrollWidth,
            jobTitle: {
              title: jobTitle.title,
              textOverflow: getComputedStyle(jobTitle).textOverflow,
              overlapsStatus: titleBox.right > statusBox.left && titleBox.left < statusBox.right
                && titleBox.bottom > statusBox.top && titleBox.top < statusBox.bottom,
            },
            statusRight: statusBox.right,
            routeWidth: route.getBoundingClientRect().width,
            routeScrollWidth: route.scrollWidth,
            labels: Array.from(route.querySelectorAll<HTMLElement>(".df2-jobs-v3-summary-endpoint > span"))
              .map((label) => getComputedStyle(label).display),
            names: names.map((name) => ({ title: name.title, textOverflow: getComputedStyle(name).textOverflow })),
            controlBoxes: controls.map((control) => {
              const rect = control.getBoundingClientRect();
              return { width: rect.width, height: rect.height };
            }),
          };
        });
        expect(layout.documentWidth).toBeLessThanOrEqual(viewport.width);
        expect(layout.titleLineScrollWidth).toBeLessThanOrEqual(layout.titleLineWidth + 1);
        expect(layout.jobTitle.title).toBe("qa_my_seed → defaultdb.mahi_qa_20261106_1902_schedule");
        expect(layout.jobTitle.textOverflow).toBe("ellipsis");
        expect(layout.jobTitle.overlapsStatus).toBe(false);
        expect(layout.statusRight).toBeLessThanOrEqual(viewport.width);
        expect(layout.routeScrollWidth).toBeLessThanOrEqual(layout.routeWidth + 1);
        expect(layout.labels).toEqual(["block", "block"]);
        expect(layout.names.map((name) => name.title)).toEqual([
          "enterprise_primary_warehouse_source_cluster_identifier_region_replica_2026",
          "regional_analytics_destination_database_customer_archive_2026.transactional_orders_versioned_destination_collection",
        ]);
        expect(layout.names.every((name) => name.textOverflow === "ellipsis")).toBe(true);
        expect(layout.controlBoxes).toHaveLength(4);
        expect(layout.controlBoxes.every((box) => Math.abs(box.height - 40) <= 1), JSON.stringify(layout.controlBoxes)).toBe(true);
        if (viewport.width > 900 || viewport.width <= 540) {
          const widths = layout.controlBoxes.map((box) => box.width);
          expect(Math.max(...widths) - Math.min(...widths)).toBeLessThanOrEqual(1);
        } else {
          const widths = layout.controlBoxes.slice(0, 3).map((box) => box.width);
          expect(Math.max(...widths) - Math.min(...widths)).toBeLessThanOrEqual(1);
        }
        const filter = page.locator("#cdc-snap-filter-qa-long-route");
        await filter.scrollIntoViewIfNeeded();
        const filterGeometry = await filter.evaluate((control) => {
          const field = control.getBoundingClientRect();
          const footer = document.querySelector<HTMLElement>(".df2-jobs-detail-footer")?.getBoundingClientRect();
          return footer ? { left: field.left, right: field.right, bottom: field.bottom, footerTop: footer.top } : null;
        });
        expect(filterGeometry).not.toBeNull();
        expect(filterGeometry!.left).toBeGreaterThanOrEqual(0);
        expect(filterGeometry!.right).toBeLessThanOrEqual(viewport.width);
        expect(filterGeometry!.bottom).toBeLessThanOrEqual(filterGeometry!.footerTop + 1);
        await testInfo.attach("jobs-route-snapshot-layout", { body: JSON.stringify(layout, null, 2), contentType: "application/json" });
        await page.screenshot({ path: testInfo.outputPath(`jobs-route-snapshot-${viewport.width}x${viewport.height}.png`), fullPage: true });
      } finally {
        await browser.close();
      }
    });
  }
});

test.describe("Settings responsive navigation and notice layout", () => {
  for (const viewport of [
    { width: 1440, height: 1000 },
    { width: 390, height: 844 },
  ]) {
    test(`settings sections and alerts stay contained ${viewport.width}x${viewport.height}`, async ({ playwright, baseURL }, testInfo) => {
      const browser = await playwright.chromium.launch({ channel: process.env.QA_BROWSER_CHANNEL ?? "msedge" });
      const page = await browser.newPage({ viewport });
      const origin = new URL(baseURL ?? "http://127.0.0.1:5173").origin;
      const failures: string[] = [];
      page.on("pageerror", (error) => failures.push(error.message));
      try {
        await page.route("**/*", async (route) => {
          const url = new URL(route.request().url());
          if (url.pathname.startsWith("/api/")) {
            return route.fulfill({ status: 503, contentType: "application/json", body: JSON.stringify({ detail: "Synthetic settings read refusal" }) });
          }
          if (url.origin !== origin || url.pathname !== "/__qa_settings_layout") return route.continue();
          return route.fulfill({ contentType: "text/html", body: `<!doctype html>
            <html><head><meta name="viewport" content="width=device-width, initial-scale=1">
            <link rel="stylesheet" href="/src/styles/marketing-base.css?direct">
            <link rel="stylesheet" href="/src/styles/app-styles.css?direct"></head>
            <body><main id="root" class="df2-app" style="display:block;min-height:100vh;padding:12px"></main>
            <script type="module">
              import RefreshRuntime from '/@react-refresh';
              RefreshRuntime.injectIntoGlobalHook(window);
              window.$RefreshReg$ = () => {};
              window.$RefreshSig$ = (type) => type;
              window.__vite_plugin_react_preamble_installed__ = true;
              const React = (await import('/node_modules/.vite/deps/react.js')).default;
              const { createRoot } = (await import('/node_modules/.vite/deps/react-dom_client.js')).default;
              const { SettingsPage } = await import('/src/pages/SettingsPage.tsx');
              const { ToastProvider } = await import('/src/components/Toast.tsx');
              const { ConfirmProvider } = await import('/src/components/ui/ConfirmDialog.tsx');
              const element = React.createElement;
              createRoot(document.getElementById('root')).render(
                element(ToastProvider, null, element(ConfirmProvider, null, element(SettingsPage)))
              );
            </script></body></html>` });
        });
        await page.goto(`${origin}/__qa_settings_layout`);
        await expect(page.locator(".df2-settings-layout")).toBeVisible();
        await expect(page.getByRole("tab", { name: /General/ })).toBeVisible();
        await expect(page.getByTestId("settings-load-error")).toBeVisible();
        await expect.poll(() => failures).toEqual([]);
        const layout = await page.evaluate(() => {
          const nav = document.querySelector<HTMLElement>(".df2-settings-nav")!;
          const notice = document.querySelector<HTMLElement>("[data-testid=settings-load-error]")!;
          const section = notice.closest<HTMLElement>(".df2-settings-section")!;
          const navBox = nav.getBoundingClientRect();
          const sectionBox = section.getBoundingClientRect();
          const noticeBox = notice.getBoundingClientRect();
          return {
            viewportWidth: innerWidth,
            documentWidth: Math.max(document.documentElement.scrollWidth, document.body.scrollWidth),
            navDisplay: getComputedStyle(nav).display,
            navGridColumns: getComputedStyle(nav).gridTemplateColumns,
            navOverflowX: getComputedStyle(nav).overflowX,
            navOverflowY: getComputedStyle(nav).overflowY,
            navClientHeight: nav.clientHeight,
            navScrollHeight: nav.scrollHeight,
            navClientWidth: nav.clientWidth,
            navScrollWidth: nav.scrollWidth,
            tabWidths: Array.from(nav.querySelectorAll<HTMLElement>("button")).map((button) => ({
              width: button.getBoundingClientRect().width,
              minWidth: getComputedStyle(button).minWidth,
              flex: getComputedStyle(button).flex,
            })),
            navRight: navBox.right,
            sectionLeft: sectionBox.left,
            noticeLeft: noticeBox.left,
            noticeRight: noticeBox.right,
            noticeBottom: noticeBox.bottom,
            sectionBodyTop: section.querySelector(".df2-settings-section-body")?.getBoundingClientRect().top ?? 0,
          };
        });
        expect(layout.documentWidth).toBeLessThanOrEqual(viewport.width);
        expect(layout.navScrollHeight).toBeLessThanOrEqual(layout.navClientHeight + 1);
        expect(layout.noticeLeft).toBeGreaterThanOrEqual(0);
        expect(layout.noticeRight).toBeLessThanOrEqual(viewport.width + 1);
        expect(layout.noticeBottom).toBeLessThanOrEqual(layout.sectionBodyTop + 1);
        if (viewport.width <= 1023) {
          expect(layout.navRight).toBeLessThanOrEqual(viewport.width + 1);
          expect(layout.navScrollWidth, JSON.stringify({
            display: layout.navDisplay, columns: layout.navGridColumns,
            overflowX: layout.navOverflowX, overflowY: layout.navOverflowY,
            clientWidth: layout.navClientWidth, scrollWidth: layout.navScrollWidth,
            tabs: layout.tabWidths,
          })).toBeLessThanOrEqual(layout.navClientWidth + 1);
        } else {
          expect(layout.sectionLeft).toBeGreaterThanOrEqual(layout.navRight);
        }
        for (const section of ["General", "Security", "Enterprise", "SSO", "Team", "Notifications", "AI Models", "API Keys", "Audit Logs"]) {
          await page.getByRole("tab", { name: new RegExp(section) }).click();
          await expect(page.locator('[role="tabpanel"]')).toHaveAttribute("aria-label", section);
          const sectionLayout = await page.evaluate(() => {
            const nav = document.querySelector<HTMLElement>(".df2-settings-nav")!;
            const panel = document.querySelector<HTMLElement>(".df2-settings-panel")!;
            const buttons = Array.from(document.querySelectorAll<HTMLElement>(".df2-settings-panel button"));
            return {
              documentWidth: Math.max(document.documentElement.scrollWidth, document.body.scrollWidth),
              navHeightOverflow: nav.scrollHeight - nav.clientHeight,
              navWidthOverflow: nav.scrollWidth - nav.clientWidth,
              panelOverflowY: getComputedStyle(panel).overflowY,
              buttonsOutsideViewport: buttons.some((button) => {
                const rect = button.getBoundingClientRect();
                return rect.width > 0 && (rect.left < -1 || rect.right > innerWidth + 1);
              }),
            };
          });
          expect(sectionLayout.documentWidth, section).toBeLessThanOrEqual(viewport.width);
          expect(sectionLayout.navHeightOverflow, section).toBeLessThanOrEqual(1);
          expect(sectionLayout.navWidthOverflow, section).toBeLessThanOrEqual(1);
          expect(sectionLayout.panelOverflowY, section).not.toMatch(/auto|scroll/);
          expect(sectionLayout.buttonsOutsideViewport, section).toBe(false);
          await page.evaluate(() => window.scrollTo(0, 0));
        }
        await testInfo.attach("settings-layout", { body: JSON.stringify(layout, null, 2), contentType: "application/json" });
        await page.screenshot({ path: testInfo.outputPath(`settings-layout-${viewport.width}x${viewport.height}.png`), fullPage: true });
      } finally {
        await browser.close();
      }
    });
  }
});

test.describe("Contracts lifecycle help stays contextual", () => {
  for (const viewport of [
    { width: 1440, height: 1000 },
    { width: 390, height: 844 },
  ]) {
    test(`lifecycle help is collapsed until requested ${viewport.width}x${viewport.height}`, async ({ playwright, baseURL }, testInfo) => {
      const browser = await playwright.chromium.launch({ channel: process.env.QA_BROWSER_CHANNEL ?? "msedge" });
      const page = await browser.newPage({ viewport });
      const origin = new URL(baseURL ?? "http://127.0.0.1:5173").origin;
      const failures: string[] = [];
      page.on("pageerror", (error) => failures.push(error.message));
      try {
        await page.route("**/*", async (route) => {
          const url = new URL(route.request().url());
          if (url.pathname !== "/__qa_contract_lifecycle") return route.continue();
          return route.fulfill({ contentType: "text/html", body: `<!doctype html>
            <html><head><meta name="viewport" content="width=device-width, initial-scale=1">
            <link rel="stylesheet" href="/src/styles/marketing-base.css?direct">
            <link rel="stylesheet" href="/src/styles/app-styles.css?direct"></head>
            <body><main id="root" class="df2-app" style="display:block;min-height:100vh;padding:12px"></main>
            <script type="module">
              import RefreshRuntime from '/@react-refresh';
              RefreshRuntime.injectIntoGlobalHook(window);
              window.$RefreshReg$ = () => {};
              window.$RefreshSig$ = (type) => type;
              window.__vite_plugin_react_preamble_installed__ = true;
              const React = (await import('/node_modules/.vite/deps/react.js')).default;
              const { createRoot } = (await import('/node_modules/.vite/deps/react-dom_client.js')).default;
              const { ContractsPage } = await import('/src/pages/ContractsPage.tsx');
              const { ToastProvider } = await import('/src/components/Toast.tsx');
              const { ConfirmProvider } = await import('/src/components/ui/ConfirmDialog.tsx');
              const element = React.createElement;
              createRoot(document.getElementById('root')).render(
                element(ToastProvider, null, element(ConfirmProvider, null, element(ContractsPage, { active: false })))
              );
            </script></body></html>` });
        });
        await page.goto(`${origin}/__qa_contract_lifecycle`);
        const note = page.locator(".df2-contracts-lifecycle-note");
        await expect(note).toBeVisible();
        await expect(note).not.toHaveAttribute("open", "");
        await expect(note).not.toContainText(/policy graph|MDM product/i);
        const summary = note.locator("summary");
        await summary.click();
        await expect(note).toHaveAttribute("open", "");
        await expect(note).toContainText("Draft contracts are being prepared");
        expect(failures).toEqual([]);
        const geometry = await note.evaluate((element) => {
          const bounds = element.getBoundingClientRect();
          return { left: bounds.left, right: bounds.right, width: bounds.width, scrollWidth: element.scrollWidth };
        });
        expect(geometry.left).toBeGreaterThanOrEqual(0);
        expect(geometry.right).toBeLessThanOrEqual(viewport.width + 1);
        expect(geometry.scrollWidth).toBeLessThanOrEqual(geometry.width + 1);
        await testInfo.attach("contract-lifecycle-geometry", { body: JSON.stringify(geometry), contentType: "application/json" });
      } finally {
        await browser.close();
      }
    });
  }
});
