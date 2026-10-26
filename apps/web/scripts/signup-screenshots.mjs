// Signup-surface screenshot harness (single, parameterized).
// Drives the running stack over CDP (Devin's Chrome) or a bundled Chromium.
//
// Usage:
//   node apps/web/scripts/signup-screenshots.mjs \
//     [--sha <git-sha>] [--theme light|dark|both] [--viewport all|desktop|mobile|tablet] \
//     [--base http://127.0.0.1:5173] [--out <dir>] [--cdp http://localhost:29229]
//
// PNGs land in docs/signup-screenshots/<sha>/ at the repo root (default),
// named <viewport>-<theme>-<NN>-<state>.png. Console mailer only — never
// sends to a real inbox.
import { chromium } from "playwright";
import { mkdirSync, writeFileSync, rmSync } from "node:fs";
import { execSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { assertScratchMongo } from "./screenshot-scratch-gate.mjs";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

const args = {};
for (let i = 2; i < process.argv.length; i += 2) args[process.argv[i].replace(/^--/, "")] = process.argv[i + 1];
const SHA = args.sha || execSync("git rev-parse --short=8 HEAD", { cwd: repoRoot }).toString().trim();
const BASE = args.base || "http://127.0.0.1:5173";
const OUT = path.resolve(args.out || path.join(repoRoot, "docs/signup-screenshots", SHA));
const THEME = args.theme || "both";
const VP = args.viewport || "all";
const CDP = args.cdp || "http://localhost:29229";

// Safety gate: the harness writes real rows into whatever store the API
// resolves — refuse non-local/scratch Mongo (screenshot-scratch-gate.mjs).
assertScratchMongo();

const VIEWPORTS = {
  desktop: { width: 1440, height: 900 },
  tablet: { width: 768, height: 1024 },
  mobile: { width: 390, height: 844 },
};
mkdirSync(OUT, { recursive: true });
console.log(`output → ${OUT} (sha ${SHA})`);

const browser = await chromium.connectOverCDP(CDP);

async function shot(page, name) {
  await page.screenshot({ path: path.join(OUT, `${name}.png`), fullPage: true });
  console.log("saved", name);
}

async function gotoLogin(page, theme) {
  const ctx = page.context();
  await ctx.addInitScript((t) => {
    window.localStorage.setItem("df2.theme", t);
    // Only the anonymous auth screens lose the session; signed-in
    // destinations (settings, app) keep it.
    if (!location.hash || location.hash === "#/") {
      window.localStorage.removeItem("df2.session");
    }
  }, theme);
  await page.goto(`${BASE}/`, { waitUntil: "networkidle" });
  await page.waitForTimeout(800);
  await page.getByText(/try datawrap free/i).first().click();
  await page.waitForTimeout(900);
}

// Dev-only state seeding through the same file stores the API uses.
// The console mailer never logs codes/tokens (by design), so the harness
// mints them with the real store helpers — same code paths, no backdoor.
const API_DIR = path.join(repoRoot, "apps/api");
const PY = path.join(repoRoot, ".venv/bin/python");
const KNOWN_CODE = "654321";

function py(snippet) {
  const tmp = path.join(OUT, `_seed_${Date.now()}.py`);
  writeFileSync(tmp, snippet);
  try {
    return execSync(`"${PY}" "${tmp}"`, {
      cwd: API_DIR,
      env: { ...process.env, DATAFLOW_AUTH_SECRET: "dev-shots-secret" },
    }).toString().trim();
  } finally {
    rmSync(tmp, { force: true });
  }
}

// Point the live signup challenge at a code the harness knows. The
// challenge's subject_id is the pending-signup id, not the email, so the
// newest unconsumed signup challenge is the one the UI is holding.
function plantOtpCode() {
  py(`
import sys; sys.path.insert(0, ".")
from services.platform_config import data_dir
from services.metadata_backend import json_doc_transaction, mongo_database
from services import otp_service, signup_store
patched = []
# Patch every live signup challenge — concurrent runs and resends leave
# several unconsumed rows, and the newest-by-created_at is not always the
# challenge the UI is holding.
def _hmac(row_id):
    return otp_service.code_hmac(row_id, "signup", "${KNOWN_CODE}")
p = data_dir() / "otp_challenges.json"
with json_doc_transaction(p, {"otp_challenges": []}) as data:
    for row in data["otp_challenges"]:
        if row.get("purpose") == "signup" and not row.get("consumed_at"):
            row["code_hmac"] = _hmac(row["id"])
            patched.append(row["id"])
# The API resolves the Mongo tier whenever MONGODB_URI connects — the
# challenge the UI verifies against lives there, not in the file.
db = mongo_database()
if db is not None:
    coll = db[signup_store.CHALLENGES_COLLECTION]
    for row in coll.find({"purpose": "signup", "consumed_at": None}):
        coll.update_one({"_id": row["_id"]},
                        {"$set": {"code_hmac": _hmac(row["id"])}})
        patched.append(row["id"])
print("patched", len(patched), patched[-1] if patched else "")
`);
}

// Issue a real reset-link token for a verified account via the real store.
function mintResetToken(email) {
  const out = py(`
import sys; sys.path.insert(0, ".")
from services import password_reset_store
token, _doc = password_reset_store.issue(email="${email}", ip="127.0.0.1", ua="screenshot-harness")
print("DWTOKEN::" + token)
`);
  // Log noise can surround the token; extract it by sentinel.
  const m = out.match(/DWTOKEN::([A-Za-z0-9_\-]+)/);
  if (!m) throw new Error(`reset token mint produced no sentinel: ${out.slice(-200)}`);
  return m[1];
}

async function run(vpName, viewport, theme) {
  const ctx = await browser.newContext({ viewport, deviceScaleFactor: 1 });
  const page = await ctx.newPage();
  const tag = `${vpName}-${theme}`;
  const uniq = `${vpName}.${theme}`;
  const runId = Date.now().toString(36);
  const email = `verify.${uniq}.${runId}@datawrap.io`;

  await gotoLogin(page, theme);
  await shot(page, `${tag}-01-login`);

  // Signup — empty state.
  await page.getByText("Create an account").first().click();
  // The form token mints at page mount; submits younger than the 3s
  // anti-bot floor are blocked with a fake challenge and the whole
  // verify/login cascade downstream breaks. Wait past the floor.
  await page.waitForTimeout(4200);
  await shot(page, `${tag}-02-signup-empty`);

  // Signup — error state.
  await page.locator("#signup-name").fill("Ada Lovelace");
  await page.locator('input[type="email"]').first().fill("not-an-email");
  await page.locator('input[type="password"]').first().fill("short");
  await page.locator('button[type="submit"]').last().click();
  await page.waitForTimeout(900);
  await shot(page, `${tag}-03-signup-error`);

  // Signup — valid submission → verify step.
  await page.locator('input[type="email"]').first().fill(email);
  await page.locator('input[type="password"]').first().fill("correct-horse-staple-9");
  await page.locator('button[type="submit"]').last().click();
  await page.waitForSelector("#auth-code", { timeout: 25000 });
  await page.waitForTimeout(400);
  await shot(page, `${tag}-04-signup-verify-empty`);

  // Resend cooldown state (visibly-disabled countdown).
  if (await page.locator('button:has-text("Resend")').count()) {
    await shot(page, `${tag}-05-verify-resend-state`);
  }

  // Wrong code → error.
  await page.locator("#auth-code").fill("000000");
  await page.locator('button[type="submit"]').last().click();
  await page.waitForTimeout(1100);
  await shot(page, `${tag}-06-verify-error`);

  // Real code → verify success hand-off. Gate on the verify form
  // unmounting (success navigates into the app), NOT df2.session — the
  // verify path writes token:"" which readSession() deliberately
  // treats as no-session, so the key can vanish on a good path.
  try {
    plantOtpCode();
    await page.locator("#auth-code").fill(KNOWN_CODE);
    await page.locator('button[type="submit"]').last().click();
    await page.waitForSelector("#auth-code", { state: "detached", timeout: 20000 });
    await page.waitForTimeout(1500);
    await shot(page, `${tag}-07-verify-success`);
  } catch (e) {
    console.log("verify-success skipped:", e.message.split("\n")[0]);
  }

  // Login with an unverified second account → email_unverified alert.
  try {
    const email2 = `pending.${uniq}@datawrap.io`;
    await gotoLogin(page, theme);
    await page.getByText("Create an account").first().click();
    await page.waitForTimeout(600);
    await page.locator("#signup-name").fill("Pat Pending");
    await page.locator('input[type="email"]').first().fill(email2);
    await page.locator('input[type="password"]').first().fill("correct-horse-staple-9");
    await page.waitForTimeout(4200); // form-token min fill age (3s floor + margin)
    await page.locator('button[type="submit"]').last().click();
    await page.waitForSelector("#auth-code", { timeout: 25000 });
    await gotoLogin(page, theme);
    await page.locator('input[type="email"]').first().fill(email2);
    await page.locator('input[type="password"]').first().fill("correct-horse-staple-9");
    await page.locator('button[type="submit"]').last().click();
    await page.waitForTimeout(1400);
    await shot(page, `${tag}-08-login-unverified-alert`);
  } catch (e) {
    console.log("unverified-alert skipped:", e.message.split("\n")[0]);
  }

  // Forgot flow — emailed reset link.
  await gotoLogin(page, theme);
  await page.getByText(/forgot/i).first().click();
  await page.waitForTimeout(700);
  await shot(page, `${tag}-09-forgot-empty`);
  await page.locator('input[type="email"]').first().fill(email);
  await page.waitForTimeout(4200); // form-token min fill age (3s floor + margin)
  await page.locator('button[type="submit"]').last().click();
  await page.waitForTimeout(1400);
  await shot(page, `${tag}-10-forgot-check-email`);

  // Valid reset link → password form.
  try {
    const token = mintResetToken(email);
    await page.goto(`${BASE}/reset-password#token=${token}`, { waitUntil: "networkidle" });
    const newPass = page.locator("#reset-password");
    try {
      await newPass.waitFor({ timeout: 15000 });
    } catch {
      await shot(page, `${tag}-11-reset-form-BAD`);
      throw new Error("reset form never reached status=ok");
    }
    await shot(page, `${tag}-11-reset-form`);

    // Weak password → inline error.
    if (await newPass.count()) {
      await newPass.fill("short");
      await page.locator('button[type="submit"]').last().click();
      await page.waitForTimeout(900);
      await shot(page, `${tag}-12-reset-error`);

      // Valid password → password-changed success.
      await newPass.fill("correct-horse-staple-9");
      await page.locator('button[type="submit"]').last().click();
      await page.waitForTimeout(1500);
      await shot(page, `${tag}-13-reset-success`);
    }
  } catch (e) {
    console.log("reset-form skipped:", e.message.split("\n")[0]);
  }

  // Bogus link → expired/used page.
  await page.goto(`${BASE}/reset-password#token=not-a-real-token-000`, { waitUntil: "networkidle" });
  await page.getByText("This link has expired or was already used").waitFor({ timeout: 15000 }).catch(() => {});
  await shot(page, `${tag}-14-reset-expired`);

  // ---- review-8 item 11: missing states ----

  // Submit button: hover, focus-visible, loading (spinner + label).
  await gotoLogin(page, theme);
  await page.getByText("Create an account").first().click();
  await page.waitForTimeout(600);
  const submitBtn = page.locator('button[type="submit"]').last();
  await submitBtn.hover();
  await shot(page, `${tag}-15-btn-hover`);
  await submitBtn.focus();
  await shot(page, `${tag}-16-btn-focus`);
  await page.route("**/api/v1/auth/signup", async (route) => {
    await new Promise((r) => setTimeout(r, 1800));
    try { await route.continue(); } catch { /* request already settled */ }
  });
  await page.locator("#signup-name").fill("Ada Lovelace");
  await page.locator('input[type="email"]').first().fill(`busy.${uniq}@datawrap.io`);
  await page.locator('input[type="password"]').first().fill("correct-horse-staple-9");
  await page.waitForTimeout(3300);
  await submitBtn.click();
  await page.waitForTimeout(450);
  await shot(page, `${tag}-17-btn-loading`);
  await page.unroute("**/api/v1/auth/signup");
  await page.waitForTimeout(1600);

  // Turnstile production look — MOCK container (no real sitekey locally);
  // the filename labels it as a test-sitekey mock.
  await gotoLogin(page, theme);
  await page.getByText("Create an account").first().click();
  await page.waitForTimeout(600);
  await page.evaluate(() => {
    const form = document.querySelector(".lp-login-form");
    if (form && !form.querySelector("[data-shot-mock]")) {
      // Hide the real test-key widget so the shot shows the production look.
      const real = form.querySelector(".lp-login-turnstile");
      if (real) real.style.display = "none";
      const dark = document.documentElement.dataset.theme === "dark";
      const mock = document.createElement("div");
      mock.className = "lp-login-turnstile";
      mock.setAttribute("data-shot-mock", "turnstile-test-sitekey");
      mock.innerHTML = `<div style="display:flex;align-items:center;gap:10px;width:100%;height:65px;border-radius:8px;padding:0 14px;box-sizing:border-box;border:1px solid ${dark ? "#4b5563" : "#d4d4d4"};background:${dark ? "#1f2937" : "#f9f9f9"}"><span style="width:20px;height:20px;border:2px solid ${dark ? "#9ca3af" : "#6b7280"};border-radius:4px;flex:none"></span><span style="font:14px Roboto,system-ui,sans-serif;color:${dark ? "#e5e7eb" : "#333"}">Verify you are human</span><span style="margin-left:auto;font:10px Roboto,system-ui,sans-serif;color:${dark ? "#9ca3af" : "#888"};text-align:right;line-height:1.4">Cloudflare<br>Privacy · Terms</span></div>`;
      form.insertBefore(mock, form.querySelector('button[type="submit"]'));
    }
  });
  await shot(page, `${tag}-18-signup-turnstile-testkey-mock`);

  // Authenticated settings: admin metrics card + change-email flow.
  // Re-login first — the anonymous-screen navigations above wiped the
  // verify-success session.
  try {
    await gotoLogin(page, theme);
    await page.locator('input[type="email"]').first().fill(email);
    await page.locator('input[type="password"]').first().fill("correct-horse-staple-9");
    await page.locator('button[type="submit"]').last().click();
    // Wait for the app shell to mount, not a fixed sleep — a slow API
    // under load otherwise leaves the page on the login screen and the
    // Settings nav click below burns a 30s actionability timeout.
    await page.waitForSelector(
      'button[title="Settings"], .df2-mobile-menu',
      { state: "attached", timeout: 20000 }
    );
    await page.waitForTimeout(800);
  } catch (e) {
    const alert = await page.locator('[role="alert"], .lp-alert').first().textContent().catch(() => "");
    console.log(`  ${tag}: re-login failed —`, e.message.split("\n")[0], `| hash=${page.url().split("#")[1] || "/"} alert=${(alert || "").trim()}`);
  }
  await page.route("**/api/v1/auth/admin/signup-metrics", (route) =>
    route.fulfill({
      status: 200, contentType: "application/json",
      body: JSON.stringify({
        pending_signups: 3, active_challenges: 2,
        email_quota: { day_used: 41, day_limit: 500, backend: "file" },
      }),
    }));
  try {
    // The icon-only Settings button is hidden on narrow layouts — open the
    // mobile nav and pick the Settings item (sidebar is inert until then).
    // Check the hamburger FIRST: on narrow layouts the sidebar (and its
    // Settings gear) renders offscreen via transform — isVisible() is true
    // but the element is inert, and clicking it hangs on actionability.
    const menu = page.locator(".df2-mobile-menu").first();
    if (await menu.isVisible().catch(() => false)) {
      await menu.click();
      await page.locator('.df2-sidebar.open').waitFor({ timeout: 8000 });
      await page.locator('button.df2-nav-item[title="Security, team, SSO"]').click();
    } else {
      await page.locator('button[title="Settings"]').first().click();
    }
    await page.waitForTimeout(1500);
    await page.getByRole("tab", { name: /Security/ }).first().click();
    await page.waitForTimeout(1200);
    const metrics = page.locator('[data-testid="signup-metrics-card"]');
    if (await metrics.count()) {
      await metrics.scrollIntoViewIfNeeded();
      await page.waitForTimeout(300);
      await shot(page, `${tag}-19-admin-metrics`);
    } else {
      console.log(`  ${tag}: metrics card not rendered`);
    }
    const card = page.locator('[data-testid="change-email-card"]');
    if (await card.count()) {
      await card.scrollIntoViewIfNeeded();
      await page.waitForTimeout(300);
      await shot(page, `${tag}-20-change-email-idle`);
      await card.locator('button').first().click();
      await page.waitForTimeout(400);
      const input = page.locator("#change-email-input");
      if (await input.count()) {
        await input.fill(`new.${uniq}@datawrap.io`);
        await shot(page, `${tag}-21-change-email-request`);
        // Rate-limit/quota message via a stubbed 429.
        await page.route("**/api/v1/auth/email/change", (route) =>
          route.fulfill({
            status: 429, contentType: "application/json",
            body: JSON.stringify({ error: { code: "rate_limited", retry_after_sec: 120, message: "Too many requests — retry in 2 min." } }),
          }));
        await card.locator('button[type="submit"]').first().click();
        await page.waitForTimeout(800);
        await shot(page, `${tag}-22-change-email-rate-limited`);
        await page.unroute("**/api/v1/auth/email/change");
      }
    } else {
      console.log(`  ${tag}: change-email card not rendered`);
    }
  } catch (e) {
    console.log(`  ${tag}: settings states skipped —`, e.message.split("\n")[0]);
  }

  await ctx.close();
}

const vps = VP === "all" ? Object.keys(VIEWPORTS) : [VP];
const themes = THEME === "both" ? ["light", "dark"] : [THEME];
for (const v of vps) {
  for (const t of themes) await run(v, VIEWPORTS[v], t);
}
await browser.close();
console.log("done");
