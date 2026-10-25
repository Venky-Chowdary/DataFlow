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

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

const args = {};
for (let i = 2; i < process.argv.length; i += 2) args[process.argv[i].replace(/^--/, "")] = process.argv[i + 1];
const SHA = args.sha || execSync("git rev-parse --short=8 HEAD", { cwd: repoRoot }).toString().trim();
const BASE = args.base || "http://127.0.0.1:5173";
const OUT = path.resolve(args.out || path.join(repoRoot, "docs/signup-screenshots", SHA));
const THEME = args.theme || "both";
const VP = args.viewport || "all";
const CDP = args.cdp || "http://localhost:29229";

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
    window.localStorage.removeItem("df2.session");
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
from services.metadata_backend import json_doc_transaction
from services import otp_service
p = data_dir() / "otp_challenges.json"
with json_doc_transaction(p, {"otp_challenges": []}) as data:
    rows = [r for r in data["otp_challenges"]
            if r.get("purpose") == "signup" and not r.get("consumed_at")]
    rows.sort(key=lambda r: r.get("created_at", ""))
    row = rows[-1]
    row["code_hmac"] = otp_service.code_hmac(row["id"], "signup", "${KNOWN_CODE}")
    print("patched", row["id"])
`);
}

// Issue a real reset-link token for a verified account via the real store.
function mintResetToken(email) {
  const out = py(`
import sys; sys.path.insert(0, ".")
from services import password_reset_store
token, _doc = password_reset_store.issue(email="${email}", ip="127.0.0.1", ua="screenshot-harness")
print(token)
`);
  // The Mongo-fallback warning line precedes the token on stdout.
  return out.split("\n").pop().trim();
}

async function run(vpName, viewport, theme) {
  const ctx = await browser.newContext({ viewport, deviceScaleFactor: 1 });
  const page = await ctx.newPage();
  const tag = `${vpName}-${theme}`;
  const uniq = `${vpName}.${theme}`;
  const email = `verify.${uniq}@datawrap.io`;

  await gotoLogin(page, theme);
  await shot(page, `${tag}-01-login`);

  // Signup — empty state.
  await page.getByText("Create an account").first().click();
  await page.waitForTimeout(700);
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

  // Real code → verify success hand-off.
  try {
    plantOtpCode();
    await page.locator("#auth-code").fill(KNOWN_CODE);
    await page.locator('button[type="submit"]').last().click();
    await page.waitForTimeout(2500);
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
    await page.waitForTimeout(3200); // form-token min fill age
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
  await page.waitForTimeout(3200); // form-token min fill age
  await page.locator('button[type="submit"]').last().click();
  await page.waitForTimeout(1400);
  await shot(page, `${tag}-10-forgot-check-email`);

  // Valid reset link → password form.
  try {
    const token = mintResetToken(email);
    await page.goto(`${BASE}/reset-password?token=${token}`, { waitUntil: "networkidle" });
    await page.waitForTimeout(1200);
    await shot(page, `${tag}-11-reset-form`);

    // Weak password → inline error.
    const newPass = page.locator('input[autocomplete="new-password"]').first();
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
  await page.goto(`${BASE}/reset-password?token=not-a-real-token-000`, { waitUntil: "networkidle" });
  await page.waitForTimeout(1200);
  await shot(page, `${tag}-14-reset-expired`);

  await ctx.close();
}

const vps = VP === "all" ? Object.keys(VIEWPORTS) : [VP];
const themes = THEME === "both" ? ["light", "dark"] : [THEME];
for (const v of vps) {
  for (const t of themes) await run(v, VIEWPORTS[v], t);
}
await browser.close();
console.log("done");
