import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, it } from "node:test";
import { focusFirstInvalid, PasswordToggle, ResendRow } from "../components/AuthFlow.js";
import { ChangeEmailCard } from "../components/ChangeEmailCard.js";
import { CodeStep, ForgotPasswordPage, ResetPasswordPage } from "./SignupPage.js";
import { inspectResetToken, resetPassword } from "../lib/signupApi.js";

describe("ResendRow", () => {
  it("renders the cooldown state muted, aria-disabled, and announces via aria-live", () => {
    const markup = renderToStaticMarkup(
      createElement(ResendRow, { challengeId: "otc_x", initialCooldown: 42 }),
    );
    assert.match(markup, /Resend in 42s/);
    assert.match(markup, /aria-disabled="true"/);
    assert.match(markup, /disabled=""/);
    assert.match(markup, /aria-live="polite"/);
    // one live countdown only — no duplicate sentence under the link
    assert.doesNotMatch(markup, /request a new code in/);
  });

  it("renders an active resend link once the cooldown is zero", () => {
    const markup = renderToStaticMarkup(
      createElement(ResendRow, { challengeId: "otc_x", initialCooldown: 0 }),
    );
    assert.match(markup, /Resend code/);
    assert.doesNotMatch(markup, /disabled=""/);
  });
});

describe("focusFirstInvalid", () => {
  it("focuses the first aria-invalid control inside the card", () => {
    const focused: string[] = [];
    const fakeField = { focus: () => focused.push("focused") };
    const root = {
      querySelector: (selector: string) => {
        assert.match(selector, /aria-invalid/);
        return fakeField;
      },
    };
    focusFirstInvalid(root as unknown as HTMLElement);
    assert.deepEqual(focused, ["focused"]);
  });

  it("does nothing on a card with no invalid fields", () => {
    const root = { querySelector: () => null };
    assert.doesNotThrow(() =>
      focusFirstInvalid(root as unknown as HTMLElement));
  });

  it("does nothing without a root", () => {
    assert.doesNotThrow(() => focusFirstInvalid(null));
  });
});

describe("ChangeEmailCard", () => {
  it("renders the request step with a labeled field and df2 classes", () => {
    const markup = renderToStaticMarkup(
      createElement(ChangeEmailCard, { enabled: true }),
    );
    assert.match(markup, /df2-settings/);
    assert.match(markup, /New sign-in email|new-email|email/i);
    assert.match(markup, /df2-btn/);
  });

  it("names every input programmatically (aria-labelledby resolves)", () => {
    const requestMarkup = renderToStaticMarkup(
      createElement(ChangeEmailCard, { enabled: true, initialStep: "request" }),
    );
    // The input is labelled by the visible heading — no label-less field.
    assert.match(requestMarkup, /id="change-email-input-label"/);
    assert.match(requestMarkup, /aria-labelledby="change-email-input-label"/);
    const codeMarkup = renderToStaticMarkup(
      createElement(ChangeEmailCard, { enabled: true, initialStep: "code" }),
    );
    assert.match(codeMarkup, /id="change-email-code-label"/);
    assert.match(codeMarkup, /aria-labelledby="change-email-code-label"/);
  });
});

describe("ForgotPasswordPage (reset link)", () => {
  it("asks only for the account email and explains the 30-minute link", () => {
    const markup = renderToStaticMarkup(
      createElement(ForgotPasswordPage, { onSignIn: () => {}, onBack: () => {} }),
    );
    assert.match(markup, /type="email"/);
    assert.match(markup, /Send reset link|reset link/i);
    assert.match(markup, /lp-btn/);
    // No code entry — the flow is an emailed single-use link, not an OTP.
    assert.doesNotMatch(markup, /one-time-code|6-digit/i);
  });
});

describe("ResetPasswordPage", () => {
  it("starts in the checking state while the token is inspected", () => {
    const markup = renderToStaticMarkup(
      createElement(ResetPasswordPage, {
        token: "tok123",
        onSignIn: () => {},
        onSendNewLink: () => {},
      }),
    );
    assert.match(markup, /Checking your link/i);
  });
});

describe("reset link API helpers", () => {
  const savedFetch = globalThis.fetch;

  function withMockFetch(status: number, body: unknown, calls: string[]) {
    globalThis.fetch = (async (url: unknown, init?: RequestInit) => {
      calls.push(String(url));
      return new Response(JSON.stringify(body), {
        status,
        headers: { "Content-Type": "application/json" },
      });
    }) as typeof fetch;
    return () => { globalThis.fetch = savedFetch; };
  }

  it("inspectResetToken sends the token in the POST body, never the URL", async () => {
    const calls: string[] = [];
    const restore = withMockFetch(200, { ok: true, status: "ok" }, calls);
    try {
      const r = await inspectResetToken("SECRET-TOKEN-VALUE");
      assert.equal(r.ok, true);
      assert.equal(r.status, "ok");
      assert.equal(calls.length, 1);
      assert.match(calls[0], /\/auth\/password\/reset\/inspect$/);
      // Token must travel in the body so it cannot leak via access logs.
      assert.doesNotMatch(calls[0], /SECRET-TOKEN-VALUE/);
    } finally {
      restore();
    }
  });

  it("resetPassword posts token + new_password to /auth/password/reset", async () => {
    const calls: string[] = [];
    const restore = withMockFetch(200, { ok: true }, calls);
    try {
      await resetPassword({ token: "SECRET-TOKEN-VALUE", newPassword: "x".repeat(14) });
      assert.equal(calls.length, 1);
      assert.match(calls[0], /\/auth\/password\/reset$/);
      assert.doesNotMatch(calls[0], /SECRET-TOKEN-VALUE/);
    } finally {
      restore();
    }
  });
});

/**
 * Review-8 item 7: show/hide uses a real eye/eye-off icon with aria-pressed
 * and a 44px hit target — not the old bracket/expand glyph.
 */
describe("PasswordToggle", () => {
  it("renders the eye icon with aria-pressed and a labelled state", () => {
    const off = renderToStaticMarkup(
      createElement(PasswordToggle, { show: false, onToggle: () => {} }),
    );
    assert.match(off, /lp-input-password-toggle/);
    assert.match(off, /aria-label="Show password"/);
    assert.match(off, /aria-pressed="false"/);
    const on = renderToStaticMarkup(
      createElement(PasswordToggle, { show: true, onToggle: () => {} }),
    );
    assert.match(on, /aria-label="Hide password"/);
    assert.match(on, /aria-pressed="true"/);
  });
});

/** D323: the code screen after signup is honest for both cases without
 *  saying which one applies (enumeration-safe): a new address got a code; an
 *  existing account got a sign-in reminder, and the screen offers sign-in. */
describe("CodeStep copy", () => {
  it("covers an existing account and offers sign in and a different email", () => {
    const markup = renderToStaticMarkup(createElement(CodeStep, {
      email: "ada@example.com", challengeId: "otc_x", resendAfterSec: 60,
      onSubmitCode: () => {}, busy: false, error: "", errorIsField: false,
      accountMayExist: true, onSignIn: () => {}, onChangeEmail: () => {},
    }));
    assert.match(markup, /If <strong>ada@example.com<\/strong> is new to Datawrap, we sent it a 6-digit code/);
    assert.match(markup, /If it already has a Datawrap account, we sent a sign-in reminder instead of a code/);
    assert.match(markup, /<span>Sign in<\/span>/);
    assert.match(markup, /<span>Use a different email<\/span>/);
    assert.doesNotMatch(markup, /We sent a verification code to/);
  });

  it("keeps the plain promise when the address is a known pending signup", () => {
    const markup = renderToStaticMarkup(createElement(CodeStep, {
      email: "ada@example.com", challengeId: "otc_x", resendAfterSec: 60,
      onSubmitCode: () => {}, busy: false, error: "", errorIsField: false,
      accountMayExist: false, onSignIn: () => {}, onChangeEmail: () => {},
    }));
    assert.match(markup, /We sent a 6-digit code to <strong>ada@example.com<\/strong>/);
    assert.doesNotMatch(markup, /sign-in reminder/);
  });
});
