/**
 * UX-28 on the real page: a fast submit is held (button busy) until the form
 * token is old enough, a retry uses a fresh token, and an address refusal is a
 * sentence on the email field with no "Valid" badge beside it.
 */
import assert from "node:assert/strict";
import { after, describe, it } from "node:test";
import { JSDOM } from "jsdom";

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
Object.assign(globalThis, {
  window: dom.window,
  document: dom.window.document,
  HTMLElement: dom.window.HTMLElement,
  Element: dom.window.Element,
  Node: dom.window.Node,
  MutationObserver: dom.window.MutationObserver,
  IS_REACT_ACT_ENVIRONMENT: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });

const issued: Array<{ token: string; at: number }> = [];
const signups: Array<{ token: string; at: number }> = [];
let refuseNext = true;
const realFetch = globalThis.fetch;
globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
  const url = String(input);
  const json = (status: number, body: unknown) => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
  if (url.includes("/auth/form-token")) {
    const token = `tok-${issued.length + 1}`;
    issued.push({ token, at: Date.now() });
    return json(200, { form_token: token });
  }
  if (url.endsWith("/auth/signup")) {
    const body = JSON.parse(String(init?.body ?? "{}"));
    signups.push({ token: body.form_token, at: Date.now() });
    if (refuseNext) {
      refuseNext = false;
      return json(400, { error: { code: "blocked_address", message: "this address can't be used for signup" } });
    }
    return json(202, { challenge_id: "otc_1", resend_after_sec: 60, expires_in_sec: 600 });
  }
  return json(404, {});
}) as typeof fetch;

const { act, createElement } = await import("react");
const { createRoot } = await import("react-dom/client");
const { ToastProvider } = await import("../components/Toast.js");
const { SignupPage } = await import("./SignupPage.js");

after(() => {
  globalThis.fetch = realFetch;
  dom.window.close();
});

const tick = (ms: number) => act(async () => { await new Promise((r) => setTimeout(r, ms)); });

function setValue(id: string, value: string) {
  const input = document.getElementById(id) as HTMLInputElement;
  const setter = Object.getOwnPropertyDescriptor(dom.window.HTMLInputElement.prototype, "value")!.set!;
  setter.call(input, value);
  input.dispatchEvent(new dom.window.Event("input", { bubbles: true }));
}

describe("signup submit", () => {
  it("holds a fast submit, retries with a fresh token, and words the refusal", async () => {
    const host = document.createElement("div");
    document.body.appendChild(host);
    const root = createRoot(host);
    await act(async () => root.render(createElement(ToastProvider, null,
      createElement(SignupPage, { onAuthenticated: () => {}, onSignIn: () => {}, onBack: () => {} } as never))));
    await tick(20);
    await act(async () => {
      setValue("signup-name", "Jane Doe");
      setValue("signup-email", "jane.doe@example.io");
      setValue("signup-password", "Contoso-Audit-2026!x");
    });
    const form = () => document.querySelector("form.lp-login-form") as HTMLFormElement;
    await act(async () => { form().requestSubmit(); });
    await tick(800);
    assert.equal(signups.length, 0, "a submit within the fill floor is held, not sent");
    assert.match(document.body.textContent ?? "", /Sending code/);
    await tick(3200);
    assert.equal(signups.length, 1);
    assert.ok(signups[0].at - issued[0].at >= 3000, `sent ${signups[0].at - issued[0].at} ms after the token`);

    const text = document.body.textContent ?? "";
    assert.match(text, /This address can't be used to sign up\. Use your work email\./);
    assert.doesNotMatch(text, /this address can't be used for signup/);
    assert.equal(document.querySelector(".lp-field-status--ok"), null, "no Valid badge beside a refusal");
    assert.equal(document.getElementById("signup-email")?.getAttribute("aria-invalid"), "true");

    await act(async () => setValue("signup-email", "jane.doe@contoso.io"));
    await act(async () => { form().requestSubmit(); });
    await tick(3500);
    assert.equal(signups.length, 2);
    assert.notEqual(signups[1].token, signups[0].token, "a retry must not replay a used token");
    assert.match(document.body.textContent ?? "", /Enter the 6-digit code/);
    await act(async () => root.unmount());
  });
});
