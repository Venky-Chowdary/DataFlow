/**
 * UX-27: after a refused password and a good one, "Sign-in failed" stayed on
 * screen next to "Signed in" for 16 s. A successful sign-in shows one toast and
 * clears the toasts of the failed attempts.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
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

const { act, createElement, useEffect } = await import("react");
const { createRoot } = await import("react-dom/client");
const { ToastProvider, useToast } = await import("../components/Toast.js");
const { AUTH_ATTEMPT_TOAST_TITLES, clearAuthAttemptToasts } = await import("../lib/authToasts.js");

const src = join(dirname(fileURLToPath(import.meta.url)), "..");
const read = (rel: string) => readFileSync(join(src, rel), "utf8");

after(() => dom.window.close());

const shown = () => [...document.querySelectorAll(".dt-toast")].map((el) => el.textContent ?? "");

describe("toasts around sign-in", () => {
  it("a sign-in after failed attempts leaves exactly one toast", async () => {
    let api: ReturnType<typeof useToast> | null = null;
    function Probe() {
      const value = useToast();
      useEffect(() => { api = value; }, [value]);
      return null;
    }
    const host = document.createElement("div");
    document.body.appendChild(host);
    const root = createRoot(host);
    await act(async () => root.render(createElement(ToastProvider, null, createElement(Probe))));
    await act(async () => {
      api!.toast({ title: "Session expired", message: "Sign in again to load connectors, jobs, and transfers.", tone: "warning" });
      api!.toast({ title: "Sign-in failed", message: "Invalid email or password.", tone: "error" });
      api!.toast({ title: "Sign-in failed", message: "Check your credentials and try again.", tone: "error" });
    });
    assert.ok(shown().length >= 2);
    await act(async () => {
      clearAuthAttemptToasts(api!.dismissMatching);
      api!.toast({ title: "Signed in", message: "Opening Transfer Studio.", tone: "success" });
    });
    const after = shown();
    assert.equal(after.length, 1, JSON.stringify(after));
    assert.match(after[0], /Signed in/);
    await act(async () => root.unmount());
  });

  it("every way in clears the attempt toasts before its success toast", () => {
    const login = read("pages/LoginPage.tsx");
    const finish = login.slice(login.indexOf("const finishSignIn"), login.indexOf("const startSso"));
    assert.ok(finish.indexOf("clearAuthAttemptToasts(dismissMatching)") > -1);
    assert.ok(finish.indexOf("clearAuthAttemptToasts(dismissMatching)") < finish.indexOf('title: "Signed in"'));
    const signup = read("pages/SignupPage.tsx");
    assert.ok(signup.indexOf("clearAuthAttemptToasts(dismissMatching)") > -1);
    assert.ok(signup.indexOf("clearAuthAttemptToasts(dismissMatching)") < signup.indexOf('title: "Email verified"'));
  });

  it("each failure or retry toast on the auth screens is one that sign-in clears", () => {
    const titles = new Set<string>(AUTH_ATTEMPT_TOAST_TITLES);
    const sources = [
      read("pages/LoginPage.tsx").split("const finishSignIn")[0],
      read("components/AuthFlow.tsx"),
      read("pages/SignupPage.tsx").split("export function SignupPage")[1].split("const submitCode")[0],
    ].join("\n");
    const raised = [...sources.matchAll(/toast\(\{\s*title: "([^"]+)",[^}]*?tone: "(?:error|warning|info)"/g)].map((m) => m[1]);
    const conditional = [...sources.matchAll(/title: [^"\n]*\? "([^"]+)" : "([^"]+)"/g)].flatMap((m) => [m[1], m[2]]);
    for (const title of [...raised, ...conditional]) {
      assert.ok(titles.has(title), `"${title}" is raised before sign-in but not cleared by it`);
    }
    for (const title of ["Sign-in failed", "Complete the form", "API offline", "Resend unavailable", "Couldn't start signup", "New code sent"]) {
      assert.ok([...raised, ...conditional].includes(title), `scan missed ${title}`);
    }
    assert.ok(raised.length + conditional.length >= 5);
  });
});
