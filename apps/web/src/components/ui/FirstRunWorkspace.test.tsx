/**
 * UX-1: a fresh install has no workspace. The first sign-in asks a platform
 * admin to create one, the switcher offers "Create workspace…", and the API's
 * "X-Workspace-Id required" never reaches a person as written.
 */
import assert from "node:assert/strict";
import { after, afterEach, describe, it } from "node:test";
import { JSDOM } from "jsdom";

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
Object.assign(globalThis, {
  window: dom.window,
  document: dom.window.document,
  localStorage: dom.window.localStorage,
  HTMLElement: dom.window.HTMLElement,
  HTMLSelectElement: dom.window.HTMLSelectElement,
  Element: dom.window.Element,
  Node: dom.window.Node,
  Event: dom.window.Event,
  CustomEvent: dom.window.CustomEvent,
  KeyboardEvent: dom.window.KeyboardEvent,
  getComputedStyle: dom.window.getComputedStyle.bind(dom.window),
  IS_REACT_ACT_ENVIRONMENT: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
dom.window.requestAnimationFrame = (cb) => {
  cb(0);
  return 0;
};
dom.window.cancelAnimationFrame = () => {};

type Directory = { workspaces: { id: string; name: string }[]; platform_admin: boolean; views?: unknown[] };
let directory: Directory = { workspaces: [], platform_admin: true, views: [] };
const posts: { url: string; body: string; workspace: string | null }[] = [];
const realFetch = globalThis.fetch;
globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
  const url = String(input);
  const headers = new Headers(init?.headers);
  if (url.endsWith("/team/workspaces") && (init?.method ?? "GET") === "POST") {
    posts.push({ url, body: String(init?.body ?? ""), workspace: headers.get("X-Workspace-Id") });
    const ws = { id: "ws_new", name: JSON.parse(String(init?.body)).name };
    directory = { ...directory, workspaces: [ws] };
    return new Response(JSON.stringify({ workspace: ws }), { status: 200, headers: { "Content-Type": "application/json" } });
  }
  if (url.endsWith("/team/workspaces")) {
    return new Response(JSON.stringify({ ...directory, default_workspace_id: directory.workspaces[0]?.id ?? "__all__" }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }
  return new Response("{}", { status: 404 });
}) as typeof fetch;

const { act, createElement } = await import("react");
const { createRoot } = await import("react-dom/client");
const { FirstRunWorkspace } = await import("./FirstRunWorkspace.js");
const { WorkspaceSwitcher } = await import("./WorkspaceSwitcher.js");
const { parseApiError } = await import("../../lib/api.js");
const { getActiveWorkspaceId } = await import("../../lib/workspace.js");
const { firstRunState, workspaceRefusalCopy, workspaceNameProblem, FIRST_RUN_COPY, WORKSPACE_CREATE_REQUEST_EVENT } = await import(
  "../../lib/firstRunWorkspace.js"
);

let root: ReturnType<typeof createRoot> | undefined;
async function render(node: ReturnType<typeof createElement>) {
  if (!root) {
    const host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
  }
  await act(async () => root!.render(node));
  await act(async () => new Promise((r) => setTimeout(r, 0)));
}

afterEach(async () => {
  await act(async () => root?.unmount());
  root = undefined;
  document.body.replaceChildren();
  localStorage.clear();
  posts.length = 0;
  directory = { workspaces: [], platform_admin: true, views: [] };
});
after(() => {
  globalThis.fetch = realFetch;
  dom.window.close();
});

describe("first-run workspace state", () => {
  it("asks an admin to create one, a member to ask an admin, and nobody once one exists", () => {
    assert.equal(firstRunState(null), "unknown");
    assert.equal(firstRunState({ workspaces: [], platformAdmin: true }), "create");
    assert.equal(firstRunState({ workspaces: [], platformAdmin: false }), "ask-admin");
    assert.equal(firstRunState({ workspaces: [{}], platformAdmin: true }), "ready");
  });

  it("validates the name the way the API does", () => {
    assert.match(workspaceNameProblem("  "), /Enter a name/);
    assert.match(workspaceNameProblem("x".repeat(129)), /128/);
    assert.equal(workspaceNameProblem("Contoso"), "");
  });
});

describe("the header refusal never reaches a person as written", () => {
  it("rewrites X-Workspace-Id required into a next step", async () => {
    const res = new Response(JSON.stringify({ detail: "X-Workspace-Id required" }), { status: 400 });
    const text = await parseApiError(res, "Failed to save connector");
    assert.doesNotMatch(text, /X-Workspace-Id/);
    assert.match(text, /Choose a workspace first/);
    assert.match(text, /create one/);
  });

  it("rewrites No workspace membership", () => {
    assert.match(workspaceRefusalCopy("No workspace membership"), /not a member of any workspace/);
    assert.equal(workspaceRefusalCopy("Something else"), "");
  });
});

describe("first sign-in with no workspace", () => {
  it("opens the create dialog, creates the workspace and makes it active", async () => {
    await render(createElement(FirstRunWorkspace));
    const dialog = document.querySelector('[role="dialog"]');
    assert.ok(dialog, "the create dialog opens on first sign-in");
    assert.match(dialog!.textContent ?? "", new RegExp(FIRST_RUN_COPY.createTitle));
    const input = dialog!.querySelector("input") as HTMLInputElement;
    const labelled = dialog!.querySelector(`label[for="${input.id}"]`);
    assert.equal(labelled?.textContent, FIRST_RUN_COPY.createLabel);

    const setter = Object.getOwnPropertyDescriptor(dom.window.HTMLInputElement.prototype, "value")!.set!;
    await act(async () => {
      setter.call(input, "Contoso Data Platform");
      input.dispatchEvent(new dom.window.Event("input", { bubbles: true }));
    });
    const create = [...dialog!.querySelectorAll("button")].find((b) => b.textContent?.includes(FIRST_RUN_COPY.createAction));
    await act(async () => {
      create!.click();
    });
    await act(async () => new Promise((r) => setTimeout(r, 0)));
    assert.equal(posts.length, 1);
    assert.deepEqual(JSON.parse(posts[0].body), { name: "Contoso Data Platform" });
    assert.equal(getActiveWorkspaceId(), "ws_new");
    assert.equal(document.querySelector('[role="dialog"]'), null);
    assert.equal(document.querySelector('[data-testid="first-run-banner"]'), null);
  });

  it("leaves a banner with the same action when the dialog is dismissed", async () => {
    await render(createElement(FirstRunWorkspace));
    const notNow = [...document.querySelectorAll('[role="dialog"] button')].find((b) => b.textContent === "Not now");
    await act(async () => {
      (notNow as HTMLButtonElement).click();
    });
    const banner = document.querySelector('[data-testid="first-run-banner"]');
    assert.ok(banner, "a banner keeps the first-run action in view");
    assert.match(banner!.textContent ?? "", /No workspace yet/);
  });

  it("stays out of the dev UI preview, which is not an account", async () => {
    localStorage.setItem("df2.session", JSON.stringify({ email: "preview@local", name: "", role: "admin", token: "dev-ui-preview", expires_at: 0, signed_in_at: 0 }));
    await render(createElement(FirstRunWorkspace));
    assert.equal(document.querySelector('[role="dialog"]'), null);
    assert.equal(document.querySelector('[data-testid="first-run-banner"]'), null);
  });

  it("tells a non-admin member who can help instead of offering a create they cannot do", async () => {
    directory = { workspaces: [], platform_admin: false, views: [] };
    await render(createElement(FirstRunWorkspace));
    assert.equal(document.querySelector('[role="dialog"]'), null);
    assert.match(document.querySelector('[data-testid="first-run-ask-admin"]')?.textContent ?? "", /Ask a Datawrap administrator/);
  });
});

describe("workspace switcher", () => {
  it("offers Create workspace… to a platform admin and opens the dialog instead of switching", async () => {
    directory = {
      workspaces: [],
      platform_admin: true,
      views: [{ id: "__all__", name: "All workspaces", kind: "all" }],
    };
    let requested = 0;
    const onRequest = () => {
      requested += 1;
    };
    window.addEventListener(WORKSPACE_CREATE_REQUEST_EVENT, onRequest);
    await render(createElement(WorkspaceSwitcher, { id: "ws-switch" }));
    const select = document.getElementById("ws-switch") as HTMLSelectElement;
    assert.ok(select, "a platform admin always gets the switcher, even with one view");
    const create = [...select.options].find((o) => o.textContent === "Create workspace…");
    assert.ok(create, "the switcher offers Create workspace…");
    const before = getActiveWorkspaceId();
    await act(async () => {
      select.value = create!.value;
      select.dispatchEvent(new dom.window.Event("change", { bubbles: true }));
    });
    window.removeEventListener(WORKSPACE_CREATE_REQUEST_EVENT, onRequest);
    assert.equal(requested, 1);
    assert.equal(getActiveWorkspaceId(), before, "choosing Create does not switch workspace");
  });

  it("does not offer Create workspace… to a member", async () => {
    directory = {
      workspaces: [
        { id: "a", name: "A" },
        { id: "b", name: "B" },
      ],
      platform_admin: false,
      views: [],
    };
    await render(createElement(WorkspaceSwitcher, { id: "ws-switch" }));
    const select = document.getElementById("ws-switch") as HTMLSelectElement;
    assert.ok(![...select.options].some((o) => o.textContent === "Create workspace…"));
  });
});
