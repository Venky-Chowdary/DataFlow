import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { JSDOM } from "jsdom";

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
Object.assign(globalThis, { window: dom.window, document: dom.window.document, HTMLElement: dom.window.HTMLElement });

const { edgeScroll } = await import("./useTouchDrag.js");

const rect = (top: number, height: number) =>
  ({ top, bottom: top + height, height, left: 0, right: 300, width: 300, x: 0, y: top, toJSON() {} }) as DOMRect;

/** A 140px panel list (top 360) holding a row at the given top/height. */
function panel(rowTop: number, rowHeight = 70) {
  document.body.innerHTML = '<ul class="df2-ms-panel-list"><li data-dest-column="email_address"></li></ul>';
  const list = document.querySelector<HTMLElement>(".df2-ms-panel-list")!;
  const row = list.querySelector<HTMLElement>("li")!;
  Object.defineProperty(list, "scrollHeight", { value: 600, configurable: true });
  Object.defineProperty(list, "clientHeight", { value: 140, configurable: true });
  list.getBoundingClientRect = () => rect(360, 140);
  row.getBoundingClientRect = () => rect(rowTop - list.scrollTop, rowHeight);
  return { list, row };
}

describe("touch drag edge scrolling", () => {
  it("a finger resting on a fully visible row at the edge drops there; nothing scrolls", () => {
    const { list, row } = panel(420);
    edgeScroll(480, row);
    assert.equal(list.scrollTop, 0);
  });

  it("a clipped row is revealed, and only by the part that was hidden", () => {
    const { list, row } = panel(440); // bottom 510, list bottom 500: 10px hidden
    edgeScroll(490, row);
    assert.equal(list.scrollTop, 10);
    edgeScroll(490, row);
    assert.equal(list.scrollTop, 10, "once visible it stays put under the finger");
  });

  it("over the edge with no row under the finger, the list scrolls a step", () => {
    const { list } = panel(200);
    edgeScroll(495, null);
    assert.equal(list.scrollTop, 24);
  });

  it("a drag that starts in the edge zone does not scroll until the finger has left it", () => {
    const { list } = panel(200);
    const armed = new Set<string>();
    edgeScroll(495, null, armed);
    assert.equal(list.scrollTop, 0, "picked up at the edge: no jump");
    edgeScroll(430, null, armed); // middle of the list
    edgeScroll(495, null, armed);
    assert.equal(list.scrollTop, 24, "back at the edge after leaving it: scrolls");
  });
});
