import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { dndKeyAction, dndReduce, IDLE, type DndState, type DndTarget } from "./mappingDnd";
import { virtualRange } from "../components/mapping/useVirtualWindow";

const TARGETS: DndTarget[] = [
  { name: "legacy_ref", describe: "BIGINT, PK, required, unmapped" },
  { name: "given_name", describe: "VARCHAR(40), unmapped" },
  { name: "surname", describe: "VARCHAR(60), unmapped" },
];

function run(keys: string[], source = "order_id") {
  let state: DndState = IDLE;
  const said: string[] = [];
  let drop: { source: string; target: string } | undefined;
  let r = dndReduce(state, { type: "pickUp", source, via: "keyboard" }, TARGETS);
  state = r.state;
  if (r.announce) said.push(r.announce);
  for (const key of keys) {
    const action = dndKeyAction(key);
    assert.ok(action, key);
    r = dndReduce(state, action, TARGETS);
    state = r.state;
    if (r.announce) said.push(r.announce);
    if (r.drop) drop = r.drop;
  }
  return { state, said, drop };
}

describe("keyboard drag and drop", () => {
  it("Space/Enter picks up, arrows move, Enter drops — every step announced", () => {
    const { state, said, drop } = run(["ArrowDown", "ArrowDown", "Enter"]);
    assert.deepEqual(drop, { source: "order_id", target: "surname" });
    assert.equal(state.mode, "idle");
    assert.match(said[0], /^Picked up order_id\. .*Enter drops, Escape cancels\. Over legacy_ref, BIGINT, PK, required, unmapped\. 1 of 3\./);
    assert.match(said[1], /Over given_name.*2 of 3/);
    assert.match(said[3], /Dropped order_id on surname/);
  });

  it("Escape cancels without a drop", () => {
    const { drop, said, state } = run(["ArrowDown", "Escape"]);
    assert.equal(drop, undefined);
    assert.equal(state.mode, "idle");
    assert.match(said.at(-1)!, /Cancelled\. order_id is unchanged/);
  });

  it("arrows clamp at the ends; Home/End jump", () => {
    const { drop } = run(["ArrowUp", "End", "ArrowDown", "Home", "ArrowDown", " "]);
    assert.deepEqual(drop, { source: "order_id", target: "given_name" });
  });

  it("nothing to drop on is said out loud, not a silent no-op", () => {
    const r = dndReduce(IDLE, { type: "pickUp", source: "x", via: "keyboard" }, []);
    assert.equal(r.state.mode, "idle");
    assert.match(r.announce!, /No destination columns/);
  });

  it("other keys are not drag keys", () => {
    assert.equal(dndKeyAction("a"), null);
    assert.equal(dndKeyAction("Tab"), null);
  });
});

describe("windowed lists", () => {
  it("renders only the rows in view plus overscan", () => {
    assert.deepEqual(virtualRange(1000, 48, 0, 288, 6), { start: 0, end: 12 });
    assert.deepEqual(virtualRange(1000, 48, 48 * 500, 288, 6), { start: 494, end: 512 });
    assert.deepEqual(virtualRange(1000, 48, 48 * 2000, 288, 6), { start: 993, end: 1000 });
    assert.deepEqual(virtualRange(0, 48, 0, 288), { start: 0, end: 0 });
  });
});
