/**
 * Parity charts are drawn to scale: these pin the maths the SVG components use.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  areaPath,
  formatPercentTick,
  linearScale,
  linePath,
  lineSegments,
  minMaxTicks,
  ordinalPositions,
  percentDomain,
  percentFraction,
  splitWidths,
} from "./chartScale.js";

describe("linearScale", () => {
  it("maps the domain ends to the range ends and is linear between", () => {
    const y = linearScale([99, 100], [150, 20]);
    assert.equal(y(99), 150);
    assert.equal(y(100), 20);
    assert.equal(y(99.5), 85);
  });

  it("maps every value to the middle of the range when the domain has no width", () => {
    const y = linearScale([5, 5], [0, 100]);
    assert.equal(y(5), 50);
    assert.equal(y(999), 50);
  });
});

describe("percentDomain", () => {
  it("spans the lowest to highest known value and ignores unknowns", () => {
    assert.deepEqual(percentDomain([99.5, null, 100, 99.8, undefined, Number.NaN]), [99.5, 100]);
  });

  it("gives a flat series at 100% room below it instead of a zero-width scale", () => {
    assert.deepEqual(percentDomain([100, 100, 100]), [99.5, 100]);
    assert.deepEqual(percentDomain([100], 2), [98, 100]);
  });

  it("never leaves 0..100", () => {
    assert.deepEqual(percentDomain([0, 0]), [0, 0.5]);
    assert.deepEqual(percentDomain([120, -5]), [0, 100]);
  });

  it("is null when nothing is known", () => {
    assert.equal(percentDomain([]), null);
    assert.equal(percentDomain([null, null]), null);
  });
});

describe("ticks", () => {
  it("labels the series minimum and maximum where they sit on the scale", () => {
    const ticks = minMaxTicks([99.5, 100], [100, 99.5, 99.9]);
    assert.deepEqual(ticks.map((t) => [t.kind, t.value, t.label]), [
      ["max", 100, "100%"],
      ["min", 99.5, "99.5%"],
    ]);
  });

  it("labels the floor too when the series is flat", () => {
    const ticks = minMaxTicks([99.5, 100], [100, 100]);
    assert.deepEqual(ticks.map((t) => t.label), ["100%", "99.5%"]);
    assert.equal(ticks[1].kind, "floor");
  });

  it("never rounds a tick up into a claim the data does not make", () => {
    assert.equal(formatPercentTick(99.996), "99.99%");
    assert.equal(formatPercentTick(99.97), "99.97%");
    assert.equal(formatPercentTick(99), "99%");
    assert.equal(formatPercentTick(100), "100%");
  });
});

describe("ordinalPositions", () => {
  it("spaces runs evenly across the range", () => {
    assert.deepEqual(ordinalPositions(3, [40, 340]), [40, 190, 340]);
  });
  it("centres a single run and returns nothing for none", () => {
    assert.deepEqual(ordinalPositions(1, [40, 340]), [190]);
    assert.deepEqual(ordinalPositions(0, [40, 340]), []);
  });
});

describe("line and area paths", () => {
  const points = [
    { x: 0, y: 10 },
    { x: 10, y: 20 },
    { x: 20, y: null },
    { x: 30, y: 5 },
    { x: 40, y: 15 },
  ];

  it("breaks the line at an unknown value instead of inventing one", () => {
    assert.equal(lineSegments(points).length, 2);
    assert.equal(linePath(points), "M0,10 L10,20 M30,5 L40,15");
  });

  it("closes each segment's area down to the baseline", () => {
    assert.equal(areaPath(points, 100), "M0,10 L10,20 L10,100 L0,100 Z M30,5 L40,15 L40,100 L30,100 Z");
  });

  it("draws no area for a lone point", () => {
    assert.equal(areaPath([{ x: 5, y: 5 }], 50), "");
    assert.equal(linePath([{ x: 5, y: 5 }]), "M5,5");
  });
});

describe("splitWidths (explained | unexplained)", () => {
  it("is proportional with a gap between the parts", () => {
    const w = splitWidths(8, 4, 302, 2);
    assert.equal(w.gap, 2);
    assert.equal(Math.round(w.first), 200);
    assert.equal(Math.round(w.second), 100);
    assert.equal(w.first + w.gap + w.second, 302);
  });

  it("keeps one unexplained row in a million visible", () => {
    const w = splitWidths(1_000_000, 1, 320, 2, 4);
    assert.equal(w.second, 4);
    assert.equal(w.first + w.gap + w.second, 320);
  });

  it("fills the whole bar with one part when the other is zero", () => {
    assert.deepEqual(splitWidths(0, 3, 300), { first: 0, second: 300, gap: 0 });
    assert.deepEqual(splitWidths(3, 0, 300), { first: 300, second: 0, gap: 0 });
    assert.deepEqual(splitWidths(0, 0, 300), { first: 0, second: 0, gap: 0 });
  });
});

describe("percentFraction", () => {
  it("is to scale, and unknown is empty rather than full", () => {
    assert.equal(percentFraction(99.6), 0.996);
    assert.equal(percentFraction(null), 0);
    assert.equal(percentFraction(140), 1);
  });
});
