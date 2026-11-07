/** QA D7 review 3: a small-sample card advisory is shown, never as a hold. */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { ComplianceAdvisories } from "./ComplianceAdvisories.js";

describe("ComplianceAdvisories", () => {
  it("shows a possible-card note with its column and says it is not blocking", () => {
    const html = renderToStaticMarkup(
      <ComplianceAdvisories
        advisories={[{ column: "value", category: "possible_pci_pan", card_values: 3, sampled_values: 3,
          note: "Small sample: 3 of 3 sampled value(s) in 'value' are card numbers" }]}
      />,
    );
    assert.match(html, /role="note"/);
    assert.match(html, /not blocking/);
    assert.match(html, /<code>value<\/code>/);
    assert.match(html, /possible card numbers/);
  });

  it("renders nothing without advisories", () => {
    assert.equal(renderToStaticMarkup(<ComplianceAdvisories advisories={[]} />), "");
    assert.equal(renderToStaticMarkup(<ComplianceAdvisories />), "");
  });
});
