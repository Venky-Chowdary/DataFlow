import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { EmbeddingUsageSummary } from "./EmbeddingUsageSummary.js";

describe("EmbeddingUsageSummary", () => {
  it("shows token and call counts with explicitly estimated cost", () => {
    const markup = renderToStaticMarkup(
      createElement(EmbeddingUsageSummary, {
        destinationSummary: {
          embedding_usage: {
            input_tokens: 1234,
            calls: 3,
            estimated_cost_usd: 0.0123,
            token_count_estimated: true,
          },
        },
      }),
    );
    assert.match(markup, /Embedding usage/);
    assert.match(markup, /1,234 \(estimate\)/);
    assert.match(markup, /Provider calls/);
    assert.match(markup, /3/);
    assert.match(markup, /Estimated cost/);
    assert.match(markup, /estimate/);
  });

  it("omits the summary when usage is absent", () => {
    assert.equal(
      EmbeddingUsageSummary({ destinationSummary: {} }),
      null,
    );
  });
});
