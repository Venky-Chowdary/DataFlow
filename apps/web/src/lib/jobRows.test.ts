import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { formatRowsProcessed, rowsProcessedUnknown } from "./jobRows.js";

describe("rows a cancelled job kept", () => {
  it("treats a null count as unknown, never zero", () => {
    assert.equal(rowsProcessedUnknown({ records_processed: null }), true);
    assert.equal(
      formatRowsProcessed({ records_processed: 0, records_processed_unknown: true }),
      "an unknown number of",
    );
  });

  it("keeps a real zero and real counts", () => {
    assert.equal(rowsProcessedUnknown({ records_processed: 0 }), false);
    assert.equal(rowsProcessedUnknown({}), false);
    assert.equal(formatRowsProcessed({ records_processed: 0 }), "0");
    assert.equal(formatRowsProcessed({ records_processed: 1024 }), (1024).toLocaleString());
  });
});
