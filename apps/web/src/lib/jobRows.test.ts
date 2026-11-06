import { describe, expect, it } from "vitest";

import { formatRowsProcessed, rowsProcessedUnknown } from "./jobRows";

describe("rows a cancelled job kept", () => {
  it("treats a null count as unknown, never zero", () => {
    expect(rowsProcessedUnknown({ records_processed: null })).toBe(true);
    expect(formatRowsProcessed({ records_processed: 0, records_processed_unknown: true })).toBe(
      "an unknown number of",
    );
  });

  it("keeps a real zero and real counts", () => {
    expect(rowsProcessedUnknown({ records_processed: 0 })).toBe(false);
    expect(rowsProcessedUnknown({})).toBe(false);
    expect(formatRowsProcessed({ records_processed: 0 })).toBe("0");
    expect(formatRowsProcessed({ records_processed: 1024 })).toBe((1024).toLocaleString());
  });
});
