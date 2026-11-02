import type { SourceReadOptions } from "./types";

/**
 * Serializes only the fields the operator declared.
 *
 * A default window must send nothing at all, so a file with no declaration
 * keeps today's behaviour (active sheet, header on row 1, sniffed codec). A
 * sheet index of -1 means "unset" on the wire and is dropped for the same
 * reason.
 */
export function readOptionsPayload(options?: SourceReadOptions): string {
  if (!options) return "";
  const declared = Object.entries(options).filter(([key, value]) => {
    if (value === undefined || value === null || value === "") return false;
    if (key === "sheet_index" && value === -1) return false;
    return true;
  });
  if (!declared.length) return "";
  return JSON.stringify(Object.fromEntries(declared));
}

/** How many fields of the window are declared — drives the "N declared" badge. */
export function declaredReadOptionCount(options?: SourceReadOptions): number {
  const payload = readOptionsPayload(options);
  return payload ? Object.keys(JSON.parse(payload) as object).length : 0;
}

/** What the read-window form holds, before it is a declaration. */
export interface ReadWindowDraft {
  sheet: string;
  headerRow: string;
  headerless: boolean;
  skipRows: string;
  skipFooter: string;
  encoding: string;
  delimiter: string;
}

function wholeNumber(raw: string): number | null {
  const text = raw.trim();
  if (!text) return 0;
  if (!/^\d+$/.test(text)) return null;
  return Number(text);
}

/**
 * Turns the form into a declaration, or names why it is not one.
 *
 * Only fields this source kind can honour are carried, so a sheet name is
 * never sent for a CSV and a codec is never sent for a workbook — the API
 * refuses those, and refusing them here keeps the message about the real
 * mistake.
 */
export function readWindowFromDraft(
  draft: ReadWindowDraft,
  capability: { isWorkbook: boolean; isDelimited: boolean },
): { options: SourceReadOptions; error: "" } | { options: null; error: string } {
  const headerRow = draft.headerless ? 0 : wholeNumber(draft.headerRow);
  const skipRows = wholeNumber(draft.skipRows);
  const skipFooter = wholeNumber(draft.skipFooter);
  if (headerRow === null || skipRows === null || skipFooter === null) {
    return { options: null, error: "Header row and row skips must be whole numbers (0 or more)." };
  }
  if (!draft.headerless && headerRow < 1) {
    return {
      options: null,
      error: "Header row is 1-based — use “No header row” for a headerless sheet.",
    };
  }
  const options: SourceReadOptions = {};
  if (capability.isWorkbook && draft.sheet) options.sheet = draft.sheet;
  if (headerRow !== 1) options.header_row = headerRow;
  if (skipRows) options.skip_rows = skipRows;
  if (skipFooter) options.skip_footer = skipFooter;
  if (capability.isDelimited && draft.encoding) options.encoding = draft.encoding;
  if (capability.isDelimited && draft.delimiter) options.delimiter = draft.delimiter;
  return { options, error: "" };
}
