import { useEffect, useState } from "react";
import { DtIcon } from "../DtIcon";
import type { SourceReadOptions, WorkbookSheet } from "../../lib/types";
import {
  declaredReadOptionCount,
  readWindowFromDraft,
  type ReadWindowDraft,
} from "../../lib/readOptions";

/** Codecs the API can name back to the operator; the server refuses anything else. */
const ENCODINGS = ["utf-8", "utf-8-sig", "latin-1", "cp1252", "utf-16", "utf-16-le", "utf-16-be"];

const DELIMITERS: { value: string; label: string }[] = [
  { value: ",", label: "Comma  ,"},
  { value: ";", label: "Semicolon  ;" },
  { value: "\t", label: "Tab" },
  { value: "|", label: "Pipe  |" },
];

const SHEET_WINDOW_TYPES = new Set(["excel"]);
const DELIMITED_TYPES = new Set(["csv", "tsv"]);

/** True when this source has a row window at all (workbook or delimited text). */
export function offersReadWindow(fileType: string): boolean {
  const type = (fileType || "").toLowerCase();
  return SHEET_WINDOW_TYPES.has(type) || DELIMITED_TYPES.has(type);
}

interface SourceReadWindowPanelProps {
  fileType: string;
  sheets: WorkbookSheet[];
  /** The window the server applied to the current preview. */
  applied: SourceReadOptions;
  busy: boolean;
  /** Refusal sentence when the operator may not re-profile; empty when allowed. */
  refusal?: string;
  onApply: (next: SourceReadOptions) => void;
}

function draftFrom(applied: SourceReadOptions): ReadWindowDraft {
  const headerRow = applied.header_row;
  return {
    sheet: applied.sheet || "",
    headerRow: headerRow === undefined || headerRow === 0 ? "1" : String(headerRow),
    headerless: headerRow === 0,
    skipRows: String(applied.skip_rows ?? 0),
    skipFooter: String(applied.skip_footer ?? 0),
    encoding: applied.encoding || "",
    delimiter: applied.delimiter || "",
  };
}

/**
 * Declares the window a file source is read through — sheet, header row, head
 * and footer skips, and the delimited-text codec/separator.
 *
 * The declaration is not a preview convenience: the same window is sent with
 * the run, so source COUNT, the write and Gate-8 reconcile the rows the
 * operator approved here. Applying it re-profiles the file server-side rather
 * than reshaping the sample in the browser, so an invalid declaration (an
 * unknown sheet, a blank header row) is refused in words instead of silently
 * falling back to the active sheet.
 */
export function SourceReadWindowPanel({
  fileType,
  sheets,
  applied,
  busy,
  refusal,
  onApply,
}: SourceReadWindowPanelProps) {
  const type = (fileType || "").toLowerCase();
  const [draft, setDraft] = useState<ReadWindowDraft>(() => draftFrom(applied));
  const [error, setError] = useState("");

  // Re-sync when the server confirms a window (or a new file resets it) so the
  // form never shows a declaration the preview was not built with.
  useEffect(() => {
    setDraft(draftFrom(applied));
    setError("");
  }, [applied]);

  if (!offersReadWindow(type)) return null;

  const isWorkbook = SHEET_WINDOW_TYPES.has(type);
  const isDelimited = DELIMITED_TYPES.has(type);
  const locked = Boolean(refusal);

  const submit = () => {
    const resolved = readWindowFromDraft(draft, { isWorkbook, isDelimited });
    if (resolved.options === null) {
      setError(resolved.error);
      return;
    }
    setError("");
    onApply(resolved.options);
  };

  const appliedCount = declaredReadOptionCount(applied);

  return (
    <details className="df2-source-read-window" open={appliedCount > 0}>
      <summary>
        Read window
        {appliedCount > 0 ? (
          <span className="df2-badge df2-badge-live" style={{ marginLeft: 8 }}>
            {appliedCount} declared
          </span>
        ) : (
          <span className="df2-label-hint" style={{ marginLeft: 8 }}>
            {isWorkbook ? "Active sheet · header on row 1" : "Sniffed codec · header on row 1"}
          </span>
        )}
      </summary>
      <p className="df2-label-hint">
        {isWorkbook
          ? "Pick the sheet and the physical row holding the column names, and drop title or totals rows. This window is what the run reads and reconciles — not just the preview."
          : "Declare the codec, separator and the physical header row, and drop leading or trailing rows. This window is what the run reads and reconciles — not just the preview."}
      </p>

      <div className="df2-form-row">
        {isWorkbook && (
          <div className="df2-field">
            <label className="df2-label" htmlFor="read-window-sheet">Sheet</label>
            <select
              id="read-window-sheet"
              className="df2-input df2-select"
              value={draft.sheet}
              disabled={locked || busy}
              onChange={(e) => setDraft({ ...draft, sheet: e.target.value })}
            >
              <option value="">Active sheet (default)</option>
              {sheets.map((sheet) => (
                <option key={`${sheet.index}:${sheet.name}`} value={sheet.name}>
                  {sheet.name}
                  {sheet.is_active ? " · active" : ""}
                </option>
              ))}
            </select>
            {sheets.length > 0 && (
              <span className="df2-label-hint">
                {sheets.length} sheet{sheets.length === 1 ? "" : "s"} in this workbook
              </span>
            )}
          </div>
        )}

        {isDelimited && (
          <>
            <div className="df2-field">
              <label className="df2-label" htmlFor="read-window-encoding">Encoding</label>
              <select
                id="read-window-encoding"
                className="df2-input df2-select"
                value={draft.encoding}
                disabled={locked || busy}
                onChange={(e) => setDraft({ ...draft, encoding: e.target.value })}
              >
                <option value="">Detect (default)</option>
                {ENCODINGS.map((codec) => (
                  <option key={codec} value={codec}>{codec}</option>
                ))}
              </select>
            </div>
            <div className="df2-field">
              <label className="df2-label" htmlFor="read-window-delimiter">Delimiter</label>
              <select
                id="read-window-delimiter"
                className="df2-input df2-select"
                value={draft.delimiter}
                disabled={locked || busy}
                onChange={(e) => setDraft({ ...draft, delimiter: e.target.value })}
              >
                <option value="">Sniff (default)</option>
                {DELIMITERS.map((item) => (
                  <option key={item.value} value={item.value}>{item.label}</option>
                ))}
              </select>
            </div>
          </>
        )}
      </div>

      <div className="df2-form-row">
        <div className="df2-field">
          <label className="df2-label" htmlFor="read-window-header-row">Header row</label>
          <input
            id="read-window-header-row"
            className="df2-input"
            inputMode="numeric"
            value={draft.headerless ? "" : draft.headerRow}
            placeholder={draft.headerless ? "col_0, col_1, …" : "1"}
            disabled={locked || busy || draft.headerless}
            onChange={(e) => setDraft({ ...draft, headerRow: e.target.value })}
          />
          <span className="df2-label-hint">
            Physical row number — rows above it are preamble and are never data.
          </span>
        </div>
        <div className="df2-field">
          <label className="df2-label" htmlFor="read-window-skip-rows">Skip first rows</label>
          <input
            id="read-window-skip-rows"
            className="df2-input"
            inputMode="numeric"
            value={draft.skipRows}
            disabled={locked || busy}
            onChange={(e) => setDraft({ ...draft, skipRows: e.target.value })}
          />
          <span className="df2-label-hint">Value-bearing data rows only — blank rows do not count.</span>
        </div>
        <div className="df2-field">
          <label className="df2-label" htmlFor="read-window-skip-footer">Skip last rows</label>
          <input
            id="read-window-skip-footer"
            className="df2-input"
            inputMode="numeric"
            value={draft.skipFooter}
            disabled={locked || busy}
            onChange={(e) => setDraft({ ...draft, skipFooter: e.target.value })}
          />
          <span className="df2-label-hint">Drops a totals or notes row at the end.</span>
        </div>
      </div>

      <label className="df2-policy-toggle">
        <input
          type="checkbox"
          checked={draft.headerless}
          disabled={locked || busy}
          onChange={(e) => setDraft({ ...draft, headerless: e.target.checked })}
        />
        <span>
          <strong>No header row</strong>
          <small>Every row is data; columns are named col_0, col_1, … by position.</small>
        </span>
      </label>

      {error && (
        <div className="df2-alert df2-alert-error" role="alert">
          <DtIcon name="x" size={16} />
          <div><p>{error}</p></div>
        </div>
      )}
      {refusal && (
        <p className="df2-label-hint" role="status">{refusal}</p>
      )}

      <div className="df2-upload-sample-row">
        <button
          type="button"
          className="df2-btn df2-btn-sm df2-btn-primary"
          onClick={submit}
          disabled={locked || busy}
          title="Re-profile the file through this window"
        >
          {busy ? "Re-profiling…" : "Apply read window"}
        </button>
        {appliedCount > 0 && (
          <button
            type="button"
            className="df2-btn df2-btn-sm df2-btn-ghost"
            onClick={() => onApply({})}
            disabled={locked || busy}
          >
            Reset to default
          </button>
        )}
      </div>
    </details>
  );
}
