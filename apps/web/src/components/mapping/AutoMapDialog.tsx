import { useEffect, useMemo, useState } from "react";
import { Dialog } from "../ui/Dialog";
import type { AutoMapSuggestion } from "../../lib/mappingStudioApi";
import { plural } from "../../lib/plural";

/** Type verdicts that change values on write: never pre-selected. */
const NEEDS_CHOICE = new Set(["lossy", "incompatible", "unknown"]);

const COMPAT_LABEL: Record<string, string> = {
  safe: "Safe",
  lossy: "Lossy",
  incompatible: "Incompatible",
  unknown: "Unread",
};

/**
 * A suggestion starts selected only when the matcher picked it, it clears the
 * mode's confidence floor, and the type check did not flag it lossy.
 */
export function autoMapPreselected(s: AutoMapSuggestion, floor: number): boolean {
  return Boolean(s.selected) && s.confidence >= floor && !NEEDS_CHOICE.has(s.compat?.verdict ?? "");
}

export const MATCH_KIND_LABEL: Record<string, string> = {
  exact: "Exact",
  case_insensitive: "Case",
  normalized: "Normalized",
  learned: "Learned",
  synonym: "Synonym",
  concept: "Concept",
  key_suffix: "Key",
  token: "Partial",
  template: "Template",
};

interface AutoMapDialogProps {
  open: boolean;
  mode: "all" | "remaining" | "template";
  /** Template name when ``mode`` is "template". */
  templateName?: string;
  /** Plain-language notes shown above the table (pairs a template could not re-bind). */
  notes?: string[];
  loading: boolean;
  error: string;
  suggestions: AutoMapSuggestion[];
  /** The validation mode's confidence floor (strict 0.85). Below it the bar is amber and the row starts unselected. */
  confidenceFloor?: number;
  /** Current target per source, to show what a suggestion replaces. */
  currentTarget: (source: string) => string;
  onApply: (picked: AutoMapSuggestion[]) => void;
  onClose: () => void;
}

/**
 * Preview auto-map before anything changes: each suggestion with its match
 * kind, confidence and type check. Only safe matches at or above the
 * confidence floor start selected; lossy and weak ones wait for a choice.
 * Applying is one undo step.
 */
export function AutoMapDialog({
  open,
  mode,
  templateName = "",
  notes = [],
  loading,
  error,
  suggestions,
  confidenceFloor = 0.85,
  currentTarget,
  onApply,
  onClose,
}: AutoMapDialogProps) {
  const [picked, setPicked] = useState<Set<string>>(new Set());

  useEffect(() => {
    setPicked(new Set(
      suggestions.filter((s) => autoMapPreselected(s, confidenceFloor)).map((s) => s.source),
    ));
  }, [suggestions, confidenceFloor]);

  const changes = useMemo(
    () => suggestions.filter((s) => currentTarget(s.source) !== s.target),
    [suggestions, currentTarget],
  );
  const chosen = changes.filter((s) => picked.has(s.source));

  const toggle = (source: string) =>
    setPicked((prev) => {
      const next = new Set(prev);
      if (next.has(source)) next.delete(source);
      else next.add(source);
      return next;
    });

  return (
    <Dialog
      open={open}
      onClose={onClose}
      size="lg"
      title={
        mode === "remaining" ? "Map remaining columns" : mode === "template" ? `Apply template “${templateName}”` : "Auto-map columns"
      }
      subtitle={
        mode === "remaining"
          ? "Suggestions for source columns that have no destination yet. Columns you mapped stay as they are."
          : mode === "template"
            ? "The template's pairs, re-bound onto this table's columns. Review before applying."
            : "Suggestions for every source column by name: exact, normalized, learned from this workspace, then synonyms."
      }
      ariaLabel="Auto-map preview"
      className="df2-ms-automap"
      footer={
        <>
          <button type="button" className="df2-btn df2-btn-ghost" onClick={onClose}>
            Cancel
          </button>
          <button
            type="button"
            className="df2-btn df2-btn-primary"
            disabled={chosen.length === 0}
            onClick={() => onApply(chosen)}
          >
            Apply {plural(chosen.length, "mapping")}
          </button>
        </>
      }
    >
      {notes.length > 0 && (
        <ul className="df2-ms-panel-note" aria-label="Notes">
          {notes.map((n) => <li key={n}>{n}</li>)}
        </ul>
      )}
      {loading ? (
        <p role="status">Finding matches…</p>
      ) : error ? (
        <p className="df2-ms-panel-note is-block" role="alert">{error}</p>
      ) : changes.length === 0 ? (
        <p role="status">No new matches: every suggestion is already mapped, or no destination column matches by name.</p>
      ) : (
        <>
          <div className="df2-ms-automap-tools">
            <span role="status">
              {chosen.length} of {changes.length} selected
              {changes.some((s) => !autoMapPreselected(s, confidenceFloor))
                ? " · lossy and low-confidence matches start unselected"
                : ""}
            </span>
            <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={() => setPicked(new Set(changes.map((s) => s.source)))}>
              Select all
            </button>
            <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={() => setPicked(new Set())}>
              Select none
            </button>
          </div>
          <div className="df2-ms-automap-scroll">
            <table className="df2-ms-automap-table">
              <caption className="df2-sr-only">Suggested mappings</caption>
              <thead>
                <tr>
                  <th scope="col"><span className="df2-sr-only">Apply</span></th>
                  <th scope="col">Source</th>
                  <th scope="col">Destination</th>
                  <th scope="col">Match</th>
                  <th scope="col">Confidence</th>
                  <th scope="col">Types</th>
                </tr>
              </thead>
              <tbody>
                {changes.map((s) => {
                  const was = currentTarget(s.source);
                  const pct = Math.round(s.confidence * 100);
                  const belowFloor = s.confidence < confidenceFloor;
                  const verdict = s.compat?.verdict;
                  return (
                    <tr key={s.source} className={picked.has(s.source) ? "is-picked" : undefined}>
                      <td>
                        <input
                          aria-label={`Apply ${s.source} to ${s.target}`}
                          type="checkbox"
                          checked={picked.has(s.source)}
                          onChange={() => toggle(s.source)}
                        />
                      </td>
                      <th scope="row">{s.source}</th>
                      <td>
                        <span className="df2-ms-automap-target">{s.target}</span>
                        {was ? <span className="df2-ms-automap-was">was {was}</span> : null}
                      </td>
                      <td>
                        <span className={`df2-ms-match is-${s.kind}`}>{MATCH_KIND_LABEL[s.kind] ?? s.kind}</span>
                        {s.reason ? (
                          <details className="df2-ms-automap-why">
                            <summary>Why</summary>
                            {s.reason}
                          </details>
                        ) : null}
                      </td>
                      <td>
                        <span
                          className={`df2-ms-confidence${belowFloor ? " is-below-floor" : ""}`}
                          aria-label={`${pct} percent${belowFloor ? `, below the ${Math.round(confidenceFloor * 100)} percent floor` : ""}`}
                        >
                          <span className="df2-ms-confidence-bar" aria-hidden="true">
                            <span style={{ width: `${pct}%` }} />
                          </span>
                          {pct}%
                        </span>
                      </td>
                      <td>
                        {s.compat && verdict ? (
                          <>
                            <span className={`df2-ms-compat is-${verdict}`}>{COMPAT_LABEL[verdict] ?? verdict}</span>
                            {verdict !== "safe" && s.compat.reasons.length ? (
                              <details className="df2-ms-automap-why">
                                <summary>Why</summary>
                                {s.compat.reasons.map((r) => r.message).join(" ")}
                              </details>
                            ) : null}
                          </>
                        ) : null}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </>
      )}
    </Dialog>
  );
}
