import { useEffect, useMemo, useState } from "react";
import { Dialog } from "../ui/Dialog";
import type { AutoMapSuggestion } from "../../lib/mappingStudioApi";

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
  /** Current target per source, to show what a suggestion replaces. */
  currentTarget: (source: string) => string;
  onApply: (picked: AutoMapSuggestion[]) => void;
  onClose: () => void;
}

/**
 * Preview auto-map before anything changes: each suggestion with its match
 * kind, confidence and type check. Strong matches start selected, weak ones
 * (partial words) do not. Applying is one undo step.
 */
export function AutoMapDialog({
  open,
  mode,
  templateName = "",
  notes = [],
  loading,
  error,
  suggestions,
  currentTarget,
  onApply,
  onClose,
}: AutoMapDialogProps) {
  const [picked, setPicked] = useState<Set<string>>(new Set());

  useEffect(() => {
    setPicked(new Set(
      suggestions.filter((s) => s.selected && s.compat?.verdict !== "incompatible").map((s) => s.source),
    ));
  }, [suggestions]);

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
            Apply {chosen.length} mapping{chosen.length === 1 ? "" : "s"}
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
            <span role="status">{chosen.length} of {changes.length} selected</span>
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
                        <span className="df2-ms-automap-why">{s.reason}</span>
                      </td>
                      <td>
                        <span className="df2-ms-confidence" aria-label={`${pct} percent`}>
                          <span className="df2-ms-confidence-bar" aria-hidden="true">
                            <span style={{ width: `${pct}%` }} />
                          </span>
                          {pct}%
                        </span>
                      </td>
                      <td>
                        {s.compat ? (
                          <>
                            <span className={`df2-ms-compat is-${s.compat.verdict}`}>{s.compat.verdict}</span>
                            {s.compat.verdict !== "safe" && s.compat.reasons.length ? (
                              <span className="df2-ms-automap-why">
                                {s.compat.reasons.map((r) => r.message).join(" ")}
                              </span>
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
