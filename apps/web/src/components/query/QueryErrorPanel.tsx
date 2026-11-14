import { useState } from "react";
import type { QueryFailureCopy } from "../../lib/queryWorkspace";
import { DtIcon } from "../DtIcon";

export interface QueryErrorPanelProps {
  failure: QueryFailureCopy;
  /** The connection the statement ran on, so the panel names the object. */
  connectorLabel?: string;
  /** Previous results still shown under the panel (UX-37: an error keeps them). */
  keptResults?: boolean;
  onDismiss: () => void;
}

/**
 * A failed run, in the shared error taxonomy's words: what failed, why, the
 * one next action, and the support reference — never the driver's text.
 */
export function QueryErrorPanel({ failure, connectorLabel, keptResults, onDismiss }: QueryErrorPanelProps) {
  const [copied, setCopied] = useState(false);
  const copyReference = async () => {
    try {
      await navigator.clipboard.writeText(failure.reference);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 2000);
    } catch {
      setCopied(false);
    }
  };
  return (
    <section
      className="df2-qw-error"
      role="alert"
      aria-labelledby="query-error-title"
      data-testid="query-error"
      data-code={failure.code || undefined}
    >
      <span className="df2-qw-error-icon" aria-hidden>
        <DtIcon name="alert" size={16} />
      </span>
      <div className="df2-qw-error-body">
        <h2 className="df2-qw-error-title" id="query-error-title">{failure.title}</h2>
        <p className="df2-qw-error-text">{failure.message}</p>
        {failure.action && (
          <p className="df2-qw-error-action">
            <strong>Next:</strong> {failure.action}
          </p>
        )}
        <p className="df2-qw-error-meta">
          {connectorLabel ? <span className="df2-qw-error-object">Ran on {connectorLabel} ·</span> : null}
          {failure.reference ? (
            <>
              <span>Support reference</span>
              <code data-testid="query-error-reference">{failure.reference}</code>
              <button type="button" className="df2-qw-link-btn" onClick={() => void copyReference()}>
                {copied ? "Copied" : "Copy reference"}
              </button>
            </>
          ) : (
            <span>No support reference was issued for this failure.</span>
          )}
          {keptResults && <span className="df2-qw-error-kept">Results below are from the previous run.</span>}
        </p>
      </div>
      <button type="button" className="df2-close-btn df2-close-btn-sm df2-qw-error-close" onClick={onDismiss} aria-label="Dismiss error">
        <DtIcon name="x" size={14} />
      </button>
    </section>
  );
}
