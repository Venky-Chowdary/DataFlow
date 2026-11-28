import { DtIcon } from "../DtIcon";
import { ErrorReferenceChip } from "../ErrorReferenceChip";
import { splitReference } from "../../lib/errorCopy";
import { Button } from "./Button";
import { Callout } from "./Callout";

interface ErrorStateProps {
  /** What failed, in the reader's words: "Couldn't load job history". */
  title: string;
  /** The server's message; a trailing "Reference: …" renders as its own chip. */
  message?: string;
  /** One extra short line: what the reader can do besides retrying. */
  hint?: string;
  onRetry?: () => void;
  retrying?: boolean;
  /** Tighter spacing inside drawers and panels. */
  compact?: boolean;
}

/**
 * A failed read, in the place of the list that failed: the shared Callout
 * (danger), the cause in one line, the support reference and Retry. Never
 * the empty state: a failure must not read as "nothing here yet".
 */
export function ErrorState({ title, message, hint, onRetry, retrying, compact }: ErrorStateProps) {
  const { text: cause, reference } = splitReference(message);
  return (
    <Callout
      tone="danger"
      variant="tint"
      role="alert"
      className={`df2-error-state${compact ? " is-compact" : ""}`}
      title={title}
      action={onRetry ? (
        <Button
          variant="secondary"
          size="sm"
          onClick={onRetry}
          loading={retrying}
          loadingLabel="Retrying…"
          leadingIcon={<DtIcon name="refresh" size={14} />}
        >
          Retry
        </Button>
      ) : undefined}
    >
      {(cause || reference) && (
        <div className="df2-error-state-actions">
          {cause && <p className="df2-error-state-cause">{cause}</p>}
          <ErrorReferenceChip reference={reference} />
        </div>
      )}
      {hint && <p className="df2-error-state-cause">{hint}</p>}
    </Callout>
  );
}
