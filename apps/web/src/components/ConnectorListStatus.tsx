/**
 * The connector list's own state where a page lists connectors (D410).
 *
 * The shell owns one load of GET /connectors/saved. Until a read answers, or
 * after every read failed with nothing loaded, the rows are [] but the
 * workspace may well have connections. A page must then say "loading" or
 * "couldn't load" with a way to try again, never "no connections".
 */
import { DtIcon } from "./DtIcon";
import { LoadingBlock } from "./LoadingState";
import { Button } from "./ui/Button";
import { EmptyState } from "./ui/EmptyState";

/** The list's load state as the shell hands it to every page that shows connectors. */
export interface ConnectorListStatusProps {
  /** Nothing has loaded yet and a read may still answer. */
  connectorsLoading?: boolean;
  /** Every read failed and nothing has loaded: the reason, without a trailing period. */
  connectorsError?: string;
  /** Read the list again. */
  onRetryConnectors?: () => void;
}

/** True only when the rows are known: a page may say "no connections" for [] then. */
export function connectorListKnown({ connectorsLoading, connectorsError }: ConnectorListStatusProps): boolean {
  return !connectorsLoading && !connectorsError;
}

/**
 * The loading or failure notice, or null when the list is known. ``compact``
 * fits a card or a picker; the default fills a page section.
 */
export function ConnectorListStatus({
  connectorsLoading = false,
  connectorsError = "",
  onRetryConnectors,
  compact = false,
}: ConnectorListStatusProps & { compact?: boolean }) {
  if (connectorsLoading) {
    return (
      <LoadingBlock
        title="Loading connections"
        hint="Fetching saved connectors from your workspace…"
        size="sm"
        variant={compact ? "card" : "glass"}
      />
    );
  }
  if (!connectorsError) return null;
  return (
    <EmptyState
      compact={compact}
      role="alert"
      icon="alert"
      title="Couldn't load your connections"
      description={`${connectorsError}. Reading the list doesn't change your connections. Try again in a moment.`}
      action={
        onRetryConnectors ? (
          <Button variant="primary" onClick={() => onRetryConnectors()} leadingIcon={<DtIcon name="refresh" size={14} />}>
            Try again
          </Button>
        ) : undefined
      }
    />
  );
}
