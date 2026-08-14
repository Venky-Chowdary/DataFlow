import { Component, type ErrorInfo, type ReactNode } from "react";
import {
  isStaleChunkError,
  pageErrorCopy,
  reloadOnceForStaleChunk,
  shouldAutoReloadStaleChunk,
  isStaleChunkReloadInFlight,
} from "../lib/lazyPage";
import { LoadingBlock } from "./LoadingState";
import { Button } from "./ui/Button";
import { EmptyState } from "./ui/EmptyState";

interface Props {
  children: ReactNode;
  label?: string;
}

interface State {
  error: Error | null;
}

/** Catches render and stale-chunk errors so one broken panel does not white-screen the app. */
export class PageErrorBoundary extends Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    if (reloadOnceForStaleChunk(error)) return;
    console.error(`[Datawrap] ${this.props.label ?? "Page"} crashed`, error, info.componentStack);
  }

  private retry = () => {
    const error = this.state.error;
    if (error && isStaleChunkError(error)) {
      window.location.reload();
      return;
    }
    this.setState({ error: null });
  };

  render() {
    const error = this.state.error;
    if (!error) return this.props.children;

    const label = this.props.label ?? "This page";
    if (isStaleChunkReloadInFlight() || shouldAutoReloadStaleChunk(error)) {
      return (
        <div className="df2-page" role="status" aria-live="polite">
          <LoadingBlock title="Refreshing workspace…" hint="Loading the current Datawrap build." />
        </div>
      );
    }

    const copy = pageErrorCopy(label, error);
    return (
      <EmptyState
        page
        role="alert"
        icon={copy.reload ? "refresh" : "alert"}
        title={copy.title}
        description={copy.description}
        action={
          <div className="df2-empty-actions-row">
            <Button variant="primary" size="sm" onClick={this.retry}>
              {copy.reload ? "Reload" : "Try again"}
            </Button>
            {!copy.reload && (
              <Button variant="ghost" size="sm" onClick={() => window.location.reload()}>
                Reload app
              </Button>
            )}
          </div>
        }
      />
    );
  }
}
