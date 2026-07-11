import { Component, type ErrorInfo, type ReactNode } from "react";
import { DtIcon } from "./DtIcon";

interface Props {
  children: ReactNode;
  label?: string;
}

interface State {
  error: Error | null;
}

/** Catches render errors so one broken panel does not white-screen the app. */
export class PageErrorBoundary extends Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error(`[DataFlow] ${this.props.label ?? "Page"} crashed`, error, info.componentStack);
  }

  render() {
    if (this.state.error) {
      return (
        <div className="df2-page-error" role="alert">
          <DtIcon name="alert" size={24} />
          <div>
            <strong>{this.props.label ?? "This page"} failed to load</strong>
            <p>{this.state.error.message}</p>
            <button
              type="button"
              className="df2-btn df2-btn-primary df2-btn-sm"
              onClick={() => this.setState({ error: null })}
            >
              Try again
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}
