import type { ReactNode } from "react";
import { DtIcon, type IconName } from "../DtIcon";

export type CalloutTone = "info" | "success" | "warning" | "held" | "danger" | "neutral";

const TONE_ICON: Record<CalloutTone, IconName> = {
  info: "info",
  success: "success",
  warning: "warning",
  held: "warning",
  danger: "error",
  neutral: "info",
};

const TONE_LABEL: Record<CalloutTone, string> = {
  info: "Note",
  success: "Done",
  warning: "Warning",
  held: "Held",
  danger: "Problem",
  neutral: "Note",
};

export interface CalloutProps {
  /** info (blue), success (green), warning / held (amber), danger (red), neutral (grey). */
  tone?: CalloutTone;
  /** One line, in the user's terms. */
  title: ReactNode;
  /** Optional body: at most one or two short lines. */
  children?: ReactNode;
  /** One button or link when there is something to do. */
  action?: ReactNode;
  /**
   * Technical detail for administrators (env var names, server phrasing,
   * route names). Rendered only when `isAdmin` is true, behind a disclosure.
   */
  adminDetails?: ReactNode;
  /** Whether the viewer is an administrator; without it adminDetails never renders. */
  isAdmin?: boolean;
  /** `hairline` (default): 1px tone border on the surface. `tint`: subtle tinted fill. */
  variant?: "hairline" | "tint";
  /** `status` / `alert` when the callout appears in response to an action. */
  role?: "status" | "alert";
  className?: string;
}

/**
 * The one callout/banner. Tone is a 16px icon plus a hairline or subtle tint,
 * never a coloured stripe down the side.
 */
export function Callout({
  tone = "info",
  title,
  children,
  action,
  adminDetails,
  isAdmin = false,
  variant = "hairline",
  role,
  className = "",
}: CalloutProps) {
  return (
    <div
      className={`df-callout df-callout--${tone} df-callout--${variant} ${className}`.trim()}
      role={role}
      data-tone={tone}
    >
      <span className="df-callout-icon">
        <DtIcon name={TONE_ICON[tone]} size={16} chrome="product" />
        <span className="df2-sr-only">{TONE_LABEL[tone]}: </span>
      </span>
      <div className="df-callout-content">
        <p className="df-callout-title">{title}</p>
        {children ? <div className="df-callout-body">{children}</div> : null}
        {isAdmin && adminDetails ? (
          <details className="df-callout-admin">
            <summary>Details for administrators</summary>
            <div className="df-callout-admin-body">{adminDetails}</div>
          </details>
        ) : null}
      </div>
      {action ? <div className="df-callout-action">{action}</div> : null}
    </div>
  );
}
