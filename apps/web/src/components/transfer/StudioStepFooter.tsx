/**
 * The Transfer Studio step footer controls: one Back, one primary, one place.
 *
 * Every step (Source, Destination, Transform, Map, Validate) uses these so the
 * primary is always the rightmost control of the footer, at the same height,
 * radius and type size, with a trailing arrow icon (never a "→" character).
 * When the primary is disabled, the footer says why in visible text next to
 * it (`StepFooterReason`), not only in a tooltip.
 */
import type { ReactNode } from "react";
import { DtIcon } from "../DtIcon";
import { Button, type ButtonProps } from "../ui/Button";

export function StepBackButton({
  label = "Back",
  ...rest
}: Omit<ButtonProps, "variant" | "children" | "leadingIcon"> & { label?: string }) {
  return (
    <Button
      {...rest}
      className={`df2-step-back${rest.className ? ` ${rest.className}` : ""}`}
      leadingIcon={<DtIcon name="arrow-left" size={16} />}
    >
      {label}
    </Button>
  );
}

export function StepPrimaryButton({
  children,
  busy = false,
  loadingLabel,
  arrow = true,
  leadingIcon,
  ...rest
}: Omit<ButtonProps, "variant" | "loading" | "trailingIcon"> & {
  children: ReactNode;
  /** Shows the inline spinner and `loadingLabel`, keeping the button's width and place. */
  busy?: boolean;
  /** Continue buttons carry a trailing arrow; actions such as Run transfer do not. */
  arrow?: boolean;
}) {
  return (
    <Button
      {...rest}
      variant="primary"
      loading={busy}
      loadingLabel={loadingLabel}
      leadingIcon={leadingIcon}
      className={`df2-step-primary${rest.className ? ` ${rest.className}` : ""}`}
      trailingIcon={arrow && !busy ? <DtIcon name="arrow-right" size={16} /> : undefined}
    >
      {children}
    </Button>
  );
}

/** Why the primary is disabled, in one short visible line beside it. */
export function StepFooterReason({ id, children }: { id?: string; children: ReactNode }) {
  if (!children) return null;
  return (
    <span className="df2-step-footer-reason" id={id} role="status">
      {children}
    </span>
  );
}
