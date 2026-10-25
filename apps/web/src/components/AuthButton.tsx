import { type ButtonHTMLAttributes, type ReactNode, forwardRef } from "react";
import { DtIcon } from "./DtIcon";

export type AuthButtonVariant = "primary" | "secondary" | "link" | "destructive";
export type AuthButtonSize = "sm" | "md" | "lg";

export interface AuthButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: AuthButtonVariant;
  size?: AuthButtonSize;
  /** Spinner + label; the control keeps its width and cannot double-submit. */
  loading?: boolean;
  loadingLabel?: string;
  /** Stretch to container width (default on the mobile breakpoint via CSS). */
  block?: boolean;
  leadingIcon?: ReactNode;
  trailingIcon?: ReactNode;
}

const VARIANT_CLASS: Record<AuthButtonVariant, string> = {
  primary: "lp-btn--brand",
  secondary: "lp-btn--outline",
  link: "lp-btn--link",
  destructive: "lp-btn--danger",
};

const SIZE_CLASS: Record<AuthButtonSize, string> = {
  sm: "lp-btn--sm",
  md: "",
  lg: "lp-btn--lg",
};

/**
 * The ONE button used across the auth surfaces (login, signup, verify,
 * forgot/reset, change-email, onboarding). Every state — hover, active,
 * focus-visible, disabled, loading — is styled once in landing.css from
 * logo-palette tokens; raw <button> and inline styles stay out of the
 * auth screens. Hit target is never below 44 px.
 */
export const AuthButton = forwardRef<HTMLButtonElement, AuthButtonProps>(function AuthButton(
  {
    variant = "primary",
    size = "md",
    loading = false,
    loadingLabel,
    block = false,
    leadingIcon,
    trailingIcon,
    className = "",
    disabled,
    children,
    type = "button",
    ...rest
  },
  ref,
) {
  const off = disabled || loading;
  const classes = [
    "lp-btn",
    VARIANT_CLASS[variant],
    SIZE_CLASS[size],
    block ? "lp-btn--block" : "",
    loading ? "is-loading" : "",
    className,
  ].filter(Boolean).join(" ");

  return (
    <button
      ref={ref}
      type={type}
      className={classes}
      disabled={off}
      aria-disabled={off || undefined}
      aria-busy={loading || undefined}
      {...rest}
    >
      {loading ? (
        <>
          <span className="lp-btn-spinner" aria-hidden="true" />
          <span>{loadingLabel ?? children}</span>
        </>
      ) : (
        <>
          {leadingIcon}
          <span>{children}</span>
          {trailingIcon}
        </>
      )}
    </button>
  );
});
