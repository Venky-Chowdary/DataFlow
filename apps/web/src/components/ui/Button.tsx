import {
  type ButtonHTMLAttributes,
  type ReactNode,
  forwardRef,
} from "react";
import { ButtonLoader } from "../LoadingState";

export type ButtonVariant = "primary" | "secondary" | "ghost" | "danger" | "default";
export type ButtonSize = "sm" | "md" | "lg";

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
  loading?: boolean;
  loadingLabel?: string;
  leadingIcon?: ReactNode;
  trailingIcon?: ReactNode;
  block?: boolean;
  /** Optional class for gradual migration from raw df2-btn usage */
  className?: string;
}

const VARIANT_CLASS: Record<ButtonVariant, string> = {
  primary: "df2-btn-primary",
  secondary: "df2-btn-secondary",
  ghost: "df2-btn-ghost",
  danger: "df2-btn-danger",
  default: "",
};

const SIZE_CLASS: Record<ButtonSize, string> = {
  sm: "df2-btn-sm",
  md: "",
  lg: "df2-btn-lg",
};

/**
 * Canonical enterprise button. Prefer this over ad-hoc `df2-btn` class strings
 * so loading, disabled, and variants stay consistent app-wide.
 */
export const Button = forwardRef<HTMLButtonElement, ButtonProps>(function Button(
  {
    variant = "default",
    size = "md",
    loading = false,
    loadingLabel,
    leadingIcon,
    trailingIcon,
    block,
    className = "",
    disabled,
    children,
    type = "button",
    ...rest
  },
  ref,
) {
  const classes = [
    "df2-btn",
    VARIANT_CLASS[variant],
    SIZE_CLASS[size],
    block ? "df2-btn-block" : "",
    loading ? "is-loading" : "",
    className,
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <button
      ref={ref}
      type={type}
      className={classes}
      disabled={disabled || loading}
      aria-busy={loading || undefined}
      data-loading={loading ? "true" : undefined}
      {...rest}
    >
      {loading ? (
        <ButtonLoader label={loadingLabel} />
      ) : (
        <>
          {leadingIcon}
          {children}
          {trailingIcon}
        </>
      )}
    </button>
  );
});
