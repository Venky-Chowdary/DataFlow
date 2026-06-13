import type { ButtonHTMLAttributes, ReactNode } from "react";

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "primary" | "secondary" | "ghost";
  size?: "sm" | "md" | "lg";
  children: ReactNode;
}

export function Button({
  variant = "primary",
  size = "md",
  className = "",
  children,
  ...props
}: ButtonProps) {
  const classes = [
    "df-btn",
    variant === "primary" ? "df-btn-primary" : "",
    variant === "secondary" ? "df-btn-secondary" : "",
    variant === "ghost" ? "df-btn-ghost" : "",
    size === "sm" ? "df-btn-sm" : "",
    size === "lg" ? "df-btn-lg" : "",
    className,
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <button type="button" className={classes} {...props}>
      {children}
    </button>
  );
}
