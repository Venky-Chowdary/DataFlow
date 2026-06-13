import type { ReactNode } from "react";

interface ActionBarProps {
  children: ReactNode;
  align?: "split" | "end";
  sticky?: boolean;
}

export function ActionBar({ children, align = "end", sticky = false }: ActionBarProps) {
  return (
    <div
      className={[
        "df-action-bar",
        sticky ? "df-action-bar--sticky" : "",
        align === "end" ? "df-action-bar--end" : "df-action-bar--split",
      ]
        .filter(Boolean)
        .join(" ")}
    >
      {children}
    </div>
  );
}
