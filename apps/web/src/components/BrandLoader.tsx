/** Branded loader — SVG lattice mark (unique, vector-crisp) */

import { DtLogo } from "./DtLogo";

interface BrandLoaderProps {
  size?: number;
  label?: string;
  className?: string;
  variant?: "default" | "premium";
}

export function BrandLoader({
  size = 40,
  label = "Loading",
  className = "",
  variant = "default",
}: BrandLoaderProps) {
  const isPremium = variant === "premium" || size >= 48;
  const mark = Math.max(18, Math.round(size * 0.72));

  return (
    <span
      className={`df-brand-loader ${isPremium ? "df-brand-loader--premium" : ""} ${className}`.trim()}
      role="status"
      aria-label={label}
      style={{ width: size, height: size }}
    >
      {isPremium && (
        <>
          <span className="df-brand-loader-orbit df-brand-loader-orbit--1" aria-hidden />
          <span className="df-brand-loader-orbit df-brand-loader-orbit--2" aria-hidden />
          <span className="df-brand-loader-orbit df-brand-loader-orbit--3" aria-hidden />
          <span className="df-brand-loader-glow" aria-hidden />
          <span className="df-brand-loader-ring df-brand-loader-ring--outer" aria-hidden />
        </>
      )}
      <span className="df-brand-loader-ring" aria-hidden />
      <span className="df-brand-loader-mark" aria-hidden style={{ display: "grid", placeItems: "center" }}>
        <DtLogo size={mark} title="" fidelity="svg" />
      </span>
    </span>
  );
}
