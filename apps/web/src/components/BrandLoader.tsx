/** Branded loader — DataFlow mark with triple orbit rings and data particles */

import { useId } from "react";

interface BrandLoaderProps {
  size?: number;
  label?: string;
  className?: string;
  /** Larger hero / section loader with orbiting particles */
  variant?: "default" | "premium";
}

export function BrandLoader({
  size = 40,
  label = "Loading",
  className = "",
  variant = "default",
}: BrandLoaderProps) {
  const gradId = useId();
  const isPremium = variant === "premium" || size >= 48;

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
      <svg className="df-brand-loader-mark" viewBox="0 0 36 36" fill="none" aria-hidden>
        <rect width="36" height="36" rx="9" fill={`url(#${gradId})`} />
        <path
          d="M10 24V14M10 14h6l2 4 4-8h4"
          stroke="white"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          opacity="0.95"
        />
        <circle cx="10" cy="24" r="2.5" fill="#F59E0B" />
        <circle cx="10" cy="14" r="2.5" fill="#2DD4BF" />
        <circle cx="26" cy="10" r="2.5" fill="#A7F3D0" />
        <defs>
          <linearGradient id={gradId} x1="4" y1="4" x2="32" y2="32" gradientUnits="userSpaceOnUse">
            <stop stopColor="#0F3D3A" />
            <stop offset="0.55" stopColor="#0D9488" />
            <stop offset="1" stopColor="#14B8A6" />
          </linearGradient>
        </defs>
      </svg>
    </span>
  );
}
