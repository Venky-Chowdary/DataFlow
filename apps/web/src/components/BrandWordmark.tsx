/**
 * Datawrap wordmark — clean type beside the wrap mark.
 * No under-word arrows (peer brands: Fivetran, Airbyte, Confluent).
 */

import { DtLogo } from "./DtLogo";

interface BrandWordmarkProps {
  markSize?: number;
  mark?: boolean;
  word?: boolean;
  className?: string;
  title?: string;
  size?: "sm" | "md" | "lg";
}

export function BrandWordmark({
  markSize = 36,
  mark = true,
  word = true,
  className = "",
  title = "",
  size = "md",
}: BrandWordmarkProps) {
  return (
    <span className={`dw-wordmark dw-wordmark--${size} ${word ? "" : "dw-wordmark--mark-only"} ${className}`.trim()}>
      {mark && <DtLogo size={markSize} title={word ? title : title || "Datawrap"} />}
      {word && <span className="dw-wordmark-text">Datawrap</span>}
    </span>
  );
}
