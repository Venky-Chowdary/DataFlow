import { useEffect, useState } from "react";
import { DtIcon } from "./DtIcon";
import { fetchPlatformStatus } from "../lib/api";

interface PlatformHonestyBannerProps {
  compact?: boolean;
  className?: string;
}

/** Shows honest transfer-ready counts — never overclaims catalog size. */
export function PlatformHonestyBanner({ compact = false, className = "" }: PlatformHonestyBannerProps) {
  const [status, setStatus] = useState<Awaited<ReturnType<typeof fetchPlatformStatus>> | null>(null);

  useEffect(() => {
    fetchPlatformStatus().then(setStatus).catch(() => setStatus(null));
  }, []);

  if (!status?.transfer_ready) return null;

  return (
    <div className={`df2-honesty-banner ${compact ? "is-compact" : ""} ${className}`.trim()} role="status">
      <DtIcon name="shield" size={compact ? 16 : 18} />
      <div className="df2-honesty-banner-copy">
        <strong>
          {status.transfer_ready} transfer-ready
          {status.catalog_total > status.transfer_ready && ` of ${status.catalog_total} catalog`}
        </strong>
        {!compact && (
          <span>
            {status.live_route_combinations}+ live routes · 8 preflight gates ·{" "}
            {status.llm_mapping_available ? "Hybrid LLM + BM25 mapping" : "Deterministic BM25 mapping"}
          </span>
        )}
      </div>
    </div>
  );
}
