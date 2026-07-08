import { ConnectorCatalogPanel } from "./ConnectorCatalogPanel";
import type { CatalogConnector } from "../lib/api";

interface ConnectorMarketplaceProps {
  role: "source" | "destination" | "all";
  title: string;
  onSelect: (connector: CatalogConnector) => void;
}

/** Compact marketplace wrapper — used in Transfer Studio wizard */
export function ConnectorMarketplace({ role, title, onSelect }: ConnectorMarketplaceProps) {
  return (
    <div className="edp-section">
      <div className="edp-section-head">
        <div>
          <h2 className="edp-section-title">{title}</h2>
          <p className="edp-section-sub">Select a connector to configure credentials</p>
        </div>
      </div>
      <ConnectorCatalogPanel role={role} onSelect={onSelect} limit={48} />
    </div>
  );
}

export type { CatalogConnector };
