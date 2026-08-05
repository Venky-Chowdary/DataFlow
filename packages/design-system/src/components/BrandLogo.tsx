import { IconFlowMark } from "../icons";

/** Datawrap wordmark — mark + product name. */
export function BrandLogo() {
  return (
    <div className="df-brand" title="Datawrap">
      <div className="df-brand-mark" aria-hidden>
        <IconFlowMark size={36} />
      </div>
      <div className="df-brand-text">
        <span className="df-brand-name">Datawrap</span>
        <span className="df-brand-tagline">8-gate preflight · fail-fast transfer</span>
      </div>
    </div>
  );
}
