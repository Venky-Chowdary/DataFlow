import { IconFlowMark } from "../icons";

export function BrandLogo() {
  return (
    <div className="df-brand" title="DataFlow">
      <div className="df-brand-mark" aria-hidden>
        <IconFlowMark size={36} />
      </div>
      <div className="df-brand-text">
        <span className="df-brand-name">DataFlow</span>
        <span className="df-brand-tagline">8-gate preflight · fail-fast transfer</span>
      </div>
    </div>
  );
}
