import { ReactNode } from "react";
import { BrandWordmark } from "./BrandWordmark";
import { DtIcon } from "./DtIcon";
import { AuthButton } from "./AuthButton";

interface AuthBrandPanelProps {
  title: string;
  lead: string;
  onBack?: () => void;
  /** Optional trust metrics row (login only). */
  metrics?: Array<{ value: string; label: string }>;
  /** Optional "Continue to X" line under the body copy. */
  dest?: ReactNode;
}

/**
 * Shared dark brand rail for every gated auth screen (login, signup,
 * verify, forgot/reset). Extracted so the chrome — Back placement,
 * logo/wordmark treatment, proof strip, footer — can't drift between
 * pages. Back sits in the dark header, never inside the light card.
 */
export function AuthBrandPanel({ title, lead, onBack, metrics, dest }: AuthBrandPanelProps) {
  return (
    <section className="lp-login-brand" aria-label="Datawrap">
      <div className="lp-login-brand-bg" aria-hidden>
        <span className="lp-login-brand-mesh" />
        <span className="lp-login-brand-orb lp-login-brand-orb--a" />
        <span className="lp-login-brand-orb lp-login-brand-orb--b" />
      </div>

      <div className="lp-login-brand-inner">
        <header className="lp-login-brand-head">
          {onBack ? (
            <AuthButton variant="link" className="lp-login-back" onClick={onBack}
              leadingIcon={<DtIcon name="chevron-left" size={15} />}>
              Back
            </AuthButton>
          ) : (
            <span />
          )}
          <div className="lp-login-brand-logo">
            <BrandWordmark markSize={32} title="" />
          </div>
        </header>

        <div className="lp-login-brand-body">
          <p className="lp-login-brand-kicker">Enterprise data platform</p>
          <h1 className="lp-login-brand-title">{title}</h1>
          <span className="lp-login-brand-rule" aria-hidden="true" />
          <p className="lp-login-brand-lead">{lead}</p>

          <div className="lp-login-proof" aria-hidden>
            <div className="lp-login-proof-flow">
              <span>Map</span>
              <i />
              <span>Preflight</span>
              <i />
              <span>Prove</span>
            </div>
            <p>Every session inherits workspace RBAC. Agents never skip gates.</p>
          </div>

          {metrics && metrics.length > 0 && (
            <div className="lp-login-metrics" role="list">
              {metrics.map((m) => (
                <div key={m.label} className="lp-login-metric" role="listitem">
                  <strong>{m.value}</strong>
                  <span>{m.label}</span>
                </div>
              ))}
            </div>
          )}

          {dest && <p className="lp-login-brand-dest">{dest}</p>}
        </div>

        <footer className="lp-login-brand-foot">
          <span>Self-host ready</span>
          <span aria-hidden>·</span>
          <span>SSO / RBAC</span>
          <span aria-hidden>·</span>
          <span>Audit trails</span>
        </footer>
      </div>
    </section>
  );
}
