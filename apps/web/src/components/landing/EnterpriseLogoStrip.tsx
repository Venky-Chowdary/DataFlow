import { useRevealOnScroll } from "../../hooks/useRevealOnScroll";

const LOGOS = [
  { name: "Nexus Bank", abbr: "NB" },
  { name: "Summit Health", abbr: "SH" },
  { name: "Apex SaaS", abbr: "AX" },
  { name: "Meridian Retail", abbr: "MR" },
  { name: "Vantage Logistics", abbr: "VL" },
];

export function EnterpriseLogoStrip() {
  const reveal = useRevealOnScroll();
  return (
    <section className={`lp-trust lp-reveal ${reveal.className}`} ref={reveal.ref} aria-label="Trusted by">
      <p className="lp-trust-eyebrow">Trusted by data teams at</p>
      <div className="lp-trust-logos">
        {LOGOS.map((logo) => (
          <div key={logo.abbr} className="lp-trust-logo" title={logo.name}>
            <span className="lp-trust-logo-mark" aria-hidden>{logo.abbr}</span>
            <span className="lp-trust-logo-name">{logo.name}</span>
          </div>
        ))}
      </div>
    </section>
  );
}
