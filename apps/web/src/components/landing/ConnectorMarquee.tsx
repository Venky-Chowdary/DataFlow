import { ConnectorIcon } from "../../app/brand-icons";
import { catalogHonestyLead } from "../../lib/provenEvidence";

/** Certified duplex logos only — Planned brands (SAP, Workday, Netsuite) stay off this band. */
const ROW_A = [
  "postgresql", "mysql", "mongodb", "redis", "kafka", "salesforce", "hubspot", "json",
];

const ROW_B = [
  "postgresql", "mongodb", "kafka", "salesforce", "hubspot", "mysql", "redis", "json",
];

function MarqueeRow({ ids, reverse }: { ids: string[]; reverse?: boolean }) {
  const track = [...ids, ...ids];
  return (
    <div className={`lp-marquee-row ${reverse ? "reverse" : ""}`}>
      <div className="lp-marquee-track">
        {track.map((id, i) => (
          <div key={`${id}-${i}`} className="lp-marquee-item" title={id}>
            <ConnectorIcon id={id} size={26} />
          </div>
        ))}
      </div>
    </div>
  );
}

export function ConnectorMarquee() {
  return (
    <section className="lp-marquee-band" aria-label="TRANSFER_READY connectors — catalog tiles are not transfer-live">
      <p className="lp-marquee-eyebrow">{catalogHonestyLead()}</p>
      <MarqueeRow ids={ROW_A} />
      <MarqueeRow ids={ROW_B} reverse />
    </section>
  );
}
