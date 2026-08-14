import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import {
  conservationKindLabel,
  destHeadline,
  isDestMeasured,
  ledgerEquation,
  ledgerIdentityCells,
  readConservationLedger,
  writerAckDisagrees,
  writerHeadline,
  type LedgerCarrier,
} from "../../lib/conservationLedger";

interface ConservationLedgerCardProps {
  job: LedgerCarrier;
  className?: string;
  compact?: boolean;
  onOpenValidate?: () => void;
}

function toneClass(tone: string | undefined): string {
  if (tone === "ok") return "is-ok";
  if (tone === "warn") return "is-warn";
  if (tone === "danger") return "is-danger";
  return "is-muted";
}

/**
 * Operator-visible dest COUNT(*) conservation.
 * Displays the engine ledger — does not recompute dest from writer ack.
 */
export function ConservationLedgerCard({
  job,
  className = "",
  compact = false,
  onOpenValidate,
}: ConservationLedgerCardProps) {
  const ledger = readConservationLedger(job);
  const dest = destHeadline(job);
  const writer = writerHeadline(job);
  const measured = isDestMeasured(ledger);
  const disagrees = writerAckDisagrees(ledger);
  const unbalanced = Boolean(ledger && ledger.balanced === false);
  const tone = unbalanced ? "danger" : disagrees ? "warn" : dest.tone;
  const cells = ledger ? ledgerIdentityCells(ledger) : [];

  const cta = unbalanced && onOpenValidate
    ? { label: "Open Validate", onClick: onOpenValidate }
    : !measured && onOpenValidate
      ? { label: "Open Validate", onClick: onOpenValidate }
      : null;

  return (
    <section
      className={`df2-conservation-ledger ${toneClass(tone)} ${compact ? "is-compact" : ""} ${className}`.trim()}
      aria-label="Destination population conservation"
    >
      <div className="df2-conservation-ledger-head">
        <div className="df2-conservation-ledger-count" aria-hidden>
          <strong>{dest.value}</strong>
          <span>COUNT(*)</span>
        </div>
        <div className="df2-conservation-ledger-title">
          <div className="df2-conservation-ledger-title-row">
            <h3>Destination population</h3>
            <span className="df2-conservation-ledger-kind">
              {conservationKindLabel(ledger?.conservation_kind)}
            </span>
          </div>
          <p>
            {measured
              ? "Independent dest-engine read-back. Writer acknowledgement is diagnostic only."
              : "Dest COUNT(*) was not captured. Writer ack is not destination proof."}
          </p>
        </div>
      </div>

      <div className="df2-conservation-ledger-compare" aria-label="Dest COUNT versus writer acknowledgement">
        <article className={measured ? "is-dest" : "is-muted"}>
          <span>{dest.label}</span>
          <strong title={dest.title}>{dest.value}</strong>
        </article>
        <article className={disagrees ? "is-warn" : "is-ack"}>
          <span>{writer.label}</span>
          <strong title={writer.title}>{writer.value}</strong>
        </article>
      </div>

      {!compact && cells.length > 0 && (
        <ul className="df2-conservation-ledger-chips" aria-label="Conservation identity">
          {cells.map((cell) => (
            <li key={cell.label}>
              <span>{cell.label}</span>
              <strong>{cell.value}</strong>
            </li>
          ))}
        </ul>
      )}

      {!compact && ledger && (
        <p className="df2-conservation-ledger-eq" title={ledger.note}>
          {ledgerEquation(ledger)}
        </p>
      )}

      {disagrees && (
        <div className="df2-conservation-ledger-ack" role="note">
          <DtIcon name="alert" size={14} />
          <div>
            <strong>Writer ack disagrees with dest COUNT(*)</strong>
            <span>
              Dest holds {dest.measured ? dest.value : "an unmeasured population"}. Writer
              counted {writer.value}. That is the DMS Full Load / MISSING_TARGET hole —
              writer acknowledgement never closes conservation.
            </span>
          </div>
        </div>
      )}

      {ledger?.note && !compact && (
        <p className="df2-conservation-ledger-note">{ledger.note}</p>
      )}

      <div className="df2-conservation-ledger-next">
        <DtIcon name={unbalanced || !measured ? "alert" : "check"} size={14} />
        <div>
          <strong>
            {unbalanced
              ? "Ledger unbalanced"
              : measured
                ? "Ledger balanced"
                : "Dest unmeasured"}
          </strong>
          <span>
            {unbalanced
              ? "Rows read do not equal dest COUNT(*) plus hold-outs and skips."
              : measured
                ? "Every source row is at destination, quarantined, or skipped."
                : "Do not treat writer events as rows at destination."}
          </span>
        </div>
        {cta && (
          <Button size="sm" variant={unbalanced ? "secondary" : "ghost"} onClick={cta.onClick}>
            {cta.label}
          </Button>
        )}
      </div>
    </section>
  );
}
