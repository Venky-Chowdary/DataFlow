import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import {
  conservationKindLabel,
  destHeadline,
  isDestMeasured,
  ledgerEquation,
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

  const cta = unbalanced && onOpenValidate
    ? { label: "Open Validate", onClick: onOpenValidate }
    : !measured && onOpenValidate
      ? { label: "Open Validate", onClick: onOpenValidate }
      : null;

  const keyed = ledger?.conservation_kind === "keyed"
    && (ledger.inserts != null || ledger.deletes != null);

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
        <div>
          <h3>Destination population</h3>
          <p>
            {conservationKindLabel(ledger?.conservation_kind)}
            {measured
              ? " — independent dest-engine read-back, not writer acknowledgement."
              : " — dest COUNT(*) not captured. Writer ack is not destination proof."}
          </p>
        </div>
      </div>

      {!compact && ledger && (
        <p className="df2-conservation-ledger-eq" title={ledger.note}>
          {ledgerEquation(ledger)}
        </p>
      )}

      {!compact && keyed && ledger && (
        <ul className="df2-conservation-ledger-chips" aria-label="Keyed census">
          <li>
            <span>Inserts</span>
            <strong>{(ledger.inserts ?? 0).toLocaleString()}</strong>
          </li>
          <li>
            <span>Updates</span>
            <strong>{(ledger.updates ?? 0).toLocaleString()}</strong>
          </li>
          <li>
            <span>Deletes</span>
            <strong>{(ledger.deletes ?? 0).toLocaleString()}</strong>
          </li>
          <li>
            <span>Dest Δ</span>
            <strong>{(ledger.dest_delta ?? 0).toLocaleString()}</strong>
          </li>
        </ul>
      )}

      {disagrees && (
        <div className="df2-conservation-ledger-ack" role="note">
          <DtIcon name="alert" size={14} />
          <div>
            <strong>Writer ack {writer.value}</strong>
            <span>
              Dest COUNT(*) is {dest.measured ? dest.value : "unmeasured"}. Writer
              acknowledgement never closes conservation (DMS Full Load / MISSING_TARGET).
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
