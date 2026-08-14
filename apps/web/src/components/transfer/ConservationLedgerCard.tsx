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
  const isMirror = ledger?.conservation_kind === "mirror";
  const isJob = ledger?.conservation_kind === "job_rollup";
  const isArtifact = ledger?.rows_written_source === "artifact_readback";
  const leftover =
    isMirror && ledger?.dest_count != null && ledger.active_count != null
      ? Math.max(ledger.dest_count - ledger.active_count, 0)
      : 0;
  const tone = unbalanced ? "danger" : disagrees ? "warn" : dest.tone;
  const cells = ledger ? ledgerIdentityCells(ledger) : [];

  const cta = unbalanced && onOpenValidate
    ? { label: "Open Validate", onClick: onOpenValidate }
    : !measured && onOpenValidate
      ? { label: "Open Validate", onClick: onOpenValidate }
      : null;

  const unit = isMirror ? "ACTIVE" : isJob && dest.value === "—" ? "STREAMS" : isArtifact ? "RECORDS" : "COUNT(*)";
  const nextTitle = unbalanced
    ? "Ledger unbalanced"
    : measured
      ? "Ledger balanced"
      : "Dest unmeasured";
  const nextBody = unbalanced
    ? isJob
      ? "The job is closed iff every stream ledger is closed. Last-table dest COUNT(*) is not the job."
      : isMirror
      ? "Rows read do not equal dest-engine active population plus hold-outs and skips."
      : isArtifact
        ? "Rows read do not equal independent artifact record count plus hold-outs and skips."
        : "Rows read do not equal dest COUNT(*) plus hold-outs and skips."
    : measured
      ? isJob
        ? dest.value === "—"
          ? "Every stream ledger is closed. Dest COUNT(*) is not summed across mixed or keyed kinds."
          : "Every stream ledger is closed. Job dest is the sum of dest-engine counts of the same kind."
        : isMirror
        ? leftover
          ? `Every source row is active at destination, quarantined, or skipped. ${leftover.toLocaleString()} leftover dest key(s) stay as _deleted — physical COUNT(*) does not drop.`
          : "Every source row is active at destination, quarantined, or skipped. Physical COUNT(*) does not drop on soft-delete."
        : isArtifact
          ? "Every source row is in the export artifact, quarantined, or skipped. Cell fidelity is unproven."
          : "Every source row is at destination, quarantined, or skipped."
      : "Do not treat writer events as rows at destination.";

  return (
    <section
      className={`df2-conservation-ledger ${toneClass(tone)} ${isMirror ? "is-mirror" : ""} ${compact ? "is-compact" : ""} ${className}`.trim()}
      aria-label={
        isJob
          ? "Job stream conservation"
          : isMirror
            ? "Mirror active population conservation"
            : isArtifact
              ? "Export artifact population conservation"
              : "Destination population conservation"
      }
    >
      <div className="df2-conservation-ledger-head">
        <div className="df2-conservation-ledger-count" aria-hidden>
          <strong>{dest.value}</strong>
          <span>{unit}</span>
        </div>
        <div className="df2-conservation-ledger-title">
          <div className="df2-conservation-ledger-title-row">
            <h3>
              {isJob
                ? "Job destination population"
                : isMirror
                  ? "Active destination population"
                  : isArtifact
                    ? "Export artifact population"
                    : "Destination population"}
            </h3>
            <span className="df2-conservation-ledger-kind">
              {conservationKindLabel(ledger?.conservation_kind)}
            </span>
          </div>
          <p>
            {measured
              ? isJob
                ? dest.value === "—"
                  ? "Every stream has a dest-engine ledger. Dest COUNT(*) is not summed across mixed or keyed kinds. Writer acknowledgement is diagnostic only."
                  : "Sum of dest-engine counts across streams of the same kind. Last-table COUNT(*) is not the job. Writer acknowledgement is diagnostic only."
                : isMirror
                ? "Dest-engine COUNT(*) WHERE NOT _deleted. Writer acknowledgement is diagnostic only. Physical COUNT(*) does not drop."
                : isArtifact
                  ? "Independent record count of the written file. Writer acknowledgement is diagnostic only. Gate-8 cell fidelity remains unproven."
                  : "Independent dest-engine read-back. Writer acknowledgement is diagnostic only."
              : isMirror
                ? "Active dest population was not captured. Writer ack is not COUNT(*) WHERE NOT _deleted."
                : isJob
                  ? "Not every stream has a dest-engine ledger. Last-table COUNT(*) and writer ack are not the job."
                  : "Dest COUNT(*) was not captured. Writer ack is not destination proof."}
          </p>
        </div>
      </div>

      <div className="df2-conservation-ledger-compare" aria-label={isMirror ? "Active dest versus writer acknowledgement" : isArtifact ? "Artifact records versus writer acknowledgement" : "Dest COUNT versus writer acknowledgement"}>
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
            <strong>
              {isMirror
                ? "Writer ack disagrees with active dest population"
                : isArtifact
                  ? "Writer ack disagrees with artifact record count"
                  : "Writer ack disagrees with dest COUNT(*)"}
            </strong>
            <span>
              {isMirror
                ? `Active dest holds ${dest.measured ? dest.value : "an unmeasured population"}. Writer counted ${writer.value}. Soft-deleted leftovers are not writer events — acknowledgement never closes conservation.`
                : isArtifact
                  ? `The export artifact holds ${dest.measured ? dest.value : "an unmeasured population"}. Writer counted ${writer.value}. That is the DMS Full Load / MISSING_TARGET hole — writer acknowledgement never closes conservation.`
                  : `Dest holds ${dest.measured ? dest.value : "an unmeasured population"}. Writer counted ${writer.value}. That is the DMS Full Load / MISSING_TARGET hole — writer acknowledgement never closes conservation.`}
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
          <strong>{nextTitle}</strong>
          <span>{nextBody}</span>
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
