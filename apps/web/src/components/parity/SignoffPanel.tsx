import { useEffect, useId, useState } from "react";
import { Button } from "../ui/Button";
import { CopyIdChip } from "../ui/CopyIdChip";
import { DtIcon } from "../DtIcon";
import {
  badgeClass,
  formatClock,
  formatDateTime,
  roleLabel,
  approvalAuthority,
  invalidatedApprovalsNote,
  shortSignature,
  signoffBlockedReason,
  signoffRows,
  unsignedRoles,
  type ParityReport,
  type ParityTone,
} from "../../lib/parity";

export type ParityBusy =
  | { kind: "refresh" | "explain" | "sign" | "export" | "pack" }
  | { kind: "approve" | "dismiss"; id: string }
  | null;

export interface SignoffPanelProps {
  report: ParityReport;
  busy: ParityBusy;
  /** False hides the sign form and says why (the API refuses regardless). */
  canSign: boolean;
  signDeniedReason?: string;
  onSign: (role: string, note: string) => void;
  onExport: () => void;
  /** Download the signed pack (JSON); the button shows only once the report is signed. */
  onDownloadSigned?: () => void;
  /** Last sign-off or export failure, already phrased for the reader. */
  error?: string;
}

const STATUS: Record<string, { label: string; tone: ParityTone }> = {
  signed: { label: "Signed", tone: "ok" },
  open: { label: "Open for sign-off", tone: "muted" },
  blocked: { label: "Blocked", tone: "warn" },
};

/**
 * Page C: who must sign, who has, why it is blocked, and the evidence pack.
 * Sign-off opens only at zero unexplained differences and a decided verdict
 * (not "unverifiable"); a role signs once and one person signs one role. The API
 * enforces all of it
 * with a 409, and this panel says so before anyone tries.
 */
export function SignoffPanel({ report, busy, canSign, signDeniedReason, onSign, onExport, onDownloadSigned, error }: SignoffPanelProps) {
  const { signoff } = report;
  const rows = signoffRows(signoff);
  const waiting = unsignedRoles(signoff);
  const blockedReason = signoffBlockedReason(report);
  const status = STATUS[signoff.status] ?? STATUS.open;
  const [role, setRole] = useState(waiting[0] ?? "");
  const [note, setNote] = useState("");
  const formId = useId();

  // Keep the chosen role valid as signatures arrive.
  useEffect(() => {
    if (!waiting.includes(role)) setRole(waiting[0] ?? "");
  }, [waiting.join("|")]); // eslint-disable-line react-hooks/exhaustive-deps

  const anyBusy = busy != null;
  const signing = busy?.kind === "sign";
  const exporting = busy?.kind === "export";
  const signed = signoff.status === "signed";
  const blocked = Boolean(blockedReason);

  return (
    <section
      className="df2-parity-signoff"
      aria-labelledby={`${formId}-title`}
      id={`parity-signoff-${report.job_id}`}
      tabIndex={-1}
    >
      <header className="df2-parity-section-head">
        <div>
          <h3 id={`${formId}-title`} className="df2-parity-h3">Sign-off and evidence pack</h3>
          <p className="df2-parity-muted">
            Each approval is written to the audit log with your role in this job's workspace, which must hold the
            authority the role needs. When every required role has signed the same report, the pack is signed with
            the workspace proof key and the report is final.
          </p>
        </div>
        <span className={badgeClass(status.tone)}>{status.label}</span>
      </header>

      <div className="df2-parity-grid2">
        <div className="df2-parity-card">
          <h4 className="df2-parity-h4">Approvals</h4>
          {blocked && (
            <p className="df2-parity-notice is-warn" role="status">
              <DtIcon name="alert" size={14} />
              <span>{blockedReason}</span>
            </p>
          )}
          {invalidatedApprovalsNote(signoff) && (
            <p className="df2-parity-notice is-warn" role="status">
              <DtIcon name="alert" size={14} />
              <span>{invalidatedApprovalsNote(signoff)}</span>
            </p>
          )}
          <ul className="df2-parity-sigs">
            {rows.map((row) => (
              <li key={row.role} className="df2-parity-sig">
                <span>
                  <b>{row.label}</b>
                  {row.approval ? <> · {row.approval.user}</> : null}
                  {row.approval && approvalAuthority(row.approval) ? (
                    <span className="df2-parity-muted"> ({approvalAuthority(row.approval)})</span>
                  ) : null}
                  {row.approval?.note ? <span className="df2-parity-sig-note">{row.approval.note}</span> : null}
                </span>
                {row.approval ? (
                  <span className={badgeClass("ok")} title={formatDateTime(row.approval.at)}>
                    Signed {formatClock(row.approval.at)}
                  </span>
                ) : (
                  <span className={badgeClass("warn")}>Waiting</span>
                )}
              </li>
            ))}
            {rows.length === 0 && <li className="df2-parity-muted">No roles are required to sign this report.</li>}
          </ul>

          {signoff.status === "signed" && signoff.signature ? (
            <div className="df2-parity-signed">
              <p>
                Signed {formatDateTime(signoff.signed_at)}. The exported pack carries this signature, so anyone can
                check it was not changed.
              </p>
              <CopyIdChip id={signoff.signature} label="Signature" className="df2-parity-signature" />
            </div>
          ) : canSign ? (
            waiting.length > 0 && (
              <form
                className="df2-parity-sign-form"
                onSubmit={(e) => {
                  e.preventDefault();
                  if (!blocked && role && !anyBusy) onSign(role, note);
                }}
              >
                <div className="df2-field">
                  <label className="df2-label" htmlFor={`${formId}-role`}>Sign as</label>
                  <select
                    id={`${formId}-role`}
                    className="df2-select"
                    value={role}
                    onChange={(e) => setRole(e.target.value)}
                    disabled={blocked || anyBusy}
                  >
                    {waiting.map((r) => (
                      <option key={r} value={r}>{roleLabel(r)}</option>
                    ))}
                  </select>
                </div>
                <div className="df2-field">
                  <label className="df2-label" htmlFor={`${formId}-note`}>Note (optional)</label>
                  <textarea
                    id={`${formId}-note`}
                    className="df2-input df2-parity-note-input"
                    rows={2}
                    maxLength={500}
                    value={note}
                    onChange={(e) => setNote(e.target.value)}
                    disabled={blocked || anyBusy}
                    placeholder="What you checked before signing"
                  />
                </div>
                <div className="df2-parity-actions">
                  <Button
                    type="submit"
                    variant="primary"
                    size="sm"
                    disabled={blocked || anyBusy || !role}
                    loading={signing}
                    loadingLabel="Signing…"
                    title={blocked ? blockedReason : undefined}
                  >
                    Sign as {roleLabel(role || waiting[0])}
                  </Button>
                </div>
              </form>
            )
          ) : (
            <p className="df2-parity-muted">{signDeniedReason || "You don't have permission to sign this report."}</p>
          )}
          {error && (
            <p className="df2-parity-notice is-danger" role="alert">
              <DtIcon name="alert" size={14} />
              <span>{error}</span>
            </p>
          )}
        </div>

        <div className="df2-parity-card">
          <h4 className="df2-parity-h4">What the pack contains</h4>
          <ul className="df2-parity-list">
            <li>Verdict, parity per table and the confidence statement</li>
            <li>Every difference with its cause and how it was resolved</li>
            <li>Transforms applied, PII protection proof, schema changes</li>
            <li>Approvals with names, roles and times</li>
            <li>
              {signoff.signature ? (
                <>
                  Signature <code className="df2-mono" title={signoff.signature}>{shortSignature(signoff.signature)}</code>
                </>
              ) : (
                "The signature, once every required role has signed"
              )}
            </li>
          </ul>
          <div className="df2-parity-actions">
            <Button
              size="sm"
              onClick={onExport}
              disabled={anyBusy}
              loading={exporting}
              loadingLabel="Exporting…"
              leadingIcon={<DtIcon name="download" size={14} />}
            >
              Export Markdown
            </Button>
            {signed && onDownloadSigned && (
              <Button
                size="sm"
                variant="primary"
                onClick={onDownloadSigned}
                disabled={anyBusy}
                loading={busy?.kind === "pack"}
                loadingLabel="Downloading…"
                leadingIcon={<DtIcon name="download" size={14} />}
                title="The signed JSON pack; POST /api/v1/transfer/proof-pack/verify checks its signature."
              >
                Download signed pack
              </Button>
            )}
          </div>
        </div>
      </div>
    </section>
  );
}
