/**
 * Confirmation surface for a staged Pilot mutation.
 *
 * The backend stages the real TransferRequest (or connector payload) in a
 * server-side ack ledger and returns only a redacted preview. This card is the
 * operator's last look before Confirm — so it has to show the things that
 * decide the answer: the route, the sync mode, how many casts are lossy, and
 * whether the destination will be overwritten. A bare "Confirm / Cancel" pair
 * with no preview made an overwrite look identical to a benign append.
 */

import { Button } from "../ui/Button";
import { contractBindFromPreview } from "../../lib/contractBind";
import { breakerLabel } from "../../lib/contractBreakerUi";
import {
  isDestructiveSchedulePreview,
  scheduleConfirmBind,
  scheduleConfirmBlocksRun,
  schedulePreviewFromPayload,
} from "../../lib/pilotScheduleConfirm";
import type {
  CopilotPendingAction,
  PilotGate,
  PilotTransferPlan,
  PilotTransferPreview,
  PilotTypeConversion,
} from "../../lib/api";

type Props = {
  action: CopilotPendingAction;
  busy?: boolean;
  onConfirm: () => void;
  onCancel: () => void;
};

const GATE_ICON: Record<string, string> = {
  pass: "✓",
  block: "✗",
  warn: "!",
  skip: "–",
};

function asPlan(payload: Record<string, unknown> | undefined): PilotTransferPlan | null {
  if (!payload) return null;
  const plan = (payload.plan && typeof payload.plan === "object"
    ? payload.plan
    : payload) as PilotTransferPlan;
  if (!plan || typeof plan !== "object") return null;
  if (!plan.source && !plan.destination && !plan.preflight) return null;
  return plan;
}

function asPreview(payload: Record<string, unknown> | undefined): PilotTransferPreview | null {
  const preview = payload?.preview;
  if (!preview || typeof preview !== "object") return null;
  return preview as PilotTransferPreview;
}

function GateStrip({ gates }: { gates: PilotGate[] }) {
  if (!gates.length) return null;
  return (
    <div className="df2-pilot-confirm-gates" aria-label="Preflight gates">
      {gates.map((g) => {
        const status = String(g.status || "").toLowerCase();
        return (
          <span
            key={g.id}
            className={`df2-pilot-confirm-gate is-${status || "unknown"}`}
            title={g.message || g.id}
          >
            <span aria-hidden>{GATE_ICON[status] || "?"}</span>
            {g.id}
          </span>
        );
      })}
    </div>
  );
}

function LossyList({ items }: { items: PilotTypeConversion[] }) {
  if (!items.length) return null;
  return (
    <ul className="df2-pilot-confirm-lossy">
      {items.slice(0, 5).map((c, i) => (
        <li key={`${c.source_column}-${c.target_column}-${i}`}>
          <code>{c.source_column}</code>
          {" "}
          <span className="df2-pilot-confirm-cast">
            {c.from_type} → {c.to_type}
          </span>
          {c.target_column && c.target_column !== c.source_column ? (
            <> on <code>{c.target_column}</code></>
          ) : null}
        </li>
      ))}
      {items.length > 5 ? (
        <li className="df2-pilot-confirm-more">+{items.length - 5} more</li>
      ) : null}
    </ul>
  );
}

function TransferBody({
  action,
  plan,
  preview,
}: {
  action: CopilotPendingAction;
  plan: PilotTransferPlan | null;
  preview: PilotTransferPreview | null;
}) {
  const source = preview?.source
    || (plan?.source
      ? `${plan.source.connector_name || "?"}.${plan.source.table || "?"}`
      : "—");
  const destination = preview?.destination
    || (plan?.destination
      ? `${plan.destination.connector_name || "?"}.${plan.destination.table || "?"}`
      : "—");
  const syncMode = preview?.sync_mode || plan?.sync_mode || "full_refresh_append";
  const mapped = preview?.mapped_columns ?? plan?.mapped_count;
  const destExists = preview?.destination_table_exists
    ?? plan?.destination?.table_exists;
  const readiness = preview?.readiness_score ?? plan?.preflight?.readiness_score;
  const runId = preview?.preflight_run_id || plan?.preflight?.run_id;
  const lossy = plan?.lossy_conversions || [];
  const unmapped = preview?.unmapped_source_columns
    || plan?.unmapped_source_columns
    || [];
  const contractBind = contractBindFromPreview(preview);
  const gates = plan?.preflight?.gates || [];
  const destructive = Boolean(
    action.destructive
    || syncMode === "full_refresh_overwrite"
    || payloadDestructive(action.payload),
  );

  return (
    <>
      <div className="df2-pilot-confirm-route" aria-label="Transfer route">
        <div className="df2-pilot-confirm-end">
          <span className="df2-pilot-confirm-end-label">From</span>
          <strong>{source}</strong>
        </div>
        <span className="df2-pilot-confirm-arrow" aria-hidden>→</span>
        <div className="df2-pilot-confirm-end">
          <span className="df2-pilot-confirm-end-label">To</span>
          <strong>{destination}</strong>
        </div>
      </div>

      <dl className="df2-pilot-confirm-meta">
        <div>
          <dt>Sync</dt>
          <dd>
            <code>{syncMode}</code>
            {destructive ? (
              <span className="df2-pilot-confirm-badge is-danger">overwrites destination</span>
            ) : null}
          </dd>
        </div>
        {typeof mapped === "number" ? (
          <div>
            <dt>Mapped</dt>
            <dd>
              <strong>{mapped}</strong>
              {typeof plan?.source?.column_count === "number"
                ? ` of ${plan.source.column_count} source columns`
                : " columns"}
              {destExists === false ? " · destination will be created" : null}
              {destExists === true ? " · destination exists" : null}
            </dd>
          </div>
        ) : null}
        {typeof readiness === "number" ? (
          <div>
            <dt>Preflight</dt>
            <dd>
              <strong>{readiness}%</strong>
              {plan?.preflight?.passed_count != null && plan?.preflight?.total_gates != null
                ? ` · ${plan.preflight.passed_count}/${plan.preflight.total_gates} gates`
                : null}
              {runId ? <> · <code className="df2-pilot-confirm-runid">{runId}</code></> : null}
            </dd>
          </div>
        ) : null}
        {contractBind.contractId ? (
          <div>
            <dt>Contract</dt>
            <dd>
              <code>{contractBind.contractId}</code>
              {contractBind.requireSigned
                ? " · Confirm fails closed unless SIGNED"
                : null}
            </dd>
          </div>
        ) : null}
      </dl>

      <GateStrip gates={gates} />

      {lossy.length > 0 ? (
        <div className="df2-pilot-confirm-section is-warn">
          <p className="df2-pilot-confirm-section-title">
            {lossy.length} lossy cast{lossy.length === 1 ? "" : "s"} — data changes shape on write
          </p>
          <LossyList items={lossy} />
        </div>
      ) : null}

      {unmapped.length > 0 ? (
        <div className="df2-pilot-confirm-section">
          <p className="df2-pilot-confirm-section-title">
            {unmapped.length} source column{unmapped.length === 1 ? "" : "s"} have no destination
          </p>
          <p className="df2-pilot-confirm-muted">
            {unmapped.slice(0, 8).map((c, i) => (
              <span key={c}>
                {i > 0 ? ", " : null}
                <code>{c}</code>
              </span>
            ))}
            {unmapped.length > 8 ? "…" : null}
          </p>
        </div>
      ) : null}

      {destructive ? (
        <p className="df2-pilot-confirm-danger" role="alert">
          This overwrites the destination table. Nothing moves until you confirm.
        </p>
      ) : (
        <p className="df2-pilot-confirm-muted">
          Nothing moves until you confirm. Credentials never leave the server.
        </p>
      )}
    </>
  );
}

function payloadDestructive(payload: Record<string, unknown> | undefined): boolean {
  if (!payload) return false;
  if (payload.destructive === true) return true;
  const preview = asPreview(payload);
  return preview?.sync_mode === "full_refresh_overwrite";
}

function ScheduleBody({
  action,
  payload,
}: {
  action: CopilotPendingAction;
  payload: Record<string, unknown> | undefined;
}) {
  const preview = schedulePreviewFromPayload(payload);
  const bind = scheduleConfirmBind(preview);
  const block = scheduleConfirmBlocksRun(preview);
  const destructive = Boolean(
    action.destructive || isDestructiveSchedulePreview(preview),
  );
  const source = preview.source_table || "—";
  const destination = preview.dest_table || "—";

  return (
    <>
      <div className="df2-pilot-confirm-route" aria-label="Pipeline route">
        <div className="df2-pilot-confirm-end">
          <span className="df2-pilot-confirm-end-label">From</span>
          <strong>{source}</strong>
        </div>
        <span className="df2-pilot-confirm-arrow" aria-hidden>→</span>
        <div className="df2-pilot-confirm-end">
          <span className="df2-pilot-confirm-end-label">To</span>
          <strong>{destination}</strong>
        </div>
      </div>

      <dl className="df2-pilot-confirm-meta">
        {preview.sync_mode ? (
          <div>
            <dt>Sync</dt>
            <dd>
              <code>{preview.sync_mode}</code>
              {destructive ? (
                <span className="df2-pilot-confirm-badge is-danger">overwrites destination</span>
              ) : null}
            </dd>
          </div>
        ) : null}
        {bind.contractId ? (
          <div>
            <dt>Contract</dt>
            <dd>
              <code>{bind.contractId}</code>
              {bind.requireSigned
                ? " · Confirm fails closed unless SIGNED"
                : null}
              {bind.breakerState ? (
                <>
                  {" · "}
                  {breakerLabel(bind.breakerState) || bind.breakerState}
                </>
              ) : null}
            </dd>
          </div>
        ) : null}
      </dl>

      {block ? (
        <p className="df2-pilot-confirm-danger" role="alert">{block}</p>
      ) : destructive ? (
        <p className="df2-pilot-confirm-danger" role="alert">
          This overwrites the destination table. Nothing moves until you confirm.
        </p>
      ) : (
        <p className="df2-pilot-confirm-muted">
          Immediate run only — the regular cadence does not change. Nothing moves until you confirm.
        </p>
      )}
    </>
  );
}

function ConnectorBody({ payload }: { payload: Record<string, unknown> | undefined }) {
  const preview = (payload?.preview && typeof payload.preview === "object"
    ? payload.preview
    : {}) as Record<string, unknown>;
  const name = String(preview.name || "Connector");
  const type = String(preview.type || "connector");
  const host = String(preview.host || "");
  const database = String(preview.database || "");
  return (
    <dl className="df2-pilot-confirm-meta">
      <div>
        <dt>Name</dt>
        <dd><strong>{name}</strong></dd>
      </div>
      <div>
        <dt>Type</dt>
        <dd><code>{type}</code></dd>
      </div>
      {host ? (
        <div>
          <dt>Host</dt>
          <dd>{host}</dd>
        </div>
      ) : null}
      {database ? (
        <div>
          <dt>Database</dt>
          <dd>{database}</dd>
        </div>
      ) : null}
    </dl>
  );
}

export function PilotConfirmCard({ action, busy, onConfirm, onCancel }: Props) {
  const isTransfer = action.type === "start_transfer";
  const isConnector = action.type === "create_connector";
  const isSchedule = action.type === "run_schedule";
  const plan = isTransfer ? asPlan(action.payload) : null;
  const preview = isTransfer ? asPreview(action.payload) : null;
  const schedulePreview = isSchedule ? schedulePreviewFromPayload(action.payload) : null;
  const scheduleBlock = schedulePreview ? scheduleConfirmBlocksRun(schedulePreview) : "";
  const destructive = Boolean(
    action.destructive
    || (isTransfer && (
      preview?.sync_mode === "full_refresh_overwrite"
      || payloadDestructive(action.payload)
    ))
    || (isSchedule && schedulePreview && isDestructiveSchedulePreview(schedulePreview)),
  );

  return (
    <div
      className={`df2-pilot-confirm${destructive ? " is-destructive" : ""}`}
      role="group"
      aria-label={action.label || "Confirm this change"}
    >
      <div className="df2-pilot-confirm-head">
        <span className="df2-pilot-confirm-kicker">
          {isTransfer
            ? "Transfer ready to run"
            : isSchedule
              ? "Pipeline ready to run"
              : isConnector
                ? "Save connector"
                : "Needs your confirmation"}
        </span>
        <strong className="df2-pilot-confirm-title">{action.label || action.type}</strong>
      </div>

      {isTransfer ? (
        <TransferBody action={action} plan={plan} preview={preview} />
      ) : isSchedule ? (
        <ScheduleBody action={action} payload={action.payload} />
      ) : isConnector ? (
        <ConnectorBody payload={action.payload} />
      ) : (
        <p className="df2-pilot-confirm-muted">
          Review the change above, then confirm. Nothing happens until you do.
        </p>
      )}

      <div className="df2-pilot-confirm-actions">
        <Button
          variant={destructive ? "danger" : "primary"}
          size="sm"
          loading={busy}
          loadingLabel="Starting…"
          disabled={Boolean(scheduleBlock)}
          onClick={onConfirm}
        >
          {destructive
            ? "Overwrite & run"
            : isTransfer
              ? "Run transfer"
              : isSchedule
                ? "Run pipeline"
                : "Confirm"}
        </Button>
        <Button variant="ghost" size="sm" disabled={busy} onClick={onCancel}>
          Cancel
        </Button>
      </div>
    </div>
  );
}
