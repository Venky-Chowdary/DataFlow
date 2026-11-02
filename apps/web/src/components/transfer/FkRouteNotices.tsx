import { DtIcon } from "../DtIcon";
import {
  FK_ORPHAN_ACK_TEXT,
  fkOrphansNeedAcknowledgment,
  fkRouteNotices,
} from "../../lib/foreignKeysMode";

/**
 * Round 11 route notices on the Validate dashboard: the G22 "parent not at the
 * destination" warning, catalog probes that stopped at their budget, and the
 * explicit FK-risk acknowledgment when sample orphans block the run.
 */
export function FkRouteNotices({
  preflight,
  fkRiskAcknowledged = false,
  onAcknowledgeFkRisk,
  running = false,
}: {
  preflight: unknown;
  fkRiskAcknowledged?: boolean;
  onAcknowledgeFkRisk?: () => void;
  running?: boolean;
}) {
  const notices = fkRouteNotices(preflight);
  const needsAck = fkOrphansNeedAcknowledgment(preflight);
  if (!notices.length && !needsAck) return null;
  return (
    <div className="df2-vd-honesty df2-vd-fk-route" aria-label="Foreign key and catalog notices">
      {notices.map((n, i) => (
        <div key={`${n.id}-${i}`} className="df2-vd-honesty-note" role="note">
          <DtIcon name="alert" size={14} />
          <span>
            <strong>{n.title}.</strong> {n.message}
          </span>
        </div>
      ))}
      {needsAck && (
        <label className="df2-vd-honesty-toggle" htmlFor="df2-vd-fk-risk-ack">
          <input
            id="df2-vd-fk-risk-ack"
            type="checkbox"
            checked={fkRiskAcknowledged}
            disabled={running || fkRiskAcknowledged || !onAcknowledgeFkRisk}
            onChange={(e) => {
              if (e.target.checked) onAcknowledgeFkRisk?.();
            }}
          />
          <span>{FK_ORPHAN_ACK_TEXT}</span>
        </label>
      )}
    </div>
  );
}
