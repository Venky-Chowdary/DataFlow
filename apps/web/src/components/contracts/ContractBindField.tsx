import { useEffect, useMemo, useState } from "react";
import { fetchContracts, type DataContractSummary } from "../../lib/api";
import { contractBindBlocksRun, isSignedContractStatus } from "../../lib/contractBind";

interface ContractBindFieldProps {
  idPrefix: string;
  contractId: string;
  requireSigned: boolean;
  onContractIdChange: (id: string) => void;
  onRequireSignedChange: (require: boolean) => void;
  onBlockReasonChange?: (reason: string) => void;
  compact?: boolean;
}

export function ContractBindField({
  idPrefix,
  contractId,
  requireSigned,
  onContractIdChange,
  onRequireSignedChange,
  onBlockReasonChange,
  compact = false,
}: ContractBindFieldProps) {
  const [contracts, setContracts] = useState<DataContractSummary[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    fetchContracts()
      .then((list) => {
        if (!cancelled) setContracts(list);
      })
      .catch(() => {
        if (!cancelled) setContracts([]);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const signedContracts = useMemo(
    () => contracts.filter((c) => isSignedContractStatus(c.status)),
    [contracts],
  );
  const selected = useMemo(
    () => contracts.find((c) => c.id === contractId) || null,
    [contracts, contractId],
  );
  const blockReason = contractBindBlocksRun({
    contractId,
    requireSigned,
    selectedStatus: selected?.status,
  });

  useEffect(() => {
    onBlockReasonChange?.(blockReason);
  }, [blockReason, onBlockReasonChange]);

  return (
    <div className={compact ? "df2-contract-bind df2-contract-bind--compact" : "df2-contract-bind"}>
      <div className="df2-field">
        <label className="df2-label" htmlFor={`${idPrefix}-contract`}>Contract</label>
        <select
          id={`${idPrefix}-contract`}
          className="df2-input"
          value={contractId}
          onChange={(e) => {
            const next = e.target.value;
            onContractIdChange(next);
            if (next) onRequireSignedChange(true);
          }}
          disabled={loading}
        >
          <option value="">None — no contract enforcement</option>
          {signedContracts.map((c) => (
            <option key={c.id} value={c.id}>
              {c.name} · v{c.version} · SIGNED
            </option>
          ))}
          {selected && !isSignedContractStatus(selected.status) && (
            <option value={selected.id}>
              {selected.name} · v{selected.version} · {selected.status} (sign required)
            </option>
          )}
          {!selected && contractId && (
            <option value={contractId}>{contractId} (not in catalog)</option>
          )}
        </select>
        <span className="df2-field-hint">
          {loading
            ? "Loading contracts…"
            : signedContracts.length === 0
              ? "No signed contracts yet — save + sign one from Validate → Contracts."
              : "Only SIGNED contracts appear here. Drafts must be signed on the Contracts page first."}
        </span>
      </div>
      {contractId && (
        <label className="df2-sched-check">
          <input
            type="checkbox"
            checked={requireSigned}
            onChange={(e) => onRequireSignedChange(e.target.checked)}
          />
          Require signed contract before each run (fail-closed)
        </label>
      )}
      {blockReason && (
        <p className="df2-label-hint df2-dest-sync-warning" role="alert">
          {blockReason}
        </p>
      )}
    </div>
  );
}
