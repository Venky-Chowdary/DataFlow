import { useEffect, useMemo, useState } from "react";
import {
  fetchContractBreaker,
  fetchContracts,
  resetContractBreaker,
  type DataContractSummary,
} from "../../lib/api";
import {
  contractBindBlocksRun,
  contractMatchesRoute,
  isSignedContractStatus,
  type ContractRoute,
} from "../../lib/contractBind";
import { contractBreakerBlocksRun } from "../../lib/contractBreakerUi";
import { Button } from "../ui/Button";

interface ContractBindFieldProps {
  idPrefix: string;
  contractId: string;
  requireSigned: boolean;
  onContractIdChange: (id: string) => void;
  onRequireSignedChange: (require: boolean) => void;
  onBlockReasonChange?: (reason: string) => void;
  compact?: boolean;
  /** When set, the list is signed contracts for this source and destination. */
  route?: ContractRoute | null;
}

export function ContractBindField({
  idPrefix,
  contractId,
  requireSigned,
  onContractIdChange,
  onRequireSignedChange,
  onBlockReasonChange,
  compact = false,
  route = null,
}: ContractBindFieldProps) {
  const [contracts, setContracts] = useState<DataContractSummary[]>([]);
  const [loading, setLoading] = useState(false);
  const [breakerState, setBreakerState] = useState("");
  const [search, setSearch] = useState("");
  const [resettingBreaker, setResettingBreaker] = useState(false);

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

  const signedContracts = useMemo(() => {
    const signed = contracts.filter((c) => isSignedContractStatus(c.status));
    if (!route) return signed;
    return signed.filter((c) => contractMatchesRoute(c, route));
  }, [contracts, route]);
  const visibleContracts = useMemo(() => {
    const query = search.trim().toLowerCase();
    if (!query) return signedContracts;
    return signedContracts.filter((contract) => {
      const blob = `${contract.name} ${JSON.stringify(contract.source || {})} ${JSON.stringify(contract.destination || {})}`.toLowerCase();
      return blob.includes(query);
    });
  }, [signedContracts, search]);
  useEffect(() => {
    if (contractId || !route || search.trim() || visibleContracts.length !== 1) return;
    onContractIdChange(visibleContracts[0].id);
    onRequireSignedChange(true);
  }, [contractId, route, search, visibleContracts, onContractIdChange, onRequireSignedChange]);
  const selected = useMemo(
    () => contracts.find((c) => c.id === contractId) || null,
    [contracts, contractId],
  );
  useEffect(() => {
    if (!contractId) {
      setBreakerState("");
      return;
    }
    let cancelled = false;
    fetchContractBreaker(contractId)
      .then((b) => {
        if (!cancelled) setBreakerState(String(b.state || ""));
      })
      .catch(() => {
        if (!cancelled) setBreakerState("");
      });
    return () => {
      cancelled = true;
    };
  }, [contractId]);

  const bindBlock = contractBindBlocksRun({
    contractId,
    requireSigned,
    selectedStatus: selected?.status,
  });
  const breakerBlock = contractBreakerBlocksRun(breakerState);
  const blockReason = bindBlock || breakerBlock;

  useEffect(() => {
    onBlockReasonChange?.(blockReason);
  }, [blockReason, onBlockReasonChange]);

  // Nothing to choose: a picker whose only option is "None" is chrome, not a decision.
  if (route && !loading && !contractId && signedContracts.length === 0) {
    return (
      <div className="df2-contract-bind df2-contract-bind--compact">
        <p className="df2-field-hint" role="status">
          <strong>Contract</strong> · No signed contract matches this source and destination.
          {" "}Save contract here, then sign it on Contracts to enforce it on future runs.
        </p>
      </div>
    );
  }

  return (
    <div className={compact ? "df2-contract-bind df2-contract-bind--compact" : "df2-contract-bind"}>
      <div className="df2-field">
        <label className="df2-label" htmlFor={`${idPrefix}-contract`}>Contract</label>
        {route && (signedContracts.length > 5 || search.trim()) && (
          <input
            className="df2-input"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Search contracts for this route"
            aria-label="Search contracts"
          />
        )}
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
          {visibleContracts.map((c) => (
            <option key={c.id} value={c.id}>
              {c.name} · v{c.version} · SIGNED
            </option>
          ))}
          {selected && isSignedContractStatus(selected.status)
            && !signedContracts.some((c) => c.id === selected.id) && (
            <option value={selected.id}>
              {selected.name} · v{selected.version} · SIGNED (different route)
            </option>
          )}
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
              ? (route
                ? "No signed contract matches this source and destination."
                : "No signed contracts yet — save + sign one from Validate → Contracts.")
              : (route
                ? "Signed contracts for this source and destination."
                : "Only SIGNED contracts appear here. Drafts must be signed on the Contracts page first.")}
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
      {breakerBlock && contractId && (
        <Button
          size="sm"
          variant="ghost"
          loading={resettingBreaker}
          loadingLabel="Resetting…"
          onClick={() => {
            void (async () => {
              setResettingBreaker(true);
              try {
                const next = await resetContractBreaker(contractId);
                setBreakerState(String(next.state || "closed"));
              } catch {
                /* parent toast / operator retries */
              } finally {
                setResettingBreaker(false);
              }
            })();
          }}
        >
          Reset breaker
        </Button>
      )}
    </div>
  );
}
