import { useEffect, useMemo, useState } from "react";
import {
  fetchContractBreaker,
  fetchContracts,
  resetContractBreaker,
  type DataContractSummary,
} from "../../lib/api";
import { contractBindBlocksRun, isSignedContractStatus } from "../../lib/contractBind";
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
  /** When set, the signed list is limited to this route and can be searched. */
  sourceLabel?: string;
  destLabel?: string;
}

export function ContractBindField({
  idPrefix,
  contractId,
  requireSigned,
  onContractIdChange,
  onRequireSignedChange,
  onBlockReasonChange,
  compact = false,
  sourceLabel = "",
  destLabel = "",
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

  const signedContracts = useMemo(
    () => contracts.filter((c) => isSignedContractStatus(c.status)),
    [contracts],
  );
  const routeContracts = useMemo(() => {
    const tokens = (value: string) => value.toLowerCase().split(/[^a-z0-9._-]+/).filter((part) => part.length > 2);
    const sourceTokens = tokens(sourceLabel);
    const destTokens = tokens(destLabel);
    const query = search.trim().toLowerCase();
    const blobOf = (contract: DataContractSummary) =>
      `${contract.name} ${JSON.stringify(contract.source || {})} ${JSON.stringify(contract.destination || {})}`.toLowerCase();
    const pairMatches = signedContracts.filter((contract) => {
      const blob = blobOf(contract);
      if (query && !blob.includes(query)) return false;
      const sourceHit = sourceTokens.length === 0 || sourceTokens.some((token) => blob.includes(token));
      const destHit = destTokens.length === 0 || destTokens.some((token) => blob.includes(token));
      return sourceHit && destHit;
    });
    const shown = pairMatches.length > 0
      ? pairMatches
      : signedContracts.filter((contract) => !query || blobOf(contract).includes(query));
    return [...shown].sort((a, b) => (b.updated_at || "").localeCompare(a.updated_at || ""));
  }, [signedContracts, sourceLabel, destLabel, search]);
  useEffect(() => {
    if (contractId || routeContracts.length !== 1 || (!sourceLabel && !destLabel)) return;
    onContractIdChange(routeContracts[0].id);
    onRequireSignedChange(true);
  }, [contractId, routeContracts, sourceLabel, destLabel, onContractIdChange, onRequireSignedChange]);
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

  return (
    <div className={compact ? "df2-contract-bind df2-contract-bind--compact" : "df2-contract-bind"}>
      <div className="df2-field">
        <label className="df2-label" htmlFor={`${idPrefix}-contract`}>Contract</label>
        {(sourceLabel || destLabel) && (
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
          {(sourceLabel || destLabel ? routeContracts : signedContracts).map((c) => (
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
