import { useCallback, useEffect, useRef, useState } from "react";
import { Button } from "../components/ui/Button";
import { EmptyState } from "../components/ui/EmptyState";
import { PageFrame } from "../components/ui/PageFrame";
import { PageShell } from "../components/ui/PageShell";
import { PageContextBar } from "../components/ui/PageContextBar";
import { PageToolbar } from "../components/ui/PageToolbar";
import { SectionLoader } from "../components/LoadingState";
import { DtIcon } from "../components/DtIcon";
import {
  CONTRACT_TABS,
  ContractDetailDrawer,
  type ContractTab,
} from "../components/ContractDetailDrawer";
import { useToast } from "../components/Toast";
import {
  DataContractSummary,
  deprecateContract,
  exportContract,
  fetchContractBreaker,
  fetchContracts,
  resetContractBreaker,
  signContract,
} from "../lib/api";

const CONTRACTS_CHANGED = "df2:contracts-changed";
const LAST_CONTRACT_KEY = "df2.last-saved-contract";

function statusBadge(status: string) {
  const s = (status || "").toLowerCase();
  if (s === "signed") return { cls: "df2-badge-live", label: "Signed" };
  if (s === "broken") return { cls: "df2-badge-error", label: "Broken" };
  if (s === "deprecated") return { cls: "df2-badge-muted", label: "Deprecated" };
  return { cls: "df2-badge-warn", label: "Draft" };
}

function readOptimisticContract(): DataContractSummary | null {
  try {
    const raw = sessionStorage.getItem(LAST_CONTRACT_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as DataContractSummary;
    return parsed?.id ? parsed : null;
  } catch {
    return null;
  }
}

function upsertContract(list: DataContractSummary[], next: DataContractSummary): DataContractSummary[] {
  const rest = list.filter((c) => c.id !== next.id);
  return [next, ...rest];
}

export function ContractsPage({ active = true }: { active?: boolean }) {
  const { toast } = useToast();
  const toastRef = useRef(toast);
  toastRef.current = toast;

  const [contracts, setContracts] = useState<DataContractSummary[]>(() => {
    const optimistic = readOptimisticContract();
    return optimistic ? [optimistic] : [];
  });
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [busyId, setBusyId] = useState<string | null>(null);
  const [breakers, setBreakers] = useState<Record<string, string>>({});
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [contractTab, setContractTab] = useState<ContractTab>(CONTRACT_TABS[0]);
  const pendingReload = useRef(false);
  const activeRef = useRef(active);
  activeRef.current = active;

  const load = useCallback(async () => {
    setLoading(true);
    pendingReload.current = false;
    try {
      const rows = await fetchContracts();
      const optimistic = readOptimisticContract();
      const merged = optimistic ? upsertContract(rows, optimistic) : rows;
      setContracts(merged);
      setLoadError("");
      const map: Record<string, string> = {};
      await Promise.all(
        merged.slice(0, 40).map(async (c) => {
          try {
            const b = await fetchContractBreaker(c.id);
            map[c.id] = b.state;
          } catch {
            /* breaker optional */
          }
        }),
      );
      setBreakers(map);
    } catch (e) {
      const message = (e as Error).message || "Could not load contracts";
      setLoadError(message);
      // Keep any optimistic/local rows so a save still appears if list fetch fails.
      setContracts((prev) => {
        const optimistic = readOptimisticContract();
        if (optimistic) return upsertContract(prev, optimistic);
        return prev;
      });
      toastRef.current({ title: "Contracts", message, tone: "error" });
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!active) return;
    void load();
  }, [active, load]);

  useEffect(() => {
    const onChanged = (event: Event) => {
      const detail = (event as CustomEvent<{ contract?: DataContractSummary; id?: string }>).detail;
      const contract = detail?.contract;
      if (contract?.id) {
        try {
          sessionStorage.setItem(LAST_CONTRACT_KEY, JSON.stringify(contract));
        } catch {
          /* ignore */
        }
        setContracts((prev) => upsertContract(prev, contract));
        setLoadError("");
      }
      if (activeRef.current) {
        void load();
      } else {
        pendingReload.current = true;
      }
    };
    window.addEventListener(CONTRACTS_CHANGED, onChanged);
    return () => window.removeEventListener(CONTRACTS_CHANGED, onChanged);
  }, [load]);

  useEffect(() => {
    if (active && pendingReload.current) void load();
  }, [active, load]);

  const signed = contracts.filter((c) => c.status === "signed").length;
  const drafts = contracts.filter((c) => c.status === "draft").length;
  const broken = contracts.filter((c) => c.status === "broken").length;
  const selectedContract = contracts.find((c) => c.id === selectedId) ?? null;

  useEffect(() => {
    if (!selectedId) return;
    if (!contracts.some((c) => c.id === selectedId)) {
      setSelectedId(null);
      setDrawerOpen(false);
    }
  }, [contracts, selectedId]);

  const openDrawer = (id: string) => {
    setSelectedId(id);
    setContractTab("Overview");
    setDrawerOpen(true);
  };

  const closeDrawer = () => setDrawerOpen(false);

  const run = async (id: string, fn: () => Promise<unknown>, ok: string) => {
    setBusyId(id);
    try {
      await fn();
      toast({ title: ok, tone: "success" });
      await load();
    } catch (e) {
      toast({ title: "Action failed", message: (e as Error).message, tone: "error" });
    } finally {
      setBusyId(null);
    }
  };

  const exportOne = async (c: DataContractSummary) => {
    try {
      const blob = await exportContract(c.id);
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `${c.name || c.id}.yaml`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      toast({ title: "Export failed", message: (e as Error).message, tone: "error" });
    }
  };

  return (
    <PageShell
      className="df2-page-contracts"
      title="Contracts"
      kicker="Platform"
      description="Signed schema agreements that gate transfers and detect drift."
      actions={
        <Button size="sm" onClick={() => void load()} leadingIcon={<DtIcon name="activity" size={14} />}>
          Refresh
        </Button>
      }
    >
      <PageFrame className="df2-contracts-workspace">
        {loadError && contracts.length === 0 ? (
          <div className="df2-alert df2-alert-error" role="alert">
            <DtIcon name="alert" size={18} />
            <div>
              <strong>Could not load contracts</strong>
              <p>{loadError}</p>
            </div>
            <Button size="sm" onClick={() => void load()}>
              Retry
            </Button>
          </div>
        ) : null}

        {loading && contracts.length === 0 ? (
          <SectionLoader title="Loading contracts…" />
        ) : contracts.length === 0 ? (
          <EmptyState
            page
            icon="shield"
            title="No data contracts yet"
            description="A contract is a saved schema agreement (source → destination mappings + quality gates). In Transfer Studio → Validate, click Save as contract — drafts appear here even if Validate is still blocked. Sign when gates pass."
          />
        ) : (
          <>
            <PageContextBar
              ariaLabel="Contracts summary"
              stats={[
                { label: "Contracts", value: contracts.length, icon: "shield" },
                { label: "Signed", value: signed, icon: "check", tone: signed > 0 ? "ok" : "muted" },
                { label: "Draft", value: drafts, icon: "file", tone: "muted" },
                { label: "Broken", value: broken, icon: "alert", tone: broken > 0 ? "danger" : "muted" },
              ]}
            />
            <PageToolbar
              actions={
                <Button size="sm" onClick={() => void load()} leadingIcon={<DtIcon name="activity" size={14} />}>
                  Refresh
                </Button>
              }
            />
            <div className="df2-contract-rows" role="list" aria-label="Data contracts">
              <div className="df2-contract-rows-head" aria-hidden>
                <span className="df2-contract-rows-head-name">Contract</span>
                <span>Version</span>
                <span>Schema</span>
                <span>Status</span>
                <span />
              </div>
              {contracts.map((c) => {
                const badge = statusBadge(c.status);
                const breaker = breakers[c.id];
                const selected = drawerOpen && selectedId === c.id;
                return (
                  <div
                    key={c.id}
                    className={`df2-contract-row df2-card-interactive${selected ? " selected" : ""}`}
                    onClick={() => openDrawer(c.id)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        openDrawer(c.id);
                      }
                    }}
                    role="button"
                    tabIndex={0}
                    aria-current={selected || undefined}
                  >
                    <span
                      className={`df2-health-dot ${c.status === "signed" ? "ok" : c.status === "broken" ? "err" : ""}`}
                      aria-hidden
                    />
                    <div className="df2-contract-row-identity">
                      <span className="df2-contract-row-name" title={c.name}>{c.name}</span>
                      <span className="df2-contract-row-meta">
                        {breaker ? `Breaker ${breaker}` : "Schema agreement"}
                      </span>
                    </div>
                    <span className="df2-contract-row-version">v{c.version}</span>
                    <span className="df2-contract-row-schema">
                      {c.columns?.length || 0} cols · {c.mappings?.length || 0} maps
                    </span>
                    <span className={`df2-badge ${badge.cls}`}>{badge.label}</span>
                    <div className="df2-contract-row-quick" onClick={(e) => e.stopPropagation()}>
                      <button
                        type="button"
                        className="df2-contract-row-open"
                        onClick={() => openDrawer(c.id)}
                        aria-label={`Open ${c.name} details`}
                      >
                        <DtIcon name="chevron-right" size={16} />
                      </button>
                    </div>
                  </div>
                );
              })}
            </div>

            <ContractDetailDrawer
              open={drawerOpen && Boolean(selectedContract)}
              contract={selectedContract}
              tab={contractTab}
              setTab={setContractTab}
              breakerHint={selectedContract ? breakers[selectedContract.id] : undefined}
              busy={Boolean(selectedContract && busyId === selectedContract.id)}
              onClose={closeDrawer}
              onSign={() =>
                selectedContract
                && void run(selectedContract.id, () => signContract(selectedContract.id), "Contract signed")
              }
              onDeprecate={() =>
                selectedContract
                && void run(selectedContract.id, () => deprecateContract(selectedContract.id), "Contract deprecated")
              }
              onResetBreaker={() =>
                selectedContract
                && void run(selectedContract.id, () => resetContractBreaker(selectedContract.id), "Breaker reset")
              }
              onExport={() => selectedContract && void exportOne(selectedContract)}
            />
          </>
        )}
      </PageFrame>
    </PageShell>
  );
}
