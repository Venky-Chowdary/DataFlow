import { useCallback, useEffect, useRef, useState } from "react";
import { Button } from "../components/ui/Button";
import { EmptyState } from "../components/ui/EmptyState";
import { PageFrame } from "../components/ui/PageFrame";
import { PageShell } from "../components/ui/PageShell";
import { PageContextBar } from "../components/ui/PageContextBar";
import { PageToolbar } from "../components/ui/PageToolbar";
import { SectionLoader } from "../components/LoadingState";
import { DtIcon } from "../components/DtIcon";
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

function statusBadge(status: string) {
  const s = (status || "").toLowerCase();
  if (s === "signed") return { cls: "df2-badge-live", label: "Signed" };
  if (s === "broken") return { cls: "df2-badge-error", label: "Broken" };
  if (s === "deprecated") return { cls: "df2-badge-muted", label: "Deprecated" };
  return { cls: "df2-badge-warn", label: "Draft" };
}

export function ContractsPage({ active = true }: { active?: boolean }) {
  const { toast } = useToast();
  const [contracts, setContracts] = useState<DataContractSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [busyId, setBusyId] = useState<string | null>(null);
  const [breakers, setBreakers] = useState<Record<string, string>>({});
  const pendingReload = useRef(false);

  const load = useCallback(async () => {
    setLoading(true);
    pendingReload.current = false;
    try {
      const rows = await fetchContracts();
      setContracts(rows);
      const map: Record<string, string> = {};
      await Promise.all(
        rows.slice(0, 40).map(async (c) => {
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
      toast({ title: "Contracts", message: (e as Error).message, tone: "error" });
      setContracts([]);
    } finally {
      setLoading(false);
    }
  }, [toast]);

  // Keep-alive screens do not remount — reload whenever Contracts becomes visible
  // and whenever Transfer Studio saves a new draft (even if the save event
  // arrives a tick before `active` flips true).
  useEffect(() => {
    if (!active) return;
    void load();
  }, [active, load]);

  useEffect(() => {
    const onChanged = () => {
      if (active) {
        void load();
      } else {
        pendingReload.current = true;
      }
    };
    window.addEventListener("df2:contracts-changed", onChanged);
    return () => window.removeEventListener("df2:contracts-changed", onChanged);
  }, [active, load]);

  useEffect(() => {
    if (active && pendingReload.current) void load();
  }, [active, load]);

  const signed = contracts.filter((c) => c.status === "signed").length;
  const drafts = contracts.filter((c) => c.status === "draft").length;
  const broken = contracts.filter((c) => c.status === "broken").length;

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

  return (
    <PageShell
      fit
      className="df2-page-contracts"
      title="Contracts"
      kicker="Platform"
      description="Signed schema agreements that gate transfers and detect drift."
    >
      <PageFrame className="df2-contracts-workspace">
        {loading ? (
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
            <div className="df2-contract-list">
              {contracts.map((c) => {
                const badge = statusBadge(c.status);
                const breaker = breakers[c.id];
                const showReset = c.status === "broken" || breaker === "open" || breaker === "half_open";
                return (
                  <article key={c.id} className="df2-contract-card">
                    <header className="df2-contract-card-head">
                      <div>
                        <h3 className="df2-contract-name">{c.name}</h3>
                        <p className="df2-contract-meta">
                          v{c.version} · {c.columns?.length || 0} columns · {c.mappings?.length || 0} mappings
                          {breaker ? ` · breaker ${breaker}` : ""}
                        </p>
                      </div>
                      <span className={`df2-badge ${badge.cls}`}>{badge.label}</span>
                    </header>
                    <div className="df2-contract-actions">
                      {(c.status === "draft" || c.status === "broken") && (
                        <Button
                          size="sm"
                          variant="primary"
                          disabled={busyId === c.id}
                          onClick={() => void run(c.id, () => signContract(c.id), "Contract signed")}
                        >
                          Sign
                        </Button>
                      )}
                      {c.status !== "deprecated" && (
                        <Button
                          size="sm"
                          disabled={busyId === c.id}
                          onClick={() => void run(c.id, () => deprecateContract(c.id), "Contract deprecated")}
                        >
                          Deprecate
                        </Button>
                      )}
                      {showReset && (
                        <Button
                          size="sm"
                          disabled={busyId === c.id}
                          onClick={() => void run(c.id, () => resetContractBreaker(c.id), "Breaker reset")}
                        >
                          Reset breaker
                        </Button>
                      )}
                      <Button
                        size="sm"
                        variant="ghost"
                        disabled={busyId === c.id}
                        onClick={async () => {
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
                        }}
                      >
                        Export
                      </Button>
                    </div>
                  </article>
                );
              })}
            </div>
          </>
        )}
      </PageFrame>
    </PageShell>
  );
}
