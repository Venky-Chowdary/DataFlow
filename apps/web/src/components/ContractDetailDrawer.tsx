import { useEffect, useState } from "react";
import { DtIcon } from "./DtIcon";
import { Button } from "./ui/Button";
import { Drawer } from "./ui/Drawer";
import { EmptyState } from "./ui/EmptyState";
import { FilterTabs } from "./ui/FilterTabs";
import {
  ContractBreaker,
  DataContractSummary,
  fetchContractBreaker,
} from "../lib/api";

export const CONTRACT_TABS = ["Overview", "Columns", "Mappings", "Quality"] as const;
export type ContractTab = (typeof CONTRACT_TABS)[number];

interface ContractDetailDrawerProps {
  open: boolean;
  contract: DataContractSummary | null;
  tab: ContractTab;
  setTab: (tab: ContractTab) => void;
  breakerHint?: string;
  busy?: boolean;
  onClose: () => void;
  onSign: () => void;
  onDeprecate: () => void;
  onResetBreaker: () => void;
  onExport: () => void;
}

function statusBadge(status: string) {
  const s = (status || "").toLowerCase();
  if (s === "signed") return { cls: "df2-badge-live", label: "Signed" };
  if (s === "broken") return { cls: "df2-badge-error", label: "Broken" };
  if (s === "deprecated") return { cls: "df2-badge-muted", label: "Deprecated" };
  return { cls: "df2-badge-warn", label: "Draft" };
}

function endpointLabel(side: Record<string, unknown> | undefined): string {
  if (!side) return "—";
  const type = String(side.type || side.format || "").trim();
  const name = String(side.name || side.database || side.table || side.collection || "").trim();
  if (type && name) return `${type} · ${name}`;
  return type || name || "—";
}

function cell(v: unknown): string {
  if (v == null || v === "") return "—";
  if (typeof v === "object") {
    try {
      return JSON.stringify(v);
    } catch {
      return String(v);
    }
  }
  return String(v);
}

export function ContractDetailDrawer({
  open,
  contract: c,
  tab,
  setTab,
  breakerHint,
  busy,
  onClose,
  onSign,
  onDeprecate,
  onResetBreaker,
  onExport,
}: ContractDetailDrawerProps) {
  const [breaker, setBreaker] = useState<ContractBreaker | null>(null);

  useEffect(() => {
    if (!open || !c?.id) {
      setBreaker(null);
      return;
    }
    let cancelled = false;
    fetchContractBreaker(c.id)
      .then((b) => {
        if (!cancelled) setBreaker(b);
      })
      .catch(() => {
        if (!cancelled) setBreaker(null);
      });
    return () => {
      cancelled = true;
    };
  }, [open, c?.id]);

  if (!c) return null;

  const badge = statusBadge(c.status);
  const breakerState = breaker?.state || breakerHint;
  const showReset =
    c.status === "broken" || breakerState === "open" || breakerState === "half_open";
  const canSign = c.status === "draft" || c.status === "broken";
  const canDeprecate = c.status !== "deprecated";

  return (
    <Drawer
      open={open}
      onClose={onClose}
      width={620}
      ariaLabel={`${c.name} contract details`}
      icon={<DtIcon name="shield" size={22} />}
      title={c.name}
      subtitle={`v${c.version} · ${c.columns?.length || 0} columns · ${c.mappings?.length || 0} mappings`}
      headerExtra={
        <>
          <span className={`df2-badge ${badge.cls}`}>{badge.label}</span>
          {breakerState && (
            <span className={`df2-badge ${breakerState === "closed" ? "df2-badge-live" : "df2-badge-warn"}`}>
              Breaker {breakerState}
            </span>
          )}
        </>
      }
      footer={
        <div className="df2-drawer-actions">
          {canSign && (
            <Button
              size="sm"
              variant="primary"
              disabled={busy}
              onClick={onSign}
              leadingIcon={<DtIcon name="check" size={14} />}
            >
              Sign
            </Button>
          )}
          {canDeprecate && (
            <Button size="sm" disabled={busy} onClick={onDeprecate}>
              Deprecate
            </Button>
          )}
          {showReset && (
            <Button size="sm" disabled={busy} onClick={onResetBreaker}>
              Reset breaker
            </Button>
          )}
          <Button
            size="sm"
            variant="ghost"
            disabled={busy}
            onClick={onExport}
            leadingIcon={<DtIcon name="download" size={14} />}
          >
            Export
          </Button>
        </div>
      }
    >
      <div className="df2-drawer-facts" aria-label="Contract summary">
        <div className="df2-drawer-fact">
          <span>Status</span>
          <strong>{badge.label}</strong>
        </div>
        <div className="df2-drawer-fact">
          <span>Version</span>
          <strong>v{c.version}</strong>
        </div>
        <div className="df2-drawer-fact">
          <span>Source</span>
          <strong title={endpointLabel(c.source)}>{endpointLabel(c.source)}</strong>
        </div>
        <div className="df2-drawer-fact">
          <span>Destination</span>
          <strong title={endpointLabel(c.destination)}>{endpointLabel(c.destination)}</strong>
        </div>
      </div>

      <div className="df2-drawer-section df2-drawer-workbench">
        <FilterTabs
          ariaLabel="Contract detail sections"
          value={tab}
          onChange={setTab}
          items={[
            { id: "Overview", label: "Overview" },
            { id: "Columns", label: "Columns", count: c.columns?.length || 0 },
            { id: "Mappings", label: "Mappings", count: c.mappings?.length || 0 },
            { id: "Quality", label: "Quality", count: c.quality_rules?.length || 0 },
          ]}
        />

        {tab === "Overview" && (
          <section className="df2-drawer-section" aria-label="Contract overview">
            <dl className="df2-drawer-kv">
              <div><dt>Contract ID</dt><dd className="df2-cell-mono">{c.id}</dd></div>
              <div><dt>Strict</dt><dd>{c.strict ? "Yes" : "No"}</dd></div>
              <div><dt>Created</dt><dd>{c.created_at ? new Date(c.created_at).toLocaleString() : "—"}</dd></div>
              <div><dt>Updated</dt><dd>{c.updated_at ? new Date(c.updated_at).toLocaleString() : "—"}</dd></div>
              {breaker && (
                <>
                  <div><dt>Breaker state</dt><dd>{breaker.state}</dd></div>
                  <div><dt>Failures</dt><dd>{breaker.failure_count} / {breaker.failure_threshold}</dd></div>
                  <div><dt>Successes</dt><dd>{breaker.success_count}</dd></div>
                  <div><dt>Recovery timeout</dt><dd>{breaker.recovery_timeout_seconds}s</dd></div>
                </>
              )}
            </dl>
            {(c.preflight_gates?.length ?? 0) > 0 && (
              <>
                <div className="df2-drawer-section-head">
                  <h3><DtIcon name="gate" size={14} /> Preflight gates</h3>
                  <span className="df2-drawer-count">{c.preflight_gates!.length}</span>
                </div>
                <ul className="df2-drawer-related-list">
                  {c.preflight_gates!.slice(0, 12).map((g, i) => (
                    <li key={i} className="df2-drawer-related-row">
                      <span className="df2-drawer-related-main">
                        <strong>{cell((g as Record<string, unknown>).name ?? (g as Record<string, unknown>).id ?? `Gate ${i + 1}`)}</strong>
                        <small>{cell((g as Record<string, unknown>).status ?? (g as Record<string, unknown>).message)}</small>
                      </span>
                    </li>
                  ))}
                </ul>
              </>
            )}
          </section>
        )}

        {tab === "Columns" && (
          <section className="df2-drawer-section" aria-label="Contract columns">
            {(c.columns?.length ?? 0) === 0 ? (
              <EmptyState compact icon="database" title="No columns" description="This contract has no column definitions." />
            ) : (
              <div className="df2-table-wrap df2-drawer-table-wrap">
                <table className="df2-table">
                  <thead>
                    <tr>
                      <th>Name</th>
                      <th>Type</th>
                      <th>Nullable</th>
                    </tr>
                  </thead>
                  <tbody>
                    {c.columns.map((col, i) => {
                      const row = col as Record<string, unknown>;
                      const name = cell(row.name ?? row.column_name ?? row.source ?? `col_${i}`);
                      const typ = cell(row.type ?? row.inferred_type ?? row.data_type);
                      const nullable = row.nullable == null ? "—" : row.nullable ? "Yes" : "No";
                      return (
                        <tr key={`${name}-${i}`}>
                          <td title={name}>{name}</td>
                          <td className="df2-cell-mono" title={typ}>{typ}</td>
                          <td>{nullable}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        )}

        {tab === "Mappings" && (
          <section className="df2-drawer-section" aria-label="Contract mappings">
            {(c.mappings?.length ?? 0) === 0 ? (
              <EmptyState compact icon="connectors" title="No mappings" description="This contract has no column mappings." />
            ) : (
              <div className="df2-table-wrap df2-drawer-table-wrap">
                <table className="df2-table">
                  <thead>
                    <tr>
                      <th>Source</th>
                      <th>Target</th>
                      <th>Type</th>
                    </tr>
                  </thead>
                  <tbody>
                    {c.mappings.map((m, i) => {
                      const row = m as Record<string, unknown>;
                      const src = cell(row.source ?? row.source_column);
                      const tgt = cell(row.target ?? row.target_column);
                      const typ = cell(row.target_type ?? row.source_type ?? row.type);
                      return (
                        <tr key={`${src}-${tgt}-${i}`}>
                          <td title={src}>{src}</td>
                          <td title={tgt}>{tgt}</td>
                          <td className="df2-cell-mono" title={typ}>{typ}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        )}

        {tab === "Quality" && (
          <section className="df2-drawer-section" aria-label="Quality rules">
            {(c.quality_rules?.length ?? 0) === 0 ? (
              <EmptyState compact icon="gate" title="No quality rules" description="No explicit quality rules are attached to this contract." />
            ) : (
              <ul className="df2-drawer-related-list">
                {c.quality_rules.map((rule, i) => {
                  const row = rule as Record<string, unknown>;
                  return (
                    <li key={i} className="df2-drawer-related-row">
                      <span className="df2-drawer-related-main">
                        <strong>{cell(row.name ?? row.rule ?? row.type ?? `Rule ${i + 1}`)}</strong>
                        <small>{cell(row.description ?? row.column ?? row.message)}</small>
                      </span>
                    </li>
                  );
                })}
              </ul>
            )}
          </section>
        )}
      </div>
    </Drawer>
  );
}
