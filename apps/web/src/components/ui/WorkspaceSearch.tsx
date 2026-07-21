import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { ConnectorIcon } from "../../app/brand-icons";
import { DtIcon } from "../DtIcon";
import { Connector, PipelineSchedule, Screen, TransferJob } from "../../lib/types";

export interface SearchNavigateTarget {
  screen: Screen;
  connectorId?: string;
  jobId?: string;
  scheduleId?: string;
  /** Optional detail panel deep-link (e.g. mapping-proof). */
  panel?: string;
}

export interface SearchResult {
  id: string;
  kind: "page" | "connector" | "job" | "pipeline";
  label: string;
  meta: string;
  screen: Screen;
  connectorType?: string;
  connectorId?: string;
  jobId?: string;
  scheduleId?: string;
}

interface WorkspaceSearchProps {
  query: string;
  onQueryChange: (q: string) => void;
  onNavigate: (target: SearchNavigateTarget) => void;
  navItems: { id: Screen; label: string; desc: string; icon: string }[];
  connectors: Connector[];
  jobs: TransferJob[];
  schedules: PipelineSchedule[];
  inputRef?: React.RefObject<HTMLInputElement | null>;
}

const PAGE_ALIASES: Record<string, Screen> = {
  overview: "dashboard",
  dashboard: "dashboard",
  transfer: "transfer",
  studio: "transfer",
  pilot: "pilot",
  connector: "connectors",
  connectors: "connectors",
  connection: "connectors",
  connections: "connectors",
  pipeline: "schedules",
  pipelines: "schedules",
  schedule: "schedules",
  schedules: "schedules",
  job: "jobs",
  jobs: "jobs",
  theater: "jobs",
  mcp: "mcp",
  settings: "settings",
  setting: "settings",
  docs: "docs",
  documentation: "docs",
  help: "docs",
  benchmark: "benchmarks",
  benchmarks: "benchmarks",
  performance: "benchmarks",
};

function matchesQuery(value: string | undefined | null, q: string): boolean {
  return (value ?? "").toLowerCase().includes(q);
}

export function WorkspaceSearch({
  query,
  onQueryChange,
  onNavigate,
  navItems,
  connectors,
  jobs,
  schedules,
  inputRef,
}: WorkspaceSearchProps) {
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(0);
  const [dropdownRect, setDropdownRect] = useState<{ top: number; left: number; width: number } | null>(null);
  const wrapRef = useRef<HTMLDivElement>(null);

  const results = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return [];
    const out: SearchResult[] = [];
    const seenPages = new Set<Screen>();

    const aliasScreen = PAGE_ALIASES[q];
    if (aliasScreen) {
      const item = navItems.find((n) => n.id === aliasScreen);
      if (item) {
        out.push({
          id: `page-${item.id}`,
          kind: "page",
          label: item.label,
          meta: item.desc,
          screen: item.id,
        });
        seenPages.add(item.id);
      }
    }

    for (const item of navItems) {
      if (seenPages.has(item.id)) continue;
      if (
        item.label.toLowerCase().includes(q)
        || item.id.includes(q)
        || item.desc.toLowerCase().includes(q)
      ) {
        out.push({
          id: `page-${item.id}`,
          kind: "page",
          label: item.label,
          meta: item.desc,
          screen: item.id,
        });
        seenPages.add(item.id);
      }
    }

    for (const c of connectors) {
      if (
        matchesQuery(c.name, q)
        || matchesQuery(c.type, q)
        || matchesQuery(c.host, q)
        || matchesQuery(c.database, q)
      ) {
        out.push({
          id: `conn-${c.id}`,
          kind: "connector",
          label: c.name,
          meta: `${c.type} · ${c.host || "managed"}${c.port ? `:${c.port}` : ""}`,
          screen: "connectors",
          connectorType: c.type,
          connectorId: c.id,
        });
      }
    }

    for (const s of schedules) {
      const source = connectors.find((c) => c.id === s.source_connector_id);
      const dest = connectors.find((c) => c.id === s.dest_connector_id);
      if (
        matchesQuery(s.name, q)
        || matchesQuery(s.interval, q)
        || matchesQuery(s.source_table, q)
        || matchesQuery(s.dest_table, q)
        || matchesQuery(source?.name, q)
        || matchesQuery(dest?.name, q)
      ) {
        out.push({
          id: `sched-${s.id}`,
          kind: "pipeline",
          label: s.name,
          meta: `${s.interval} · ${s.enabled ? "active" : "paused"} · ${source?.name ?? "source"} → ${dest?.name ?? "dest"}`,
          screen: "schedules",
          scheduleId: s.id,
        });
      }
    }

    for (const j of jobs.slice(0, 40)) {
      const srcName = j.source_name ?? "";
      if (
        matchesQuery(srcName, q)
        || matchesQuery(j.source_type, q)
        || matchesQuery(j.destination_type, q)
        || matchesQuery(j._id, q)
        || matchesQuery(j.destination_collection, q)
        || matchesQuery(j.destination_database, q)
        || matchesQuery(j.error, q)
      ) {
        out.push({
          id: `job-${j._id}`,
          kind: "job",
          label: `${srcName || j.source_type || "Source"} → ${j.destination_collection || j.destination_database || "dest"}`,
          meta: `${j.status} · ${(j.records_processed ?? 0).toLocaleString()} rows`,
          screen: "jobs",
          jobId: j._id,
        });
      }
    }

    return out.slice(0, 10);
  }, [query, navItems, connectors, jobs, schedules]);

  const syncDropdownPosition = useCallback(() => {
    const anchor = wrapRef.current;
    if (!anchor) return;
    const rect = anchor.getBoundingClientRect();
    setDropdownRect({
      top: rect.bottom + 6,
      left: rect.left,
      width: Math.max(rect.width, 280),
    });
  }, []);

  useEffect(() => {
    setActiveIndex(0);
  }, [results.length, query]);

  useEffect(() => {
    const onDocClick = (e: MouseEvent) => {
      const target = e.target as Node;
      if (wrapRef.current?.contains(target)) return;
      const portal = document.getElementById("workspace-search-results");
      if (portal?.contains(target)) return;
      setOpen(false);
    };
    document.addEventListener("mousedown", onDocClick);
    return () => document.removeEventListener("mousedown", onDocClick);
  }, []);

  useEffect(() => {
    if (!open || !query.trim()) {
      setDropdownRect(null);
      return;
    }
    syncDropdownPosition();
    const onLayout = () => syncDropdownPosition();
    window.addEventListener("resize", onLayout);
    window.addEventListener("scroll", onLayout, true);
    return () => {
      window.removeEventListener("resize", onLayout);
      window.removeEventListener("scroll", onLayout, true);
    };
  }, [open, query, results.length, syncDropdownPosition]);

  const pick = (result: SearchResult) => {
    onNavigate({
      screen: result.screen,
      connectorId: result.connectorId,
      jobId: result.jobId,
      scheduleId: result.scheduleId,
    });
    onQueryChange("");
    setOpen(false);
    setDropdownRect(null);
    inputRef?.current?.blur();
  };

  const onKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActiveIndex((i) => Math.min(i + 1, results.length - 1));
      setOpen(true);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActiveIndex((i) => Math.max(i - 1, 0));
    } else if (e.key === "Enter" && results.length > 0) {
      e.preventDefault();
      pick(results[activeIndex] ?? results[0]);
    } else if (e.key === "Escape") {
      setOpen(false);
      inputRef?.current?.blur();
    }
  };

  const kindIcon = (kind: SearchResult["kind"]) => {
    if (kind === "page") return "dashboard";
    if (kind === "connector") return "connectors";
    if (kind === "pipeline") return "activity";
    return "jobs";
  };

  const kindLabel = (kind: SearchResult["kind"]) => {
    if (kind === "page") return "page";
    if (kind === "connector") return "connection";
    if (kind === "pipeline") return "pipeline";
    return "job";
  };

  const showDropdown = open && query.trim() && dropdownRect;

  return (
    <div className="df2-workspace-search-wrap" ref={wrapRef}>
      <div className="df2-command-search df2-workspace-search" role="search">
        <DtIcon name="search" size={15} />
        <input
          ref={inputRef}
          type="text"
          role="searchbox"
          placeholder="Search pages, connections, pipelines, jobs…"
          value={query}
          onChange={(e) => {
            onQueryChange(e.target.value);
            setOpen(true);
          }}
          onFocus={() => {
            setOpen(true);
            syncDropdownPosition();
          }}
          onKeyDown={onKeyDown}
          aria-label="Search workspace"
          aria-expanded={Boolean(showDropdown)}
          aria-controls="workspace-search-results"
          autoComplete="off"
          spellCheck={false}
        />
        <kbd aria-hidden="true">⌘K</kbd>
      </div>

      {showDropdown && createPortal(
        <div
          id="workspace-search-results"
          className="df2-search-dropdown df2-search-dropdown-portal"
          role="listbox"
          style={{
            position: "fixed",
            top: dropdownRect.top,
            left: dropdownRect.left,
            width: dropdownRect.width,
          }}
        >
          {results.length === 0 ? (
            <div className="df2-search-empty">No results for &ldquo;{query.trim()}&rdquo;</div>
          ) : (
            results.map((r, i) => (
              <button
                key={r.id}
                type="button"
                role="option"
                aria-selected={i === activeIndex}
                className={`df2-search-result ${i === activeIndex ? "active" : ""}`}
                onMouseEnter={() => setActiveIndex(i)}
                onClick={() => pick(r)}
              >
                <span className="df2-search-result-icon">
                  {r.kind === "connector" ? (
                    <ConnectorIcon id={r.connectorType ?? "database"} size={18} />
                  ) : (
                    <DtIcon name={kindIcon(r.kind)} size={18} />
                  )}
                </span>
                <span className="df2-search-result-body">
                  <strong>{r.label}</strong>
                  <small>{r.meta}</small>
                </span>
                <span className="df2-search-result-kind">{kindLabel(r.kind)}</span>
              </button>
            ))
          )}
        </div>,
        document.body,
      )}
    </div>
  );
}
