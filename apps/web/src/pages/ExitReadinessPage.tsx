/**
 * Exit readiness (Reversibility Proof, Pages A and B).
 *
 * The board shows, per declared system, whether its exit has been proven by a
 * recent rehearsal and how long that exit really took against its window. A
 * rehearsal opens in place (Page B) and its URL is shareable:
 * `#/exit?system=<id>&rehearsal=<id>`, the same link the copilot returns.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import { Button } from "../components/ui/Button";
import { EmptyState } from "../components/ui/EmptyState";
import { PageFrame } from "../components/ui/PageFrame";
import { PageShell } from "../components/ui/PageShell";
import { SectionLoader } from "../components/LoadingState";
import { DtIcon } from "../components/DtIcon";
import { PermissionNotice } from "../components/PermissionNotice";
import { useToast } from "../components/Toast";
import { useConfirm } from "../components/ui/ConfirmDialog";
import { ExitBoardView } from "../components/exit/ExitBoardView";
import { RehearsalPanel } from "../components/exit/RehearsalPanel";
import { RunRehearsalDialog } from "../components/exit/RunRehearsalDialog";
import { SystemDrawer } from "../components/exit/SystemDrawer";
import { SystemFormDrawer } from "../components/exit/SystemFormDrawer";
import {
  declareExitSystem,
  deleteExitSystem,
  fetchExitReadiness,
  startExitRehearsal,
  updateExitSystem,
} from "../lib/exitApi";
import {
  exitErrorMessage,
  exitHash,
  type BoardFilter,
  type ExitFocus,
  type ExitReadiness,
  type ExitSystem,
  type ExitSystemInput,
} from "../lib/exitReadiness";
import { PERMISSIONS, useWriteGate } from "../lib/PermissionsContext";
import type { Connector } from "../lib/types";

export interface ExitReadinessPageProps {
  active?: boolean;
  connectors?: Connector[];
  /** A deep link to open (copilot link, reload); `token` re-applies the same ids. */
  focus?: (ExitFocus & { token: number }) | null;
  onOpenJob?: (jobId: string) => void;
}

/** Keep the address shareable without a history entry per click (the app owns hash navigation). */
function replaceExitHash(focus: ExitFocus): void {
  if (typeof window === "undefined") return;
  const next = exitHash(focus);
  if (window.location.hash !== next) window.history.replaceState(null, "", next);
}

export function ExitReadinessPage({ active = true, connectors = [], focus = null, onOpenJob }: ExitReadinessPageProps) {
  const { toast } = useToast();
  const { confirm } = useConfirm();
  // Declaring a system configures recurring extracts of a saved connection: job.manage.
  const manageGate = useWriteGate(PERMISSIONS.jobManage, "declare or change exit systems");
  const runGate = useWriteGate(PERMISSIONS.jobRun, "start an exit rehearsal");
  const [readiness, setReadiness] = useState<ExitReadiness | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState<BoardFilter>("all");
  const [systemId, setSystemId] = useState<string | null>(null);
  const [rehearsalId, setRehearsalId] = useState<string | null>(null);
  const [drawerKey, setDrawerKey] = useState(0);
  const [form, setForm] = useState<{ open: boolean; system: ExitSystem | null }>({ open: false, system: null });
  const [runDialog, setRunDialog] = useState<{ open: boolean; systemId: string | null; error?: string }>({ open: false, systemId: null });
  const [startingId, setStartingId] = useState<string | null>(null);
  const activeRef = useRef(active);
  activeRef.current = active;

  const load = useCallback(async (signal?: AbortSignal) => {
    setLoading(true);
    try {
      const next = await fetchExitReadiness({ signal });
      setReadiness(next);
      setLoadError("");
    } catch (err) {
      if (signal?.aborted) return;
      setLoadError(exitErrorMessage(err, "Could not load exit readiness."));
    } finally {
      if (!signal?.aborted) setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!active) return;
    const controller = new AbortController();
    void load(controller.signal);
    return () => controller.abort();
  }, [active, load]);

  // A deep link opens its rehearsal (Page B) or its system's drawer.
  useEffect(() => {
    if (!focus) return;
    if (focus.rehearsal) {
      setRehearsalId(focus.rehearsal);
      setSystemId(null);
    } else if (focus.system) {
      setRehearsalId(null);
      setSystemId(focus.system);
    }
  }, [focus?.token]); // eslint-disable-line react-hooks/exhaustive-deps

  // The address follows what is open, while this screen is the one shown.
  useEffect(() => {
    if (!active) return;
    replaceExitHash(rehearsalId ? { rehearsal: rehearsalId } : systemId ? { system: systemId } : {});
  }, [active, rehearsalId, systemId]);

  const openRehearsal = (id: string) => {
    setSystemId(null);
    setRehearsalId(id);
    if (typeof window !== "undefined") window.scrollTo?.({ top: 0 });
  };

  const start = async (id: string) => {
    if (!runGate.allowed) {
      toast({ title: "No permission to run rehearsals", message: runGate.reason, tone: "warning" });
      return;
    }
    setStartingId(id);
    setRunDialog((d) => ({ ...d, error: undefined }));
    try {
      const rehearsal = await startExitRehearsal(id);
      toast({ title: "Rehearsal started", message: `${rehearsal.system_name}: rehearsal ${rehearsal.n} is extracting.`, tone: "success" });
      setRunDialog({ open: false, systemId: null });
      openRehearsal(rehearsal.id);
      void load();
    } catch (err) {
      const message = exitErrorMessage(err, "Could not start the rehearsal.");
      if (runDialog.open) setRunDialog((d) => ({ ...d, error: message }));
      else toast({ title: "Could not start the rehearsal", message, tone: "error" });
      void load();
      setDrawerKey((n) => n + 1);
    } finally {
      setStartingId(null);
    }
  };

  const submitSystem = async (input: ExitSystemInput, system: ExitSystem | null) => {
    const saved = system ? await updateExitSystem(system.id, input, system.version) : await declareExitSystem(input);
    toast({ title: system ? "System saved" : "System declared", message: saved.name, tone: "success" });
    setForm({ open: false, system: null });
    setSystemId(saved.id);
    setDrawerKey((n) => n + 1);
    void load();
  };

  const remove = async (system: ExitSystem) => {
    const ok = await confirm({
      title: `Remove ${system.name}?`,
      message: "The system leaves the board and is no longer rehearsed. Its rehearsals and evidence packs are kept.",
      confirmLabel: "Remove system",
      tone: "danger",
      facts: [
        { label: "Tables", value: String(system.tables.length) },
        { label: "Source", value: system.source.connector_name || system.source.connector_id },
      ],
    });
    if (!ok) return;
    try {
      await deleteExitSystem(system.id);
      toast({ title: "System removed", message: system.name, tone: "success" });
      setSystemId(null);
      void load();
    } catch (err) {
      toast({ title: "Could not remove the system", message: exitErrorMessage(err, "Could not remove this system."), tone: "error" });
    }
  };

  const runDialogSystems = readiness?.systems ?? [];

  let body;
  if (rehearsalId) {
    body = (
      <RehearsalPanel
        rehearsalId={rehearsalId}
        onBack={() => setRehearsalId(null)}
        onOpenJob={onOpenJob}
        onOpenSystem={(id) => {
          setRehearsalId(null);
          setSystemId(id);
        }}
        onChanged={() => {
          if (activeRef.current) void load();
        }}
      />
    );
  } else if (!readiness && (loading || !loadError)) {
    body = <SectionLoader title="Loading exit readiness…" />;
  } else if (!readiness) {
    body = (
      <div className="df2-alert df2-alert-error" role="alert">
        <DtIcon name="alert" size={18} />
        <div>
          <strong>Could not load exit readiness</strong>
          <p>{loadError}</p>
        </div>
        <Button size="sm" onClick={() => void load()}>Retry</Button>
      </div>
    );
  } else if (readiness.systems.length === 0) {
    body = (
      <EmptyState
        page
        icon="shield"
        title="No systems declared yet"
        description="Declare what you would have to take with you if you left a provider: a saved connection, its tables, and the window an exit must fit in (a contract clause or the EU Data Act switching window). Datawrap then rehearses the exit for real and measures it."
        action={
          <Button
            variant="primary"
            onClick={() => setForm({ open: true, system: null })}
            disabled={!manageGate.allowed}
            title={!manageGate.allowed ? manageGate.reason : undefined}
            leadingIcon={<DtIcon name="plus" size={14} />}
          >
            Declare a system
          </Button>
        }
      />
    );
  } else {
    body = (
      <ExitBoardView
        readiness={readiness}
        query={query}
        onQueryChange={setQuery}
        filter={filter}
        onFilterChange={setFilter}
        selectedId={systemId}
        onOpenSystem={setSystemId}
        onOpenRehearsal={openRehearsal}
        onRunRehearsal={(id) => setRunDialog({ open: true, systemId: id })}
        onDeclare={() => setForm({ open: true, system: null })}
        canDeclare={manageGate.allowed}
        canRun={runGate.allowed}
        runDeniedReason={runGate.reason}
        declareDeniedReason={manageGate.reason}
        startingId={startingId}
      />
    );
  }

  return (
    <PageShell
      className="df2-page-exit"
      title="Exit readiness"
      description="Declared systems, rehearsed exits measured against their windows, and signed exit evidence."
    >
      <PageFrame className="df2-exit-workspace">
        {!rehearsalId && (
          <PermissionNotice
            allowed={manageGate.allowed && runGate.allowed}
            reason={!runGate.allowed ? runGate.reason : manageGate.reason}
            what="Exit readiness is read-only for you."
          />
        )}
        {loadError && readiness && !rehearsalId && (
          <p className="df2-parity-notice is-danger" role="alert">
            <DtIcon name="alert" size={14} />
            <span>{loadError}</span>
          </p>
        )}
        {body}
      </PageFrame>

      <SystemDrawer
        systemId={rehearsalId ? null : systemId}
        refreshKey={drawerKey}
        onClose={() => setSystemId(null)}
        onOpenRehearsal={openRehearsal}
        onRunRehearsal={(system) => {
          // One overlay at a time: the dialog replaces the drawer (Escape closes only what is on top).
          setSystemId(null);
          setRunDialog({ open: true, systemId: system.id });
        }}
        onEdit={(system) => {
          setSystemId(null);
          setForm({ open: true, system });
        }}
        onDelete={(system) => void remove(system)}
        canRun={runGate.allowed}
        canManage={manageGate.allowed}
        runDeniedReason={runGate.reason}
        manageDeniedReason={manageGate.reason}
        starting={Boolean(systemId && startingId === systemId)}
      />
      <SystemFormDrawer
        open={form.open}
        system={form.system}
        connectors={connectors}
        onClose={() => {
          // Cancelling an edit goes back to the system it came from.
          if (form.system) setSystemId(form.system.id);
          setForm({ open: false, system: null });
        }}
        onSubmit={submitSystem}
      />
      <RunRehearsalDialog
        open={runDialog.open}
        systems={runDialogSystems}
        systemId={runDialog.systemId}
        starting={startingId != null}
        error={runDialog.error}
        onClose={() => setRunDialog({ open: false, systemId: null })}
        onStart={(id) => void start(id)}
      />
    </PageShell>
  );
}
