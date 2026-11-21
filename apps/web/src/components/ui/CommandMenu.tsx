/**
 * The command menu (UI pass 2, item 7), on cmdk inside its Radix dialog.
 * Sections: actions for what is selected now (the selected job on Jobs),
 * then pages, then matching jobs, connections and schedules, then help.
 * Keyboard: ⌘K / Ctrl+K opens it, arrows move, Enter runs, Esc closes and
 * returns focus to where it was.
 */
import { Command } from "cmdk";
import { useEffect, useMemo } from "react";
import { pushOverlay } from "../../lib/overlayStack";
import type { IconName } from "../icons/iconMap";
import { useSelectionActions } from "../../lib/commandRegistry";
import type { Connector, PipelineSchedule, Screen, TransferJob } from "../../lib/types";
import { DtIcon } from "../DtIcon";

export interface CommandMenuProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  navItems: { id: Screen; label: string; icon: IconName }[];
  jobs: TransferJob[];
  connectors: Connector[];
  schedules: PipelineSchedule[];
  onNavigate: (target: { screen: Screen; jobId?: string; connectorId?: string; scheduleId?: string }) => void;
  onShowShortcuts: () => void;
}

const MAX_RECORDS = 8;

export function CommandMenu({ open, onOpenChange, navItems, jobs, connectors, schedules, onNavigate, onShowShortcuts }: CommandMenuProps) {
  const selection = useSelectionActions();
  const run = (fn: () => void) => {
    onOpenChange(false);
    // Let the dialog close (and focus return) before the action opens anything.
    window.setTimeout(fn, 0);
  };
  const recentJobs = useMemo(() => jobs.slice(0, MAX_RECORDS * 4), [jobs]);
  // On the app's overlay stack while open: Escape closes this menu only, never
  // a dialog underneath it (review UI2 #6).
  useEffect(() => {
    if (!open) return undefined;
    let pop = () => {};
    const timer = window.setTimeout(() => {
      const content = document.querySelector<HTMLElement>(".df2-cmdk-content");
      if (content) pop = pushOverlay(content);
    }, 0);
    return () => {
      window.clearTimeout(timer);
      pop();
    };
  }, [open]);
  return (
    <Command.Dialog
      open={open}
      onOpenChange={onOpenChange}
      label="Command menu"
      className="df2-cmdk"
      overlayClassName="df2-cmdk-overlay"
      contentClassName="df2-cmdk-content"
      loop
    >
      <div className="df2-cmdk-input-row">
        <DtIcon name="search" size={16} chrome="product" />
        <Command.Input className="df2-cmdk-input" placeholder="Type a command or search…" aria-label="Command or search" />
        <kbd className="df2-cmdk-kbd">Esc</kbd>
      </div>
      <Command.List className="df2-cmdk-list">
        <Command.Empty className="df2-cmdk-empty">No command or record matches.</Command.Empty>
        {selection && (
          <Command.Group heading={selection.title} className="df2-cmdk-group">
            {selection.actions.map((action) => (
              <Command.Item
                key={action.id}
                value={`selection ${action.label}`}
                keywords={action.keywords}
                className="df2-cmdk-item"
                data-tone={action.tone}
                onSelect={() => run(action.run)}
              >
                <DtIcon name={action.icon ?? "chevron-right"} size={16} chrome="product" />
                <span>{action.label}</span>
              </Command.Item>
            ))}
          </Command.Group>
        )}
        <Command.Group heading="Go to" className="df2-cmdk-group">
          {navItems.map((item) => (
            <Command.Item key={item.id} value={`go ${item.label}`} className="df2-cmdk-item" onSelect={() => run(() => onNavigate({ screen: item.id }))}>
              <DtIcon name={item.icon} size={16} chrome="product" />
              <span>{item.label}</span>
            </Command.Item>
          ))}
        </Command.Group>
        {recentJobs.length > 0 && (
          <Command.Group heading="Jobs" className="df2-cmdk-group">
            {recentJobs.map((job) => (
              <Command.Item
                key={job._id}
                value={`job ${job._id} ${job.name || ""} ${job.source_name || ""} ${job.destination_collection || ""}`}
                className="df2-cmdk-item"
                onSelect={() => run(() => onNavigate({ screen: "jobs", jobId: job._id }))}
              >
                <DtIcon name="jobs" size={16} chrome="product" />
                <span>{job.name || `${job.source_name || "Source"} to ${job.destination_collection || "destination"}`}</span>
                <span className="df2-cmdk-meta">{job._id.slice(0, 8)}</span>
              </Command.Item>
            ))}
          </Command.Group>
        )}
        {connectors.length > 0 && (
          <Command.Group heading="Connections" className="df2-cmdk-group">
            {connectors.slice(0, MAX_RECORDS * 4).map((c) => (
              <Command.Item key={c.id} value={`connection ${c.name} ${c.type}`} className="df2-cmdk-item" onSelect={() => run(() => onNavigate({ screen: "connectors", connectorId: c.id }))}>
                <DtIcon name="connectors" size={16} chrome="product" />
                <span>{c.name}</span>
                <span className="df2-cmdk-meta">{c.type}</span>
              </Command.Item>
            ))}
          </Command.Group>
        )}
        {schedules.length > 0 && (
          <Command.Group heading="Schedules" className="df2-cmdk-group">
            {schedules.slice(0, MAX_RECORDS * 4).map((s) => (
              <Command.Item key={s.id} value={`schedule ${s.name}`} className="df2-cmdk-item" onSelect={() => run(() => onNavigate({ screen: "schedules", scheduleId: s.id }))}>
                <DtIcon name="schedule" size={16} chrome="product" />
                <span>{s.name}</span>
              </Command.Item>
            ))}
          </Command.Group>
        )}
        <Command.Group heading="Help" className="df2-cmdk-group">
          <Command.Item value="help keyboard shortcuts" className="df2-cmdk-item" onSelect={() => run(onShowShortcuts)}>
            <DtIcon name="help" size={16} chrome="product" />
            <span>Keyboard shortcuts</span>
            <kbd className="df2-cmdk-kbd">?</kbd>
          </Command.Item>
        </Command.Group>
      </Command.List>
    </Command.Dialog>
  );
}
