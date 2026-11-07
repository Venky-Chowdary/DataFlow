import { useCallback, useEffect, useState } from "react";
import { DtIcon } from "../DtIcon";
import { useToast } from "../Toast";
import { Button } from "../ui/Button";
import { CopyIdChip } from "../ui/CopyIdChip";
import { useConfirm } from "../ui/ConfirmDialog";
import { Drawer } from "../ui/Drawer";
import { EmptyState } from "../ui/EmptyState";
import { deleteJournal, listJournals } from "../../lib/api";
import { PERMISSIONS, useWriteGate } from "../../lib/PermissionsContext";
import type { JournalManifest, JournalStorage } from "../../lib/types";

export function journalSourceLabel(journal: JournalManifest): string {
  const src = journal.source || {};
  const object = src.object || src.filename || "";
  const engine = src.format || src.kind || "source";
  return object ? `${engine} · ${object}` : engine;
}

export function journalStorageBadge(storage: JournalStorage | undefined): { label: string; tone: string; detail: string } {
  if (storage?.durable) {
    return {
      label: "Shared storage",
      tone: "df2-badge-live",
      detail: `Mirrored to ${storage.backend || "object storage"} — any API replica can replay it.`,
    };
  }
  return {
    label: "This host only",
    tone: "df2-badge-warn",
    detail: storage?.reason || "Not mirrored to shared storage — replay only works on the host that captured it.",
  };
}

function journalRows(journal: JournalManifest): number {
  return (journal.streams ?? []).reduce((sum, s) => sum + (Number(s.rows) || 0), 0);
}

/** File journals keep the exact source bytes; their row count is the file's, read on replay. */
export function journalRowsLabel(journal: JournalManifest): string {
  const rows = journalRows(journal);
  const fileOnly = (journal.streams ?? []).length > 0 && (journal.streams ?? []).every((s) => (s.format || "jsonl") !== "jsonl");
  return rows === 0 && fileOnly ? "Source file" : rows.toLocaleString();
}

function formatDate(epochSeconds?: number): string {
  return epochSeconds ? new Date(epochSeconds * 1000).toLocaleString() : "—";
}

interface JournalsDrawerProps {
  open: boolean;
  onClose: () => void;
  /** Open Transfer Studio with this journal stream as the source. */
  onReplay?: (journalId: string, stream: string) => void;
}

/** Capture-once journals: what was kept, where it lives, replay or delete. */
export function JournalsDrawer({ open, onClose, onReplay }: JournalsDrawerProps) {
  const { toast } = useToast();
  const { confirm } = useConfirm();
  const deleteGate = useWriteGate(PERMISSIONS.jobPlan, "delete journals");
  const [journals, setJournals] = useState<JournalManifest[] | null>(null);
  const [error, setError] = useState("");
  const [deleting, setDeleting] = useState<string | null>(null);

  const load = useCallback(async () => {
    setError("");
    try {
      setJournals(await listJournals());
    } catch (e) {
      setJournals([]);
      setError(e instanceof Error ? e.message : "Could not load journals");
    }
  }, []);

  useEffect(() => {
    if (open) {
      setJournals(null);
      void load();
    }
  }, [open, load]);

  const remove = async (journal: JournalManifest) => {
    const accepted = await confirm({
      title: "Delete journal",
      message: `Deletes this journal (${journalRowsLabel(journal)}${journalRows(journal) > 0 ? " rows" : ""}) from every copy, including shared storage. Runs that already loaded from it are not affected; it can no longer be replayed.`,
      confirmLabel: "Delete",
      cancelLabel: "Cancel",
      tone: "danger",
    });
    if (!accepted) return;
    setDeleting(journal.journal_id);
    try {
      await deleteJournal(journal.journal_id);
      setJournals((prev) => (prev ?? []).filter((j) => j.journal_id !== journal.journal_id));
      toast({ title: "Journal deleted", message: journal.journal_id, tone: "success" });
    } catch (e) {
      toast({ title: "Delete failed", message: e instanceof Error ? e.message : String(e), tone: "error" });
    } finally {
      setDeleting(null);
    }
  };

  const now = Date.now() / 1000;
  return (
    <Drawer
      open={open}
      onClose={onClose}
      title="Source journals"
      subtitle="Extractions kept by runs with Capture source journal on. Replay one to another destination without reading the live source again."
      icon={<DtIcon name="database" size={18} />}
      size="lg"
      className="df2-journals-drawer"
    >
      {journals === null ? (
        <p className="df2-label-hint" role="status">Loading journals…</p>
      ) : error ? (
        <EmptyState
          compact
          role="alert"
          icon="alert"
          title="Journals unavailable"
          description={error}
          action={<Button size="sm" onClick={() => void load()}>Retry</Button>}
        />
      ) : journals.length === 0 ? (
        <EmptyState
          compact
          icon="database"
          title="No journals kept"
          description="Turn on Capture source journal in Destination → Advanced. A successful run then keeps its extraction for replay."
        />
      ) : (
        <div className="df2-jobs-log-table-wrap" data-testid="journals-table">
          <table className="df2-table">
            <thead>
              <tr>
                <th>Journal</th>
                <th>Source</th>
                <th>Rows</th>
                <th>Storage</th>
                <th>Expires</th>
                <th aria-label="Actions" />
              </tr>
            </thead>
            <tbody>
              {journals.map((journal) => {
                const storage = journalStorageBadge(journal.storage);
                const expired = Boolean(journal.expires_at && journal.expires_at < now);
                const streams = journal.streams ?? [];
                return (
                  <tr key={journal.journal_id} data-journal-id={journal.journal_id}>
                    <td><CopyIdChip id={journal.journal_id} label="Journal" compact /></td>
                    <td data-label="Source">
                      {journalSourceLabel(journal)}
                      {streams.length > 1 && <small className="df2-label-hint"> · {streams.length} streams</small>}
                    </td>
                    <td className="df2-cell-mono" data-label="Rows">{journalRowsLabel(journal)}</td>
                    <td data-label="Storage">
                      <span className={`df2-badge ${storage.tone}`} title={storage.detail}>{storage.label}</span>
                    </td>
                    <td data-label="Expires">{expired ? <span className="df2-badge df2-badge-warn">Expired</span> : formatDate(journal.expires_at)}</td>
                    <td>
                      <div className="df2-journals-actions">
                        {onReplay && streams.map((stream) => (
                          <Button
                            key={stream.name}
                            size="sm"
                            variant="primary"
                            disabled={expired}
                            title={expired ? "Expired — capture a fresh run" : `Replay ${stream.name} to a destination`}
                            onClick={() => onReplay(journal.journal_id, streams.length > 1 ? stream.name : "")}
                          >
                            {streams.length > 1 ? `Replay ${stream.name}` : "Replay"}
                          </Button>
                        ))}
                        <Button
                          size="sm"
                          variant="ghost"
                          loading={deleting === journal.journal_id}
                          loadingLabel="Deleting…"
                          disabled={!deleteGate.allowed}
                          title={deleteGate.allowed ? "Delete this journal" : deleteGate.reason}
                          onClick={() => void remove(journal)}
                        >
                          Delete
                        </Button>
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </Drawer>
  );
}
