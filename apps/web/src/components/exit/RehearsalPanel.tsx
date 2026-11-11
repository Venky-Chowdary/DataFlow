/**
 * Page B's data: loads one rehearsal, re-reads it while it runs (bounded),
 * and runs one action at a time (refresh, sign, export, verify). A response
 * only replaces what is on screen when it is at least as new (version-aware),
 * and a conflict re-reads so the page shows the truth.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import { Button } from "../ui/Button";
import { EmptyState } from "../ui/EmptyState";
import { LoadingBlock } from "../LoadingState";
import { useToast } from "../Toast";
import { RehearsalView, type RehearsalBusy } from "./RehearsalView";
import {
  downloadExitEvidenceMarkdown,
  fetchExitEvidencePack,
  fetchExitRehearsal,
  signExitRehearsal,
  verifyExitEvidence,
} from "../../lib/exitApi";
import { downloadBlob } from "../../lib/downloadBlob";
import {
  REHEARSAL_POLL_MS,
  exitErrorMessage,
  newerRehearsal,
  shouldPollRehearsal,
  type ExitRehearsal,
  type ExitVerifyResult,
} from "../../lib/exitReadiness";
import { roleLabel } from "../../lib/parity";
import { PERMISSIONS, useWriteGate } from "../../lib/PermissionsContext";

export interface RehearsalPanelProps {
  rehearsalId: string;
  onBack: () => void;
  onOpenJob?: (jobId: string) => void;
  /** A rehearsal that finished or was signed changes the board; the page reloads it. */
  onChanged?: () => void;
}

export function RehearsalPanel({ rehearsalId, onBack, onOpenJob, onChanged }: RehearsalPanelProps) {
  const { toast } = useToast();
  // Signing attests the rehearsal: job.manage, and the API binds each role to the signer's workspace role.
  const signGate = useWriteGate(PERMISSIONS.jobManage, "sign off an exit rehearsal");
  const [rehearsal, setRehearsal] = useState<ExitRehearsal | null>(null);
  const [loadError, setLoadError] = useState("");
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState<RehearsalBusy>(null);
  const [errors, setErrors] = useState<{ signoff?: string; verify?: string }>({});
  const [verifyResult, setVerifyResult] = useState<ExitVerifyResult | null>(null);
  const [pollStopped, setPollStopped] = useState(false);
  const idRef = useRef(rehearsalId);
  idRef.current = rehearsalId;
  const currentRef = useRef<ExitRehearsal | null>(null);
  currentRef.current = rehearsal;
  const busyRef = useRef(false);
  const changedRef = useRef(onChanged);
  changedRef.current = onChanged;

  const accept = useCallback((forId: string, next: ExitRehearsal) => {
    if (idRef.current !== forId) return;
    setRehearsal((current) => {
      const merged = newerRehearsal(current, next);
      // Finishing or signing moves the board's figures.
      if (current && (current.verdict !== merged.verdict || current.signoff.status !== merged.signoff.status)) {
        changedRef.current?.();
      }
      return merged;
    });
  }, []);

  const load = useCallback(
    async (signal?: AbortSignal) => {
      const forId = rehearsalId;
      try {
        const next = await fetchExitRehearsal(forId, { signal });
        accept(forId, next);
        if (idRef.current === forId) setLoadError("");
      } catch (err) {
        if (signal?.aborted || idRef.current !== forId) return;
        setLoadError(exitErrorMessage(err, "Could not load this rehearsal."));
      }
    },
    [rehearsalId, accept],
  );

  useEffect(() => {
    setRehearsal(null);
    setErrors({});
    setVerifyResult(null);
    setLoadError("");
    setBusy(null);
    busyRef.current = false;
    const controller = new AbortController();
    setLoading(true);
    void load(controller.signal).finally(() => {
      if (!controller.signal.aborted) setLoading(false);
    });
    return () => controller.abort();
  }, [rehearsalId, load]);

  // A running rehearsal settles on the server: re-read until it has, bounded.
  const running = rehearsal?.verdict === "running";
  const [pollEpoch, setPollEpoch] = useState(0);
  useEffect(() => {
    setPollStopped(false);
    if (!running) return;
    let reads = 0;
    const timer = setInterval(() => {
      if (busyRef.current) return;
      reads += 1;
      if (!shouldPollRehearsal(currentRef.current, reads)) {
        clearInterval(timer);
        if (currentRef.current?.verdict === "running") setPollStopped(true);
        return;
      }
      void load();
    }, REHEARSAL_POLL_MS);
    return () => clearInterval(timer);
  }, [running, load, pollEpoch]);

  const run = useCallback(
    async (next: Exclude<RehearsalBusy, null>, area: "signoff" | "verify" | null, call: () => Promise<ExitRehearsal | void>, success?: string) => {
      if (busyRef.current) return;
      busyRef.current = true;
      const forId = rehearsalId;
      const before = currentRef.current?.version ?? -1;
      setBusy(next);
      if (area) setErrors((e) => ({ ...e, [area]: undefined }));
      try {
        const result = await call();
        if (result) {
          accept(forId, result);
          if (result.version <= before) await load();
        }
        if (success && idRef.current === forId) toast({ title: success, tone: "success" });
      } catch (err) {
        if (idRef.current !== forId) return;
        const fallback =
          area === "signoff"
            ? "Could not record your sign-off."
            : area === "verify"
              ? "Could not verify the evidence pack."
              : next.kind === "export"
                ? "Could not export the evidence report."
                : next.kind === "pack"
                  ? "Could not download the signed pack."
                  : "Could not refresh the rehearsal.";
        const message = exitErrorMessage(err, fallback);
        if (area) setErrors((e) => ({ ...e, [area]: message }));
        else toast({ title: fallback, message, tone: "error" });
        if ((err as { status?: number })?.status === 409) await load();
      } finally {
        busyRef.current = false;
        if (idRef.current === forId) setBusy(null);
      }
    },
    [rehearsalId, accept, load, toast],
  );

  const verify = (pack: unknown) =>
    run({ kind: "verify" }, "verify", async () => {
      setVerifyResult(null);
      const result = await verifyExitEvidence(pack);
      if (idRef.current === rehearsalId) setVerifyResult(result);
    });

  if (!rehearsal && (loading || !loadError)) {
    return <LoadingBlock title="Loading the rehearsal" hint="Reading its timeline, completeness and seal." size="sm" />;
  }
  if (!rehearsal) {
    return (
      <EmptyState
        compact
        icon="alert"
        role="alert"
        title="Could not load this rehearsal"
        description={loadError || "The rehearsal is not available. Try again."}
        action={
          <div className="df2-parity-actions">
            <Button size="sm" onClick={() => { setLoading(true); void load().finally(() => setLoading(false)); }}>Try again</Button>
            <Button size="sm" variant="ghost" onClick={onBack}>Back to exit readiness</Button>
          </div>
        }
      />
    );
  }

  const fileBase = `datawrap-exit-evidence-${rehearsal.system_name.replace(/[^A-Za-z0-9_-]+/g, "-").toLowerCase()}-r${rehearsal.n}`;

  return (
    <RehearsalView
      rehearsal={rehearsal}
      busy={busy}
      canSign={signGate.allowed}
      signDeniedReason={signGate.reason}
      errors={errors}
      verifyResult={verifyResult}
      pollStopped={pollStopped}
      onBack={onBack}
      onOpenJob={onOpenJob}
      onRefresh={() => {
        if (pollStopped) setPollEpoch((n) => n + 1);
        void run({ kind: "refresh" }, null, () => fetchExitRehearsal(rehearsalId));
      }}
      onSign={(role, note) =>
        void run({ kind: "sign" }, "signoff", () => signExitRehearsal(rehearsalId, role, note, rehearsal.version), `Signed as ${roleLabel(role)}`)
      }
      onExportMarkdown={() =>
        void run({ kind: "export" }, null, async () => {
          downloadBlob(`${fileBase}.md`, await downloadExitEvidenceMarkdown(rehearsalId));
        })
      }
      onDownloadSigned={() =>
        void run({ kind: "pack" }, null, async () => {
          const pack = await fetchExitEvidencePack(rehearsalId);
          downloadBlob(`${fileBase}.json`, new Blob([JSON.stringify(pack, null, 2)], { type: "application/json" }));
        })
      }
      onVerifySigned={() =>
        void run({ kind: "verify" }, "verify", async () => {
          setVerifyResult(null);
          const pack = await fetchExitEvidencePack(rehearsalId);
          const result = await verifyExitEvidence(pack);
          if (idRef.current === rehearsalId) setVerifyResult(result);
        })
      }
      onVerifyFile={(file) => {
        void (async () => {
          let pack: unknown;
          try {
            pack = JSON.parse(await file.text());
          } catch {
            setErrors((e) => ({ ...e, verify: `${file.name} is not a JSON file, so it cannot be an Exit Evidence Pack.` }));
            setVerifyResult(null);
            return;
          }
          await verify(pack);
        })();
      }}
    />
  );
}
