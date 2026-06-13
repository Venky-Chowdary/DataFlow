import { useCallback, useEffect, useMemo, useState } from "react";
import {
  AlertBanner,
  Button,
  CheckpointTimeline,
  ColumnMappingReviewPanel,
  DatabaseEndpointPanel,
  GateProgressBar,
  LoadingState,
  PreflightGateList,
  PreflightGateRail,
  ProgressBar,
  ReconciliationReport,
  TransferCta,
  TransferProgress,
  useToast,
  type CredentialFields,
  type GateItem,
  type ReconciliationData,
} from "@dataflow/design-system";
import {
  endpointSummary,
  inferOperation,
  operationLabel,
  runPreflight,
  startTransfer,
  type MappingResult,
  type PreflightResponse,
} from "../lib/api";
import { detectDatabaseType } from "../lib/connectionString";
import { emptyCredentials } from "../lib/samples";
import { fromApiMappings } from "../lib/transfer/ColumnMappingReview";
import { TransferExecutionService } from "../lib/transfer/TransferExecutionService";
import { TransferSelectionValidator } from "../lib/transfer/TransferSelectionValidator";
import { TRANSFER_TEMPLATES } from "../lib/transferModes";
import { useJobPoll } from "../lib/useJobPoll";
import type { TransferDraft } from "./TransferSelectScreen";
import { DATABASE_OPTIONS } from "../lib/types";

const GATE_LABELS: Record<string, string> = {
  g1_source: "G1 Source ready",
  g2_destination: "G2 Destination ready",
  g3_schema_contract: "G3 Schema & types",
  g4_mapping_confidence: "G4 Semantic mapping",
  g5_dry_run: "G5 Dry-run transform",
  g6_target_ddl: "G6 Target DDL",
  g7_capacity: "G7 Capacity",
  g8_reconciliation: "G8 Reconciliation",
};

const executionService = new TransferExecutionService();
const validator = new TransferSelectionValidator();

function toGateItems(pf: PreflightResponse): GateItem[] {
  return pf.gates.map((g) => ({
    id: g.gate_id,
    label: GATE_LABELS[g.gate_id] ?? g.gate_id,
    status: g.status as GateItem["status"],
    message: g.message,
    durationMs: g.duration_ms,
  }));
}

interface TransferScreenProps {
  draft: TransferDraft;
  onBack: () => void;
}

export function TransferScreen({ draft, onBack }: TransferScreenProps) {
  const { toast } = useToast();
  const template = TRANSFER_TEMPLATES.find((t) => t.id === draft.templateId) ?? TRANSFER_TEMPLATES[0];
  const needs = executionService.needsConnection(template);

  const [source, setSource] = useState(draft.source);
  const [destination, setDestination] = useState(draft.destination);
  const [sourceConnStr, setSourceConnStr] = useState("");
  const [destConnStr, setDestConnStr] = useState("");
  const [sourceCreds, setSourceCreds] = useState<CredentialFields>(() => emptyCredentials(draft.sourceDbType || "postgresql"));
  const [destCreds, setDestCreds] = useState<CredentialFields>(() => emptyCredentials(draft.destDbType || "snowflake"));

  const [prepared, setPrepared] = useState(false);
  const [preparing, setPreparing] = useState(false);
  const [prepareError, setPrepareError] = useState<string | null>(null);
  const [prepareMessage, setPrepareMessage] = useState("");

  const [mappings, setMappings] = useState<MappingResult[]>([]);
  const [preflight, setPreflight] = useState<PreflightResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [transferStatus, setTransferStatus] = useState<"idle" | "running" | "completed" | "blocked">("idle");
  const [error, setError] = useState<string | null>(null);
  const [jobId, setJobId] = useState<string | null>(null);

  const operation = inferOperation(source, destination);
  const { job, isComplete, isFailed } = useJobPoll(jobId, { enabled: transferStatus === "running" });

  const review = useMemo(() => fromApiMappings(mappings), [mappings]);
  const mappingRows = review.displayRows("all");
  const stats = review.stats;

  const targetOptions = useMemo(() => {
    const cols = destination.database?.targetColumns.map((c) => c.name) ?? [];
    const mapped = mappings.map((m) => m.target);
    return [...new Set([...cols, ...mapped])];
  }, [destination, mappings]);

  const runPrepare = useCallback(async () => {
    const connCheck = validator.validateStep2Connections(template, sourceConnStr, destConnStr);
    if (!connCheck.ok && (needs.source || needs.dest)) {
      const hasCreds =
        (needs.source && sourceCreds.username && sourceCreds.host) ||
        (needs.dest && destCreds.username && destCreds.host) ||
        sourceConnStr ||
        destConnStr ||
        draft.destConnectorId;
      if (!hasCreds) {
        setPrepareError(connCheck.message ?? "Connect databases to continue");
        return;
      }
    }

    setPreparing(true);
    setPrepareError(null);
    try {
      const result = await executionService.prepare(template, draft, {
        sourceConnStr,
        destConnStr,
        sourceCreds,
        destCreds,
      }, setPrepareMessage);

      if (result.error) throw new Error(result.error);
      setSource(result.source);
      setDestination(result.destination);
      setMappings(result.identityMappings);
      setPrepared(true);
      toast({ title: "Analysis complete", message: `${result.identityMappings.length} columns mapped`, tone: "success" });
    } catch (e) {
      setPrepareError(e instanceof Error ? e.message : "Connection failed");
    } finally {
      setPreparing(false);
    }
  }, [template, draft, sourceConnStr, destConnStr, sourceCreds, destCreds, needs, toast]);

  useEffect(() => {
    if (template.id === "file-file" && draft.source.file.fileName) {
      setMappings(
        draft.source.file.columns.map((c) => ({
          source: c.name,
          target: c.name,
          confidence: 1,
          reasoning: "Format conversion",
        }))
      );
      setPrepared(true);
    }
  }, [template.id, draft.source]);

  useEffect(() => {
    if (isComplete && job) {
      setTransferStatus("completed");
      toast({ title: "Transfer complete", message: `${job.rows_processed.toLocaleString()} rows`, tone: "success" });
    }
    if (isFailed && job) {
      setTransferStatus("blocked");
      setError(job.message);
    }
  }, [isComplete, isFailed, job, toast]);

  const handleTargetChange = useCallback((sourceCol: string, newTarget: string) => {
    setMappings((prev) =>
      prev.map((m) =>
        m.source === sourceCol
          ? { ...m, target: newTarget, user_override: true, confidence: 1, reasoning: "User override" }
          : m
      )
    );
    setPreflight(null);
  }, []);

  const handleConfirmRow = useCallback((sourceCol: string) => {
    setMappings((prev) =>
      prev.map((m) =>
        m.source === sourceCol
          ? { ...m, user_override: true, confidence: 1, reasoning: "Confirmed by user" }
          : m
      )
    );
    setPreflight(null);
  }, []);

  async function loadPreflight() {
    if (stats.needsReview > 0) {
      toast({ title: "Review required", message: `Confirm ${stats.needsReview} low-confidence column(s) first`, tone: "error" });
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const pf = await runPreflight(source, destination, mappings);
      setPreflight(pf);
      toast({
        title: pf.passed ? "Preflight passed" : "Preflight blocked",
        message: `${pf.passed_count}/${pf.total_gates} gates`,
        tone: pf.passed ? "success" : "error",
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : "Preflight failed");
    } finally {
      setLoading(false);
    }
  }

  async function handleTransfer() {
    if (stats.needsReview > 0) {
      toast({ title: "Review columns first", message: `${stats.needsReview} column(s) need confirmation`, tone: "error" });
      return;
    }
    let pf = preflight;
    if (!pf) {
      try {
        pf = await runPreflight(source, destination, mappings);
        setPreflight(pf);
      } catch (e) {
        setError(e instanceof Error ? e.message : "Preflight failed");
        return;
      }
    }
    if (!pf.passed) {
      setTransferStatus("blocked");
      setError(pf.blockers[0]?.message ?? "Preflight blocked");
      return;
    }

    setTransferStatus("running");
    setError(null);
    try {
      const result = await startTransfer(source, destination, mappings);
      setJobId(result.job_id);
    } catch (e) {
      setTransferStatus("blocked");
      setError(e instanceof Error ? e.message : "Transfer blocked");
    }
  }

  const gateItems = preflight ? toGateItems(preflight) : [];
  const reconciliation = (job?.reconciliation ?? null) as ReconciliationData | null;

  return (
    <div className="df-transfer-execute">
      <PreflightGateRail passedCount={preflight?.passed_count ?? 0} />

      <div className="df-transfer-execute-summary">
        <span className="df-transfer-execute-op">{operationLabel(operation)}</span>
        <span className="df-transfer-execute-path">
          {endpointSummary(source)} → {endpointSummary(destination)}
        </span>
      </div>

      {(needs.source || needs.dest) && !prepared && (
        <section className="df-execute-section">
          <h2 className="df-execute-section-title">Connect</h2>
          <p className="df-execute-section-desc">Enter credentials for your selected databases.</p>
          <div className="df-execute-connect-panels">
            {needs.source && (
              <DatabaseEndpointPanel
                label="Source"
                hint={`${draft.sourceDbType || "Database"} connection`}
                accent="orange"
                connectionString={sourceConnStr}
                onConnectionStringChange={setSourceConnStr}
                credentials={sourceCreds}
                onCredentialsChange={setSourceCreds}
                databaseOptions={DATABASE_OPTIONS}
                dbTypeLabel={detectDatabaseType(sourceConnStr || draft.sourceDbType).toUpperCase()}
              />
            )}
            {needs.dest && (
              <DatabaseEndpointPanel
                label="Destination"
                hint={`${draft.destDbType || "Database"} connection`}
                accent="mint"
                connectionString={destConnStr}
                onConnectionStringChange={setDestConnStr}
                credentials={destCreds}
                onCredentialsChange={setDestCreds}
                databaseOptions={DATABASE_OPTIONS}
                dbTypeLabel={detectDatabaseType(destConnStr || draft.destDbType).toUpperCase()}
              />
            )}
          </div>
          {prepareError && <AlertBanner variant="danger" message={prepareError} />}
          {preparing && <ProgressBar indeterminate label={prepareMessage || "Connecting…"} tone="brand" />}
          <Button variant="primary" disabled={preparing} onClick={() => void runPrepare()}>
            Connect & analyze columns
          </Button>
        </section>
      )}

      {prepared && (
        <section className="df-execute-section">
          <h2 className="df-execute-section-title">Column mapping</h2>
          <p className="df-execute-section-desc">
            AI mapped {stats.autoMapped + stats.overridden} of {stats.total} columns.
            {stats.needsReview > 0 ? ` Review ${stats.needsReview} below.` : " All columns are ready."}
          </p>
          {mappings.length === 0 ? (
            <LoadingState label="Loading mappings…" />
          ) : (
            <ColumnMappingReviewPanel
              rows={mappingRows}
              stats={stats}
              targetOptions={targetOptions.length ? targetOptions : mappings.map((m) => m.target)}
              onTargetChange={handleTargetChange}
              onConfirmRow={handleConfirmRow}
            />
          )}
        </section>
      )}

      {prepared && (
        <section className="df-execute-section">
          <h2 className="df-execute-section-title">Preflight</h2>
          {!preflight ? (
            <Button variant="primary" onClick={() => void loadPreflight()} disabled={loading || stats.needsReview > 0}>
              Run 8-gate preflight
            </Button>
          ) : (
            <>
              <GateProgressBar passed={preflight.passed_count} total={preflight.total_gates} />
              <PreflightGateList gates={gateItems} />
            </>
          )}
        </section>
      )}

      {(transferStatus !== "idle" || job) && (
        <section className="df-execute-section">
          <h2 className="df-execute-section-title">Transfer</h2>
          <TransferProgress
            currentChunk={job?.current_chunk ?? 0}
            totalChunks={job?.total_chunks ?? 1}
            rowsProcessed={job?.rows_processed ?? 0}
            status={transferStatus}
            message={jobId ? `Job ${jobId}` : undefined}
          />
          {job && job.checkpoints.length > 0 && (
            <CheckpointTimeline
              checkpoints={job.checkpoints}
              currentChunk={job.current_chunk}
              totalChunks={job.total_chunks}
              status={job.status}
            />
          )}
        </section>
      )}

      {reconciliation && transferStatus === "completed" && (
        <section className="df-execute-section">
          <ReconciliationReport report={reconciliation} tableName={job?.table_name || undefined} />
        </section>
      )}

      {error && <AlertBanner variant="danger" message={error} />}

      <div className="df-action-bar df-action-bar--sticky df-action-bar--split">
        <Button variant="ghost" onClick={onBack}>Back</Button>
        <TransferCta
          onClick={() => void handleTransfer()}
          disabled={!prepared || loading || stats.needsReview > 0 || (preflight !== null && !preflight.passed)}
          loading={transferStatus === "running"}
        />
      </div>
    </div>
  );
}
