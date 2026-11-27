import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  buildWitnessRequest,
  flagsBySource,
  reviewWitnessFlag,
  runMappingWitness,
  witnessAutoReady,
  witnessInputsKey,
  witnessPairsKey,
  type WitnessPair,
  type WitnessResult,
  type WitnessRow,
  type WitnessSourceRef,
} from "../../lib/mappingWitness";

export interface MappingWitnessInput {
  rows: WitnessRow[];
  sampleRows?: Record<string, unknown>[];
  destEndpoint?: Record<string, unknown> | null;
  targetKeyColumns?: string[];
  candidateColumns?: string[];
  destTableExists?: boolean | null;
  dateLocale?: string;
  /** The destination catalog has answered (or there is none to wait for). */
  catalogReady: boolean;
  /** A warehouse destination: the server reads it only on an explicit Check mapping. */
  warehouse: boolean;
  /** The Map session (plan id or minted id) the evidence belongs to. */
  sessionId: string;
  /** What the sample came from: the file name, or the table. */
  source: WitnessSourceRef;
}

export interface MappingWitnessState {
  record: WitnessResult | null;
  flags: Map<string, WitnessPair>;
  /** Flag ids a person kept with "Keep this mapping" in this session. */
  kept: Set<string>;
  loading: boolean;
  error: string;
  /** Run now, reading the destination even on a warehouse (the operator asked). */
  check: () => void;
  /** Run the last kind of check again after a failure. */
  retry: () => void;
  /** "Keep this mapping" on one flag: recorded with the flag's id. */
  keep: (pair: WitnessPair) => Promise<void>;
  /** On Continue: record "not reviewed (continued)" for every flag still unreviewed. */
  continuePastFlags: () => Promise<void>;
  /** A warehouse whose rows the current inputs were not yet checked against (offer Check mapping). */
  needsCheck: boolean;
}

/** Wait this long after the last mapping edit before asking the server again. */
const SETTLE_MS = 600;

/**
 * Runs the Mapping Witness when anything it reads changes — after the
 * destination catalog answered. Those runs are "auto": a warehouse is never read
 * by them (source values only); only Check mapping reads it, once, for the
 * inputs it was asked for. Errors and timeouts stay visible until a retry
 * succeeds. Evidence only: it never edits a mapping.
 */
export function useMappingWitness(input: MappingWitnessInput): MappingWitnessState {
  const [record, setRecord] = useState<WitnessResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [kept, setKept] = useState<Set<string>>(() => new Set());
  const [explicitNonce, setExplicitNonce] = useState(0);
  const [autoNonce, setAutoNonce] = useState(0);
  // The inputs an explicit warehouse check last succeeded for.
  const [explicitOkFor, setExplicitOkFor] = useState("");
  const lastTrigger = useRef<"auto" | "explicit">("auto");
  const pairsKey = useMemo(() => witnessPairsKey(input.rows), [input.rows]);
  const latest = useRef(input);
  latest.current = input;

  const request = (trigger: "auto" | "explicit") => {
    const now = latest.current;
    return buildWitnessRequest({
      rows: now.rows,
      sampleRows: now.sampleRows,
      destEndpoint: now.destEndpoint,
      targetKeyColumns: now.targetKeyColumns,
      candidateColumns: now.candidateColumns,
      destTableExists: now.destTableExists,
      dateLocale: now.dateLocale,
      trigger,
      sessionId: now.sessionId,
      source: now.source,
    });
  };
  const inputsKey = witnessInputsKey(request("auto"));
  const rowCount = input.sampleRows?.length ?? 0;
  const auto = witnessAutoReady({ rowsReady: rowCount > 0, pairsKey, catalogReady: input.catalogReady });
  const explicitStands = input.warehouse && explicitOkFor === inputsKey;

  const run = useCallback((trigger: "auto" | "explicit", controller: AbortController) => {
    const body = request(trigger);
    const key = witnessInputsKey(body);
    lastTrigger.current = trigger;
    setLoading(true);
    runMappingWitness(body, controller.signal)
      .then((next) => {
        setRecord(next);
        setError("");
        if (trigger === "explicit") setExplicitOkFor(key);
      })
      .catch((e: unknown) => {
        if (controller.signal.aborted) return;
        setError(e instanceof Error ? e.message : "The mapping check could not run");
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
  }, []);

  // Auto runs: on any input change, never over a warehouse result that still stands.
  useEffect(() => {
    if (!auto || explicitStands) return undefined;
    const controller = new AbortController();
    const timer = window.setTimeout(() => run("auto", controller), SETTLE_MS);
    return () => {
      window.clearTimeout(timer);
      controller.abort();
    };
  }, [auto, explicitStands, inputsKey, autoNonce, run]);

  // Explicit runs: only when asked.
  useEffect(() => {
    if (!explicitNonce) return undefined;
    const controller = new AbortController();
    run("explicit", controller);
    return () => controller.abort();
  }, [explicitNonce, run]);

  // A flagged column the reviewer remapped: record "changed" against the report that flagged it.
  const reviewed = useRef(new Set<string>());
  const recordChange = useCallback((digest: string, source: string, newTarget: string) => {
    const id = `${digest}:${source.toLowerCase()}:${newTarget.toLowerCase()}`;
    if (reviewed.current.has(id) || !latest.current.sessionId) return;
    reviewed.current.add(id);
    reviewWitnessFlag({
      mapping_digest: digest,
      session_id: latest.current.sessionId,
      destination: latest.current.destEndpoint ?? null,
      source,
      decision: "changed",
      new_target: newTarget,
    }).catch(() => {
      reviewed.current.delete(id);
    });
  }, []);
  const allFlags = useMemo(() => flagsBySource(record?.report), [record]);
  useEffect(() => {
    if (!record) return;
    for (const m of input.rows) {
      const flagged = allFlags.get(m.source.toLowerCase());
      if (flagged && m.target && !m.omitted && m.target.toLowerCase() !== flagged.target.toLowerCase()) {
        recordChange(record.mapping_digest, flagged.source, String(m.target));
      }
    }
  }, [input.rows, allFlags, record, recordChange]);

  // Flags are about the pairs that were judged: hide one once its column is remapped.
  const current = useMemo(() => {
    const out = new Map<string, WitnessPair>();
    for (const m of input.rows) {
      const flagged = allFlags.get(m.source.toLowerCase());
      if (flagged && m.target && !m.omitted && m.target.toLowerCase() === flagged.target.toLowerCase()) {
        out.set(m.source.toLowerCase(), flagged);
      }
    }
    return out;
  }, [input.rows, allFlags]);

  const answer = useCallback(async (pair: WitnessPair, decision: "kept" | "continued") => {
    const rec = record;
    if (!rec || !pair.flag?.id || !latest.current.sessionId) return;
    await reviewWitnessFlag({
      mapping_digest: rec.mapping_digest,
      session_id: latest.current.sessionId,
      destination: latest.current.destEndpoint ?? null,
      source: pair.source,
      decision,
      flag_id: pair.flag.id,
    });
  }, [record]);

  const keep = useCallback(async (pair: WitnessPair) => {
    await answer(pair, "kept");
    if (pair.flag?.id) setKept((prev) => new Set(prev).add(pair.flag!.id!));
  }, [answer]);

  const continuePastFlags = useCallback(async () => {
    const open = [...current.values()].filter((p) => p.flag?.id && !kept.has(p.flag.id));
    await Promise.all(open.map((p) => answer(p, "continued").catch(() => undefined)));
  }, [current, kept, answer]);

  const check = useCallback(() => setExplicitNonce((n) => n + 1), []);
  const retry = useCallback(() => {
    if (lastTrigger.current === "explicit") setExplicitNonce((n) => n + 1);
    else setAutoNonce((n) => n + 1);
  }, []);

  return {
    record,
    flags: current,
    kept,
    loading,
    error,
    check,
    retry,
    keep,
    continuePastFlags,
    needsCheck: input.warehouse && !explicitStands,
  };
}
