import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  buildWitnessRequest,
  flagsBySource,
  reviewWitnessFlag,
  runMappingWitness,
  witnessAutoReady,
  witnessPairsKey,
  type WitnessPair,
  type WitnessResult,
  type WitnessRow,
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
}

export interface MappingWitnessState {
  record: WitnessResult | null;
  flags: Map<string, WitnessPair>;
  loading: boolean;
  error: string;
  /** Run now, reading the destination even on a warehouse (the operator asked). */
  check: () => void;
  /** A warehouse whose rows these pairs were not yet checked against (offer Check mapping). */
  needsCheck: boolean;
}

/** Wait this long after the last mapping edit before asking the server again. */
const SETTLE_MS = 600;

/**
 * Runs the Mapping Witness when the written pairs, the keys, the destination or
 * the sample change — after the destination catalog answered. Those runs are
 * "auto": a warehouse is then not read (source values only) until the operator
 * asks with Check mapping. Records a reviewer's answer when a flagged column is
 * remapped. Evidence only: it never edits a mapping.
 */
export function useMappingWitness(input: MappingWitnessInput): MappingWitnessState {
  const [record, setRecord] = useState<WitnessResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  // An explicit check applies to the pairs it was asked for; an edit goes back to "only when asked".
  const [explicitFor, setExplicitFor] = useState<{ key: string; n: number } | null>(null);
  const pairsKey = useMemo(() => witnessPairsKey(input.rows), [input.rows]);
  const keysKey = (input.targetKeyColumns ?? []).join("␞");
  const candidatesKey = (input.candidateColumns ?? []).join("␞");
  const destKey = useMemo(() => JSON.stringify(input.destEndpoint ?? null), [input.destEndpoint]);
  const rowCount = input.sampleRows?.length ?? 0;
  const latest = useRef(input);
  latest.current = input;

  const auto = witnessAutoReady({
    rowsReady: rowCount > 0,
    pairsKey,
    catalogReady: input.catalogReady,
  });

  const explicit = explicitFor !== null && explicitFor.key === pairsKey;
  useEffect(() => {
    if (!(auto || (explicit && rowCount > 0 && pairsKey))) return undefined;
    const controller = new AbortController();
    const timer = window.setTimeout(() => {
      const now = latest.current;
      setLoading(true);
      runMappingWitness(
        buildWitnessRequest({
          rows: now.rows,
          sampleRows: now.sampleRows,
          destEndpoint: now.destEndpoint,
          targetKeyColumns: now.targetKeyColumns,
          candidateColumns: now.candidateColumns,
          destTableExists: now.destTableExists,
          dateLocale: now.dateLocale,
          trigger: explicit ? "explicit" : "auto",
        }),
        controller.signal,
      )
        .then((next) => {
          setRecord(next);
          setError("");
        })
        .catch((e: unknown) => {
          if (controller.signal.aborted) return;
          setError(e instanceof Error ? e.message : "The mapping check could not run");
        })
        .finally(() => {
          if (!controller.signal.aborted) setLoading(false);
        });
    }, explicit ? 0 : SETTLE_MS);
    return () => {
      window.clearTimeout(timer);
      controller.abort();
    };
  }, [auto, explicit, explicitFor, pairsKey, keysKey, candidatesKey, destKey, rowCount, input.destTableExists,
    input.dateLocale]);

  // A flagged column the reviewer remapped: record "changed" against the report that flagged it.
  const reviewed = useRef(new Set<string>());
  const recordChange = useCallback((digest: string, source: string, newTarget: string) => {
    const id = `${digest}:${source.toLowerCase()}:${newTarget.toLowerCase()}`;
    if (reviewed.current.has(id)) return;
    reviewed.current.add(id);
    reviewWitnessFlag({
      mapping_digest: digest,
      destination: latest.current.destEndpoint ?? null,
      source,
      decision: "changed",
      new_target: newTarget,
    }).catch(() => {
      reviewed.current.delete(id);
    });
  }, []);
  const flags = useMemo(() => flagsBySource(record?.report), [record]);
  useEffect(() => {
    if (!record) return;
    for (const m of input.rows) {
      const flagged = flags.get(m.source.toLowerCase());
      if (flagged && m.target && !m.omitted && m.target.toLowerCase() !== flagged.target.toLowerCase()) {
        recordChange(record.mapping_digest, flagged.source, String(m.target));
      }
    }
  }, [input.rows, flags, record, recordChange]);

  // Flags are about the pairs that were judged: hide one once its column is remapped.
  const current = useMemo(() => {
    const out = new Map<string, WitnessPair>();
    for (const m of input.rows) {
      const flagged = flags.get(m.source.toLowerCase());
      if (flagged && m.target && !m.omitted && m.target.toLowerCase() === flagged.target.toLowerCase()) {
        out.set(m.source.toLowerCase(), flagged);
      }
    }
    return out;
  }, [input.rows, flags]);

  const check = useCallback(
    () => setExplicitFor((prev) => ({ key: pairsKey, n: (prev?.n ?? 0) + 1 })),
    [pairsKey],
  );
  return { record, flags: current, loading, error, check, needsCheck: input.warehouse && !explicit };
}
