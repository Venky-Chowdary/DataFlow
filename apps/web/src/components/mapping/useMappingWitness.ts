import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  flagsBySource,
  reviewWitnessFlag,
  runMappingWitness,
  witnessPairsKey,
  type WitnessPair,
  type WitnessRecord,
} from "../../lib/mappingWitness";

export interface MappingWitnessInput {
  mappings: Array<{ source: string; target?: string | null; createNew?: boolean; destType?: string }>;
  sampleRows?: Record<string, unknown>[];
  destEndpoint?: Record<string, unknown> | null;
  targetKeyColumns?: string[];
  destTableExists?: boolean | null;
  dateLocale?: string;
  enabled?: boolean;
}

export interface MappingWitnessState {
  record: WitnessRecord | null;
  flags: Map<string, WitnessPair>;
  loading: boolean;
  error: string;
}

/** Wait this long after the last mapping edit before asking the server again. */
const SETTLE_MS = 600;

/**
 * Runs the Mapping Witness whenever the mapped pairs change, and records a
 * reviewer's answer when a flagged column is remapped. Evidence only: it never
 * edits a mapping.
 */
export function useMappingWitness({
  mappings,
  sampleRows,
  destEndpoint,
  targetKeyColumns,
  destTableExists,
  dateLocale = "",
  enabled = true,
}: MappingWitnessInput): MappingWitnessState {
  const [record, setRecord] = useState<WitnessRecord | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const pairsKey = useMemo(() => witnessPairsKey(mappings), [mappings]);
  const latest = useRef({ mappings, sampleRows, destEndpoint, targetKeyColumns, destTableExists, dateLocale });
  latest.current = { mappings, sampleRows, destEndpoint, targetKeyColumns, destTableExists, dateLocale };
  const rowsReady = Boolean(sampleRows && sampleRows.length > 0);

  useEffect(() => {
    if (!enabled || !rowsReady || !pairsKey) return undefined;
    const controller = new AbortController();
    const timer = window.setTimeout(() => {
      const now = latest.current;
      setLoading(true);
      runMappingWitness(
        {
          mappings: now.mappings
            .filter((m) => m.source && m.target)
            .map((m) => ({
              source: m.source,
              target: String(m.target),
              create_new: Boolean(m.createNew),
              target_type: m.destType || undefined,
            })),
          source_rows: now.sampleRows ?? [],
          destination: now.destEndpoint ?? null,
          target_key_columns: now.targetKeyColumns ?? [],
          destination_table_exists: now.destTableExists ?? null,
          date_locale: now.dateLocale || "",
        },
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
    }, SETTLE_MS);
    return () => {
      window.clearTimeout(timer);
      controller.abort();
    };
  }, [enabled, rowsReady, pairsKey, destTableExists, dateLocale]);

  // A flagged column the reviewer remapped: record "changed" against the report that flagged it.
  const reviewed = useRef(new Set<string>());
  const recordChange = useCallback((digest: string, source: string, newTarget: string) => {
    const id = `${digest}:${source.toLowerCase()}`;
    if (reviewed.current.has(id)) return;
    reviewed.current.add(id);
    reviewWitnessFlag({ mapping_digest: digest, source, decision: "changed", new_target: newTarget }).catch(() => {
      reviewed.current.delete(id);
    });
  }, []);
  const flags = useMemo(() => flagsBySource(record?.report), [record]);
  useEffect(() => {
    if (!record) return;
    for (const m of mappings) {
      const flagged = flags.get(m.source.toLowerCase());
      if (flagged && m.target && m.target.toLowerCase() !== flagged.target.toLowerCase()) {
        recordChange(record.mapping_digest, flagged.source, String(m.target));
      }
    }
  }, [mappings, flags, record, recordChange]);

  // Flags are about the pairs that were judged: hide one once its column is remapped.
  const current = useMemo(() => {
    const out = new Map<string, WitnessPair>();
    for (const m of mappings) {
      const flagged = flags.get(m.source.toLowerCase());
      if (flagged && m.target && m.target.toLowerCase() === flagged.target.toLowerCase()) {
        out.set(m.source.toLowerCase(), flagged);
      }
    }
    return out;
  }, [mappings, flags]);

  return { record, flags: current, loading, error };
}
