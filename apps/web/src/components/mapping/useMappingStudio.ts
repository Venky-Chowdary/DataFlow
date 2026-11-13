import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  catalogFromNames,
  destColumnFacts,
  destinationColumnStates,
  remapToDestination,
  summarizeMapTargets,
  targetConflicts,
  type DestColumnCatalog,
} from "../../lib/destColumnCatalog";
import { isIntentionalOmit, type EditableMapping } from "../../lib/mapping";
import { dndKeyAction, dndReduce, IDLE, type DndAction, type DndState, type DndTarget } from "../../lib/mappingDnd";
import { emptyHistory, historyKey, recordEdit, redoStep, undoStep, type History } from "../../lib/mappingHistory";
import {
  fetchDestinationColumns,
  fetchTypeCompatibility,
  type AutoMapSuggestion,
  type TypeGrade,
} from "../../lib/mappingStudioApi";

function gradeKey(src: string, tgt: string, transform: string): string {
  return `${src}\u0000${tgt}\u0000${transform}`;
}

/**
 * The destination catalog for Map: read from the API once per destination,
 * with the names/types Studio already holds standing in until it answers.
 */
export function useDestinationCatalog(opts: {
  endpoint: Record<string, unknown> | null | undefined;
  probeKey: string;
  schemaPolicy: string;
  names: string[];
  schema: Record<string, string>;
  tableExists: boolean | null;
  destType: string;
  enabled: boolean;
}) {
  const { endpoint, probeKey, schemaPolicy, names, schema, tableExists, destType, enabled } = opts;
  const [catalog, setCatalog] = useState<DestColumnCatalog | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [nonce, setNonce] = useState(0);
  const endpointRef = useRef(endpoint);
  endpointRef.current = endpoint;
  const fallback = useMemo(
    () => catalogFromNames(names, schema, tableExists, destType),
    [names, schema, tableExists, destType],
  );

  useEffect(() => {
    setCatalog(null);
    setError("");
    if (!enabled || !endpointRef.current || tableExists === false) return;
    let live = true;
    setLoading(true);
    fetchDestinationColumns(endpointRef.current, schemaPolicy)
      .then((cat) => {
        if (live) setCatalog(cat);
      })
      .catch((e: unknown) => {
        if (live) setError(e instanceof Error ? e.message : "Could not read the destination's columns.");
      })
      .finally(() => {
        if (live) setLoading(false);
      });
    return () => {
      live = false;
    };
  }, [probeKey, schemaPolicy, enabled, tableExists, nonce]);

  // The API catalog is authoritative once read; an empty read keeps the names.
  const effective = catalog && (catalog.columns.length > 0 || names.length === 0) ? catalog : fallback;
  return { catalog: effective, loading, error, reload: () => setNonce((n) => n + 1) };
}

/** Live grades for every mapped row, from the API grader (debounced, cached). */
export function useTypeGrades(mappings: EditableMapping[], destType: string, tableExists: boolean | null) {
  const cache = useRef(new Map<string, TypeGrade>());
  const [version, setVersion] = useState(0);
  const [pending, setPending] = useState(false);

  useEffect(() => {
    cache.current = new Map();
    setVersion((v) => v + 1);
  }, [destType, tableExists]);

  const wanted = useMemo(() => {
    const out = new Map<string, { sourceType: string; targetType: string; transform: string }>();
    for (const m of mappings) {
      if (isIntentionalOmit(m) || !m.target) continue;
      const src = m.inferredType || "";
      const tgt = m.destType || "";
      if (!src || !tgt) continue;
      const transform = m.engineTransform || "none";
      out.set(gradeKey(src, tgt, transform), { sourceType: src, targetType: tgt, transform });
    }
    return out;
  }, [mappings]);

  useEffect(() => {
    const missing = Array.from(wanted.entries()).filter(([k]) => !cache.current.has(k));
    if (missing.length === 0) return;
    let live = true;
    const timer = window.setTimeout(() => {
      setPending(true);
      fetchTypeCompatibility(missing.slice(0, 2000).map(([, p]) => p), destType, tableExists)
        .then((grades) => {
          if (!live) return;
          grades.forEach((g, i) => {
            const key = missing[i]?.[0];
            if (key) cache.current.set(key, g);
          });
          setVersion((v) => v + 1);
        })
        .catch(() => {
          /* The chip stays "unread"; Validate still grades every pair. */
        })
        .finally(() => {
          if (live) setPending(false);
        });
    }, 250);
    return () => {
      live = false;
      window.clearTimeout(timer);
    };
  }, [wanted, destType, tableExists]);

  const gradeFor = useCallback(
    (m: EditableMapping): TypeGrade | undefined => {
      if (!m.inferredType || !m.destType || !m.target) return undefined;
      return cache.current.get(gradeKey(m.inferredType, m.destType, m.engineTransform || "none"));
    },
    // ``version`` bumps when the cache fills.
    [version],
  );
  return { gradeFor, pending };
}

function editableTarget(el: EventTarget | null): boolean {
  const t = el as HTMLElement | null;
  if (!t) return false;
  const tag = (t.tagName || "").toLowerCase();
  return tag === "input" || tag === "textarea" || tag === "select" || t.isContentEditable;
}

/**
 * Map editing with history, one-source-per-target, drag and drop.
 *
 * Every edit goes through ``change`` so it is one undo step. A mapping list the
 * page replaced from outside (re-map, stream switch) starts a fresh history:
 * undoing across a regeneration would resurrect rows of another schema.
 */
export function useMappingStudio(opts: {
  mappings: EditableMapping[];
  onChange: (next: EditableMapping[]) => void;
  catalog: DestColumnCatalog | null;
}) {
  const { mappings, onChange, catalog } = opts;
  const [history, setHistory] = useState<History<EditableMapping[]>>(emptyHistory);
  const [announcement, setAnnouncement] = useState("");
  const emitted = useRef(mappings);

  useEffect(() => {
    if (mappings !== emitted.current) {
      emitted.current = mappings;
      setHistory(emptyHistory());
    }
  }, [mappings]);

  const announce = useCallback((text: string) => {
    // Re-announce identical text: clear first so the live region fires again.
    setAnnouncement("");
    window.setTimeout(() => setAnnouncement(text), 30);
  }, []);

  const change = useCallback(
    (next: EditableMapping[]) => {
      if (next === mappings) return;
      setHistory((h) => recordEdit(h, mappings));
      emitted.current = next;
      onChange(next);
    },
    [mappings, onChange],
  );

  const undo = useCallback(() => {
    const step = undoStep(history, mappings);
    if (!step) return;
    setHistory(step.history);
    emitted.current = step.value;
    onChange(step.value);
    announce("Undone.");
  }, [history, mappings, onChange, announce]);

  const redo = useCallback(() => {
    const step = redoStep(history, mappings);
    if (!step) return;
    setHistory(step.history);
    emitted.current = step.value;
    onChange(step.value);
    announce("Redone.");
  }, [history, mappings, onChange, announce]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const which = historyKey(e);
      if (!which || editableTarget(e.target)) return;
      e.preventDefault();
      if (which === "undo") undo();
      else redo();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [undo, redo]);

  /**
   * Point ``source`` at ``target``. One source per target: a column another
   * row already writes is moved here and that row is left unmapped (one undo
   * restores both).
   */
  const remapRows = useCallback(
    (pairs: Array<{ source: string; target: string }>): { moved: string[] } => {
      const want = new Map(pairs.map((p) => [p.source, p.target]));
      const claimed = new Map<string, string>();
      for (const [source, target] of want) if (target) claimed.set(target.toLowerCase(), source);
      const moved: string[] = [];
      const next = mappings.map((m) => {
        if (want.has(m.source)) return remapToDestination(m, want.get(m.source) || "", catalog);
        if (isIntentionalOmit(m) || !m.target) return m;
        const holder = claimed.get(m.target.toLowerCase());
        if (holder && holder !== m.source) {
          moved.push(m.source);
          return remapToDestination(m, "", catalog);
        }
        return m;
      });
      change(next);
      return { moved };
    },
    [mappings, catalog, change],
  );

  const remap = useCallback(
    (source: string, target: string) => {
      const col = catalog?.columns.find((c) => c.name.toLowerCase() === target.toLowerCase());
      if (col && !col.writable) {
        announce(`${col.name} is generated by the destination and cannot be written. ${source} is unchanged.`);
        return;
      }
      const { moved } = remapRows([{ source, target }]);
      announce(
        target
          ? `${source} now writes ${target}.${moved.length ? ` ${moved.join(", ")} unmapped from it.` : ""}`
          : `${source} is unmapped.`,
      );
    },
    [remapRows, announce, catalog],
  );

  const unmapDestination = useCallback(
    (target: string) => {
      const key = target.toLowerCase();
      const hit = mappings.filter((m) => !isIntentionalOmit(m) && m.target.toLowerCase() === key);
      if (hit.length === 0) return;
      remapRows(hit.map((m) => ({ source: m.source, target: "" })));
      announce(`${target} is unmapped.`);
    },
    [mappings, remapRows, announce],
  );

  const clearAll = useCallback(() => {
    const live = mappings.filter((m) => !isIntentionalOmit(m) && m.target);
    if (live.length === 0) return;
    remapRows(live.map((m) => ({ source: m.source, target: "" })));
    announce(`Cleared ${live.length} destination column${live.length === 1 ? "" : "s"}. Undo restores them.`);
  }, [mappings, remapRows, announce]);

  const applySuggestions = useCallback(
    (picked: AutoMapSuggestion[]) => {
      if (picked.length === 0) return;
      const { moved } = remapRows(picked.map((s) => ({ source: s.source, target: s.target })));
      announce(
        `Mapped ${picked.length} column${picked.length === 1 ? "" : "s"}.${moved.length ? ` ${moved.length} other row(s) were unmapped to keep one source per column.` : ""}`,
      );
    },
    [remapRows, announce],
  );

  // ----------------------------------------------------------------- drag and drop
  const [dnd, setDnd] = useState<DndState>(IDLE);
  const targets: DndTarget[] = useMemo(() => {
    const states = destinationColumnStates(catalog, mappings);
    return states.map((s) => ({
      name: s.column.name,
      describe: [
        destColumnFacts(s.column).join(", "),
        s.sources.length ? `mapped from ${s.sources.join(" and ")}` : s.column.required ? "required, unmapped" : "unmapped",
      ].filter(Boolean).join(", "),
    }));
  }, [catalog, mappings]);

  const dispatchDnd = useCallback(
    (action: DndAction) => {
      const res = dndReduce(dnd, action, targets);
      setDnd(res.state);
      if (res.drop) remap(res.drop.source, res.drop.target);
      else if (res.announce) announce(res.announce);
    },
    [dnd, targets, remap, announce],
  );

  /** Keydown on a source grip: Space/Enter picks up, then the drag keys apply. */
  const onGripKeyDown = useCallback(
    (source: string, e: { key: string; preventDefault: () => void; stopPropagation: () => void }) => {
      if (dnd.mode === "dragging" && dnd.source === source) {
        const action = dndKeyAction(e.key);
        if (!action) return;
        e.preventDefault();
        e.stopPropagation();
        dispatchDnd(action);
        return;
      }
      if (e.key === " " || e.key === "Enter") {
        e.preventDefault();
        const current = mappings.find((m) => m.source === source)?.target.toLowerCase();
        const at = current ? targets.findIndex((t) => t.name.toLowerCase() === current) : 0;
        dispatchDnd({ type: "pickUp", source, via: "keyboard", startAt: Math.max(0, at) });
      }
    },
    [dnd, dispatchDnd, mappings, targets],
  );

  /** Pointer drags highlight silently: the cursor already shows where it is. */
  const pointerOver = useCallback(
    (name: string | null) => {
      setDnd((s) => {
        if (s.mode !== "dragging" || name == null) return s;
        const over = targets.findIndex((t) => t.name === name);
        return over < 0 || over === s.over ? s : { ...s, over };
      });
    },
    [targets],
  );

  const pointerDrop = useCallback(
    (source: string, target: string) => {
      setDnd(IDLE);
      if (!mappings.some((m) => m.source === source)) return;
      remap(source, target);
    },
    [mappings, remap],
  );

  const conflicts = useMemo(() => targetConflicts(mappings), [mappings]);
  const claimedBy = useMemo(() => {
    const by = new Map<string, string[]>();
    for (const m of mappings) {
      if (isIntentionalOmit(m) || !m.target) continue;
      const k = m.target.toLowerCase();
      by.set(k, [...(by.get(k) ?? []), m.source]);
    }
    return by;
  }, [mappings]);
  const states = useMemo(() => destinationColumnStates(catalog, mappings), [catalog, mappings]);
  const summary = useMemo(() => summarizeMapTargets(catalog, mappings), [catalog, mappings]);

  return {
    change,
    undo,
    redo,
    canUndo: history.past.length > 0,
    canRedo: history.future.length > 0,
    remap,
    unmapDestination,
    clearAll,
    applySuggestions,
    announcement,
    announce,
    dnd,
    dispatchDnd,
    onGripKeyDown,
    pointerOver,
    pointerDrop,
    dragOver: dnd.mode === "dragging" ? targets[dnd.over]?.name ?? null : null,
    conflicts,
    claimedBy,
    states,
    summary,
  };
}
