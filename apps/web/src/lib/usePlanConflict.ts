import { useCallback, useRef, useState, type MutableRefObject } from "react";
import {
  PlanVersionConflictError,
  activeRevision,
  headVersion,
  sameFullMappings,
  type PlanMappingRow,
  type PlanView,
} from "./planConcurrency";

/**
 * Resolving a plan-version conflict (PS-1 / PS-19…): the plan was saved
 * elsewhere since this page loaded it. The page fetches the latest revision,
 * shows it next to the user's unsaved mappings, and the user chooses:
 *
 * - **Use latest** — the editor shows the latest revision; the page's head
 *   becomes the plan's head.
 * - **Keep mine** — the user's mappings are saved on top, sent with the new
 *   head as base_version: an explicit overwrite made after seeing the diff.
 * - **Cancel** — nothing changes; the head stays, so a later save still
 *   meets the conflict.
 *
 * When the latest revision already holds exactly the user's mappings (their
 * own earlier write landed), there is no conflict: the head is adopted and
 * nothing is shown.
 */

export type PlanConflictOrigin = "validate" | "map" | "execute" | "approve";

export interface PlanConflictApi {
  getPlan(planId: string): Promise<{ plan: PlanView }>;
  saveMappings(planId: string, mappings: PlanMappingRow[], baseVersion: number): Promise<{ plan: PlanView }>;
}

export interface PlanConflictState {
  planId: string;
  origin: PlanConflictOrigin;
  mine: PlanMappingRow[];
  /**
   * `version`/`mappings`: the active revision (what runs). `head`: the newest
   * revision's version; when it is not the active one (e.g. an approval
   * rolled the plan back), `headMappings` holds its mappings too.
   */
  latest: { version: number; head: number; mappings: PlanMappingRow[]; headMappings: PlanMappingRow[] | null } | null;
  status: "loading" | "ready" | "saving";
  error: string;
}

export interface PlanConflictOptions {
  api: PlanConflictApi;
  headRef: MutableRefObject<number | null>;
  /** Put the latest revision's mappings in the editor. */
  onUseLatest: (mappings: PlanMappingRow[], version: number) => void;
  notify: (title: string, message: string, tone: "success" | "info" | "warning" | "error") => void;
}

const message = (err: unknown) => (err instanceof Error ? err.message : "The request failed.");

export function usePlanConflict({ api, headRef, onUseLatest, notify }: PlanConflictOptions) {
  const [conflict, setConflict] = useState<PlanConflictState | null>(null);
  const current = useRef<PlanConflictState | null>(null);
  // Bumped by every open/close: an async step only applies its result while
  // its generation is still current (Cancel during loading stays cancelled).
  const generation = useRef(0);
  const update = useCallback((next: PlanConflictState | null) => {
    current.current = next;
    setConflict(next);
  }, []);

  /** Show the conflict for `mine`; resolves "resolved" when there was none after all. */
  const open = useCallback(
    async (planId: string, mine: PlanMappingRow[], origin: PlanConflictOrigin): Promise<"shown" | "resolved"> => {
      const pending: PlanConflictState = { planId, origin, mine, latest: null, status: "loading", error: "" };
      const gen = ++generation.current;
      update(pending);
      let plan: PlanView;
      try {
        ({ plan } = await api.getPlan(planId));
      } catch (err) {
        if (gen !== generation.current) return "shown";
        update({ ...pending, status: "ready", error: `The latest version of the plan could not be loaded: ${message(err)}` });
        return "shown";
      }
      if (gen !== generation.current) return "shown";  // cancelled or re-opened meanwhile
      const latest = activeRevision(plan);
      const head = headVersion(plan);
      const latestMappings = latest?.mappings ?? [];
      if (latest && sameFullMappings(latestMappings, mine)) {
        // The plan already holds exactly these mappings (this page's own
        // earlier write landed): nothing to resolve.
        headRef.current = head;
        update(null);
        return "resolved";
      }
      const newest = (plan.revisions ?? []).find((r) => r.version === head);
      const headMappings = latest && head !== latest.version ? newest?.mappings ?? [] : null;
      update({
        ...pending,
        status: "ready",
        latest: { version: latest?.version ?? 0, head, mappings: latestMappings, headMappings },
      });
      return "shown";
    },
    [api, headRef, update],
  );

  const useLatest = useCallback(() => {
    const state = current.current;
    if (!state?.latest) return;
    onUseLatest(state.latest.mappings, state.latest.version);
    headRef.current = state.latest.head;
    update(null);
    notify(
      `Showing revision ${state.latest.version}`,
      `Your unsaved edits were replaced by revision ${state.latest.version}, the latest saved version of this plan.`,
      "info",
    );
  }, [headRef, notify, onUseLatest, update]);

  const keepMine = useCallback(async () => {
    const state = current.current;
    if (!state?.latest || state.status === "saving") return;
    update({ ...state, status: "saving", error: "" });
    try {
      const { plan } = await api.saveMappings(state.planId, state.mine, state.latest.head);
      headRef.current = headVersion(plan);
      update(null);
      notify(
        "Your mappings were saved",
        `They are now revision ${headRef.current} and replace revision ${state.latest.version} as the active mappings. `
          + "Run Validate to check them.",
        "success",
      );
    } catch (err) {
      if (err instanceof PlanVersionConflictError) {
        // Changed again while deciding: compare against the newest one.
        await open(state.planId, state.mine, state.origin);
        const again = current.current;
        if (again) update({ ...again, error: "The plan changed again while you were deciding. Compare with the newest revision." });
        return;
      }
      update({ ...state, status: "ready", error: `Your mappings were not saved: ${message(err)}` });
    }
  }, [api, headRef, notify, open, update]);

  const cancel = useCallback(() => {
    const state = current.current;
    if (state?.status === "saving") return;
    generation.current += 1;
    update(null);
  }, [update]);

  return { conflict, open, useLatest, keepMine, cancel };
}
