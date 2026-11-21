import { lazy, type ComponentType, type LazyExoticComponent } from "react";

/** Session flag so a broken network cannot reload-loop after a deploy. */
export const STALE_CHUNK_RELOAD_KEY = "df2.chunk-reload";

let reloadInFlight = false;

export function isStaleChunkError(error: unknown): boolean {
  const msg = error instanceof Error ? error.message : String(error ?? "");
  const name = error instanceof Error ? error.name : "";
  return (
    /Failed to fetch dynamically imported module/i.test(msg)
    || /error loading dynamically imported module/i.test(msg)
    || /Importing a module script failed/i.test(msg)
    || /Loading chunk \d+ failed/i.test(msg)
    || name === "ChunkLoadError"
  );
}

function sessionStore(): Storage | null {
  try {
    if (typeof sessionStorage === "undefined") return null;
    return sessionStorage;
  } catch {
    return null;
  }
}

export function alreadyReloadedForStaleChunk(): boolean {
  return sessionStore()?.getItem(STALE_CHUNK_RELOAD_KEY) === "1";
}

export function isStaleChunkReloadInFlight(): boolean {
  return reloadInFlight;
}

export function shouldAutoReloadStaleChunk(error: unknown): boolean {
  return isStaleChunkError(error) && !alreadyReloadedForStaleChunk() && sessionStore() != null;
}

export function clearStaleChunkReloadGuard(): void {
  reloadInFlight = false;
  try {
    sessionStore()?.removeItem(STALE_CHUNK_RELOAD_KEY);
  } catch {
    /* private mode / blocked storage */
  }
}

/** Reload once after a hashed Vite chunk 404s. Returns true when a reload was started. */
export function reloadOnceForStaleChunk(error: unknown): boolean {
  if (!shouldAutoReloadStaleChunk(error) || reloadInFlight) {
    return reloadInFlight && isStaleChunkError(error);
  }
  try {
    sessionStore()?.setItem(STALE_CHUNK_RELOAD_KEY, "1");
  } catch {
    return false;
  }
  reloadInFlight = true;
  if (typeof window !== "undefined") {
    window.location.reload();
  }
  return true;
}

export function pageErrorCopy(
  label: string,
  error: unknown,
): { title: string; description: string; reload: boolean } {
  const screen = (label || "This page").trim() || "This page";
  if (isStaleChunkError(error)) {
    return {
      title: `${screen} needs a refresh`,
      description:
        "A newer version of Datawrap is available. Reload to continue — this is not a data-loss event.",
      reload: true,
    };
  }
  return {
    title: `${screen} hit an unexpected error`,
    description:
      "The rest of the workspace is still available. Try again, or open another page from the sidebar.",
    reload: false,
  };
}

export function lazyPage<T extends ComponentType<any>>(
  importer: () => Promise<{ default: T }>,
): LazyExoticComponent<T> {
  return lazy(async () => {
    try {
      const mod = await importer();
      clearStaleChunkReloadGuard();
      return mod;
    } catch (error) {
      if (reloadOnceForStaleChunk(error)) {
        return new Promise(() => {
          /* reload in flight — do not reject into the error boundary */
        });
      }
      throw error;
    }
  });
}

/** A lazy screen whose code can be fetched before it first renders (UX-43). */
export type PreloadableLazy<T extends ComponentType<any>> = LazyExoticComponent<T> & {
  preload: () => void;
  /** Settles once the screen's code has loaded, or failed to (never rejects). */
  whenLoaded: () => Promise<void>;
};

/**
 * Run `onExpire` `afterMs` after `loaded` settles; returns a cancel function.
 *
 * A deep link (open job X on Jobs) is handed to a lazy screen as a prop and
 * dropped a moment later so a later visit starts clean. Timing that from the
 * click lost the link whenever the screen's code took longer than the delay to
 * load (a cold CI browser): the page mounted after the prop was gone and
 * opened no job (studio-next "run now" flake). The delay now starts when the
 * code has loaded, so the page has mounted with the link before it expires.
 */
export function expireAfterLoad(loaded: Promise<unknown>, afterMs: number, onExpire: () => void): () => void {
  let cancelled = false;
  let timer: ReturnType<typeof setTimeout> | null = null;
  const start = () => {
    if (!cancelled) timer = setTimeout(onExpire, afterMs);
  };
  loaded.then(start, start);
  return () => {
    cancelled = true;
    if (timer != null) clearTimeout(timer);
  };
}

export function lazyNamed<T extends ComponentType<any>, K extends string>(
  importer: () => Promise<Record<K, T>>,
  exportName: K,
): PreloadableLazy<T> {
  // One import shared by preload() and the first render; a failure is forgotten
  // so the render (or a later preload) tries again.
  let pending: Promise<Record<K, T>> | null = null;
  const load = () => {
    pending ??= importer().catch((error) => {
      pending = null;
      throw error;
    });
    return pending;
  };
  const Screen = lazyPage(async () => {
    const mod = await load();
    return { default: mod[exportName] };
  }) as PreloadableLazy<T>;
  Screen.preload = () => {
    load().catch(() => {
      /* the render reports it */
    });
  };
  Screen.whenLoaded = () => load().then(() => undefined, () => undefined);
  return Screen;
}
