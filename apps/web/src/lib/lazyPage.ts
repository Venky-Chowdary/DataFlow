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

export function lazyNamed<T extends ComponentType<any>, K extends string>(
  importer: () => Promise<Record<K, T>>,
  exportName: K,
): LazyExoticComponent<T> {
  return lazyPage(async () => {
    const mod = await importer();
    return { default: mod[exportName] };
  });
}
