import { useCallback, useEffect, useRef, useState } from "react";
import { fetchWorkspaceSettings } from "./api";
import {
  STUDIO_CANVAS_KEY,
  STUDIO_SCHEMA_BROWSER_KEY,
  describeStudioFlags,
  readLocalStudioFlag,
  writeLocalStudioFlag,
  type StudioFlagView,
} from "./studioFlags";

export interface StudioFlags extends StudioFlagView {
  /** True only after settings have resolved. Until then both surfaces stay off. */
  ready: boolean;
  optInCanvas: () => void;
  optInSchema: () => void;
}

const OFF: StudioFlags = {
  ready: false,
  schemaBrowser: false,
  canvas: false,
  error: "",
  canvasOptIn: false,
  schemaOptIn: false,
  optInCanvas: () => {},
  optInSchema: () => {},
};

function browserStorage(): Storage | null {
  try {
    return globalThis.localStorage ?? null;
  } catch {
    return null;
  }
}

/**
 * Workspace setting AND this browser. A failed settings read stays off and
 * keeps the error. Storage and focus re-read both sides.
 */
export function useStudioFlags(): StudioFlags {
  const [view, setView] = useState<StudioFlagView & { ready: boolean }>({
    ready: false,
    schemaBrowser: false,
    canvas: false,
    error: "",
    canvasOptIn: false,
    schemaOptIn: false,
  });
  const generation = useRef(0);

  const load = useCallback(() => {
    const request = ++generation.current;
    const storage = browserStorage();
    const localSchema = readLocalStudioFlag(STUDIO_SCHEMA_BROWSER_KEY, storage);
    const localCanvas = readLocalStudioFlag(STUDIO_CANVAS_KEY, storage);
    fetchWorkspaceSettings()
      .then((settings) => {
        if (request !== generation.current) return;
        setView({
          ready: true,
          ...describeStudioFlags({
            serverSchema: settings.studio_schema_browser,
            serverCanvas: settings.studio_canvas,
            localSchema,
            localCanvas,
          }),
        });
      })
      .catch((err) => {
        if (request !== generation.current) return;
        const message = err instanceof Error ? err.message : "Workspace settings could not be loaded.";
        setView({
          ready: true,
          ...describeStudioFlags({
            serverSchema: undefined,
            serverCanvas: undefined,
            localSchema,
            localCanvas,
            error: message,
          }),
        });
      });
  }, []);

  useEffect(() => {
    load();
    const onStorage = (event: StorageEvent) => {
      if (
        event.key
        && event.key !== STUDIO_CANVAS_KEY
        && event.key !== STUDIO_SCHEMA_BROWSER_KEY
      ) {
        return;
      }
      load();
    };
    const onFocus = () => load();
    window.addEventListener("storage", onStorage);
    window.addEventListener("focus", onFocus);
    return () => {
      generation.current += 1;
      window.removeEventListener("storage", onStorage);
      window.removeEventListener("focus", onFocus);
    };
  }, [load]);

  const optInCanvas = useCallback(() => {
    writeLocalStudioFlag(STUDIO_CANVAS_KEY, true, browserStorage());
    load();
  }, [load]);

  const optInSchema = useCallback(() => {
    writeLocalStudioFlag(STUDIO_SCHEMA_BROWSER_KEY, true, browserStorage());
    load();
  }, [load]);

  return { ...view, optInCanvas, optInSchema };
}

export { OFF as STUDIO_FLAGS_OFF };
