import { useEffect, useState } from "react";
import { fetchWorkspaceSettings } from "./api";
import {
  STUDIO_CANVAS_KEY,
  STUDIO_SCHEMA_BROWSER_KEY,
  readLocalStudioFlag,
  resolveStudioFlag,
} from "./studioFlags";

export interface StudioFlags {
  /** True only after settings have resolved. Until then both surfaces stay off. */
  ready: boolean;
  schemaBrowser: boolean;
  canvas: boolean;
}

const OFF: StudioFlags = { ready: false, schemaBrowser: false, canvas: false };

function browserStorage(): Storage | null {
  try {
    return globalThis.localStorage ?? null;
  } catch {
    return null;
  }
}

/**
 * Workspace setting AND this browser. A failed settings read stays off.
 */
export function useStudioFlags(): StudioFlags {
  const [flags, setFlags] = useState<StudioFlags>(OFF);

  useEffect(() => {
    let cancelled = false;
    const storage = browserStorage();
    const localSchema = readLocalStudioFlag(STUDIO_SCHEMA_BROWSER_KEY, storage);
    const localCanvas = readLocalStudioFlag(STUDIO_CANVAS_KEY, storage);
    fetchWorkspaceSettings()
      .then((settings) => {
        if (cancelled) return;
        setFlags({
          ready: true,
          schemaBrowser: resolveStudioFlag({
            server: settings.studio_schema_browser === true,
            local: localSchema,
          }),
          canvas: resolveStudioFlag({
            server: settings.studio_canvas === true,
            local: localCanvas,
          }),
        });
      })
      .catch(() => {
        if (!cancelled) setFlags({ ready: true, schemaBrowser: false, canvas: false });
      });
    return () => {
      cancelled = true;
    };
  }, []);

  return flags;
}
