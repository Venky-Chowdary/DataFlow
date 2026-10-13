/**
 * Studio surface flags.
 *
 * The server flag is the workspace decision. The browser flag is one
 * operator opting in. Either side off keeps the wizard, including when the
 * settings request failed — a missing answer is not permission.
 */

export const STUDIO_SCHEMA_BROWSER_KEY = "df.studioSchemaBrowser";
export const STUDIO_CANVAS_KEY = "df.studioCanvas";

export interface StudioFlagInput {
  server: boolean | undefined;
  local: boolean;
}

export function resolveStudioFlag(input: StudioFlagInput): boolean {
  return input.server === true && input.local === true;
}

export interface StudioFlagView {
  schemaBrowser: boolean;
  canvas: boolean;
  error: string;
  /** Workspace asked for the canvas and this browser has not opted in. */
  canvasOptIn: boolean;
  schemaOptIn: boolean;
}

/**
 * What the page may show. A failed settings read is not permission: both
 * surfaces stay off, and the error is the thing the operator sees.
 */
export function describeStudioFlags(input: {
  serverSchema: boolean | undefined;
  serverCanvas: boolean | undefined;
  localSchema: boolean;
  localCanvas: boolean;
  error?: string;
}): StudioFlagView {
  if (input.error) {
    return {
      schemaBrowser: false,
      canvas: false,
      error: input.error,
      canvasOptIn: false,
      schemaOptIn: false,
    };
  }
  const serverSchema = input.serverSchema === true;
  const serverCanvas = input.serverCanvas === true;
  return {
    schemaBrowser: resolveStudioFlag({ server: input.serverSchema, local: input.localSchema }),
    canvas: resolveStudioFlag({ server: input.serverCanvas, local: input.localCanvas }),
    error: "",
    canvasOptIn: serverCanvas && input.localCanvas !== true,
    schemaOptIn: serverSchema && input.localSchema !== true,
  };
}

export function readLocalStudioFlag(key: string, storage: Pick<Storage, "getItem"> | null): boolean {
  if (!storage) return false;
  try {
    return storage.getItem(key) === "1";
  } catch {
    return false;
  }
}

export function writeLocalStudioFlag(
  key: string,
  enabled: boolean,
  storage: Pick<Storage, "setItem" | "removeItem"> | null,
): void {
  if (!storage) return;
  if (enabled) storage.setItem(key, "1");
  else storage.removeItem(key);
}
