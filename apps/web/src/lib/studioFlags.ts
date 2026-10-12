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
