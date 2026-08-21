/**
 * A finished run reports the route it wrote. Retarget the destination and that
 * report stops being an answer about the route on screen, so Run must offer
 * Execute again instead of claiming a landing in a table it never wrote.
 */

export interface DestRoute {
  destKindMode: "database" | "file_export";
  destType: string;
  targetDb: string;
  destSchema: string;
  targetCollection: string;
  exportFormat: string;
  destOutputPath: string;
}

/** Identity of the destination a run would write to. */
export function destRouteKey(route: DestRoute): string {
  const fileExport = route.destKindMode === "file_export";
  return JSON.stringify({
    destKindMode: route.destKindMode,
    destType: fileExport ? "" : route.destType,
    targetDb: fileExport ? "" : route.targetDb.trim(),
    destSchema: fileExport ? "" : route.destSchema.trim(),
    targetCollection: fileExport ? "" : route.targetCollection.trim(),
    exportFormat: fileExport ? route.exportFormat : "",
    destOutputPath: fileExport ? route.destOutputPath.trim() : "",
  });
}

/**
 * True when the run on screen wrote the route the wizard now describes. A run
 * seeded from elsewhere (no executed route) is not claimed to be stale.
 */
export function runResultDescribesRoute(
  executedRouteKey: string | null,
  currentRouteKey: string,
): boolean {
  return executedRouteKey == null || executedRouteKey === currentRouteKey;
}
