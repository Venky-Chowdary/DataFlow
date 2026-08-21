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
 * Names a recorded route the way the rest of the wizard names it — schema
 * qualified for a table, format and path for an export — so one run is not
 * described two different ways on the same page.
 */
export function describeDestRoute(routeKey: string | null): string {
  if (!routeKey) return "";
  let route: Partial<DestRoute>;
  try {
    route = JSON.parse(routeKey) as Partial<DestRoute>;
  } catch {
    return "";
  }
  if (route.destKindMode === "file_export") {
    const format = (route.exportFormat || "").toUpperCase();
    const path = route.destOutputPath || "";
    return [format && `${format} export`, path].filter(Boolean).join(" · ");
  }
  const table = route.targetCollection || "";
  const qualifier = route.destSchema || route.targetDb || "";
  if (!table) return qualifier;
  return qualifier ? `${qualifier}.${table}` : table;
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
