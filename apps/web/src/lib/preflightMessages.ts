/** Backend route-matrix success tokens must never surface as operator errors. */
export function isRouteSuccessToken(message: string | undefined | null): boolean {
  const raw = String(message || "").trim();
  return !raw || /^supported$/i.test(raw) || /^live route:/i.test(raw);
}

export function schemaIntrospectionFailureMessage(
  message: string | undefined | null,
  streamLabel?: string,
): string {
  if (isRouteSuccessToken(message)) {
    const label = streamLabel?.trim() || "source";
    return `Could not read columns from ${label}. Re-open Source, confirm the table/collection name, then Continue again.`;
  }
  return String(message || "").trim() || "Schema introspection returned no columns.";
}
