/** CSS class for logical / SQL data types — drives animated badges in the UI. */

export function typeBadgeClass(rawType: string | undefined): string {
  const t = (rawType || "string").toLowerCase();
  if (/int|bigint|smallint|number\(/.test(t)) return "df2-type-int";
  if (/decimal|numeric|float|double|real|bignumeric/.test(t)) return "df2-type-decimal";
  if (/bool/.test(t)) return "df2-type-bool";
  if (/timestamp|datetime|date|time/.test(t)) return "df2-type-temporal";
  if (/json|variant|object|array|super/.test(t)) return "df2-type-json";
  if (/uuid|guid/.test(t)) return "df2-type-uuid";
  if (/binary|blob|bytea|bytes/.test(t)) return "df2-type-binary";
  return "df2-type-string";
}
