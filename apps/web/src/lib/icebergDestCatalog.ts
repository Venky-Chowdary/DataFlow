/** Iceberg dest catalog kind for Transfer Studio + leftover MERGE extra. */

export type IcebergCatalogKind = "filesystem" | "rest";

export function inferIcebergCatalogKind(
  connectionString: string,
  explicit?: string,
): IcebergCatalogKind {
  const named = String(explicit || "").toLowerCase().trim();
  if (named === "rest" || named === "nessie") return "rest";
  if (named === "filesystem") return "filesystem";
  const cs = String(connectionString || "").trim().toLowerCase();
  if (
    cs.startsWith("http://")
    || cs.startsWith("https://")
    || cs.startsWith("iceberg+rest")
    || cs.startsWith("rest://")
  ) {
    return "rest";
  }
  return "filesystem";
}

/** dest_extra for Iceberg leftover MERGE / catalog writes. Glue stays Planned. */
export function icebergDestExtra(
  kind: IcebergCatalogKind,
  warehouse?: string,
): Record<string, unknown> {
  if (kind === "rest") {
    const extra: Record<string, unknown> = { catalog_type: "rest" };
    const wh = String(warehouse || "").trim();
    if (wh) extra.warehouse = wh;
    return extra;
  }
  return { catalog_type: "filesystem" };
}
