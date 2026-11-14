/**
 * Schema browsing helpers (UX-9): search the whole catalog, not the first
 * page of it, and say how much of it is on screen.
 */
import { fetchQuerySchema, fetchQuerySchemas, type QueryCatalogNodeInfo } from "./api";

/** "sales.orders" → { schema: "sales", name: "orders" }; a bare name has no schema. */
export function splitQualifiedName(raw: string): { schema: string; name: string } {
  const text = (raw || "").trim();
  const dot = text.lastIndexOf(".");
  if (dot <= 0 || dot === text.length - 1) return { schema: "", name: text };
  return { schema: text.slice(0, dot), name: text.slice(dot + 1) };
}

/** Schema names under the catalog tree's database nodes. */
export function schemaNamesFromNodes(nodes: readonly QueryCatalogNodeInfo[]): string[] {
  const out: string[] = [];
  const walk = (list: readonly QueryCatalogNodeInfo[]) => {
    for (const node of list) {
      if (node.kind === "schema" && node.name && !out.includes(node.name)) out.push(node.name);
      if (node.children?.length) walk(node.children);
    }
  };
  walk(nodes);
  return out;
}

/** "Showing 40 of 9,497 tables" — or every table, when they all fit. */
export function listingCountLabel(shown: number, total: number | null | undefined, noun = "table"): string {
  const plural = (n: number) => `${n.toLocaleString()} ${noun}${n === 1 ? "" : "s"}`;
  if (typeof total !== "number" || total <= shown) return plural(shown);
  return `Showing ${shown.toLocaleString()} of ${plural(total)}`;
}

/**
 * Names in the connector's catalog that match ``query`` — searched on the
 * server, so a table past the first page (or in another schema, written
 * "schema.table") is found. Names from another schema come back qualified.
 */
export async function searchCatalogNames(
  connectorId: string,
  query: string,
  defaultSchema = "",
): Promise<{ names: string[]; total: number | null }> {
  const { schema, name } = splitQualifiedName(query);
  if (!connectorId || !name) return { names: [], total: null };
  const res = await fetchQuerySchema({
    connector_id: connectorId,
    schema_name: schema || undefined,
    search: name,
  });
  const qualify = schema && schema !== defaultSchema;
  return {
    names: res.objects.map((o) => (qualify ? `${schema}.${o.name}` : o.name)),
    total: typeof res.objects_total === "number" ? res.objects_total : null,
  };
}

export async function fetchSchemaNames(connectorId: string): Promise<{ schemas: string[]; current: string }> {
  const res = await fetchQuerySchemas({ connector_id: connectorId });
  return { schemas: schemaNamesFromNodes(res.nodes), current: res.schema_name || "" };
}
