/**
 * Mapping from the `/query/schema` API shape onto the editor's `SchemaObject`.
 *
 * Kept pure and separate from the page so the two behaviours that matter are
 * testable: unknown type/nullability stays unknown (never guessed into a
 * confident-looking badge), and expanding one object never drops the objects
 * already listed.
 */

import type { QuerySchemaObjectInfo } from "./api";
import type { SchemaObject } from "./sqlIntel";

export function toSchemaObject(o: QuerySchemaObjectInfo): SchemaObject {
  return {
    name: o.name,
    type: o.type,
    schema: o.schema_name || undefined,
    rowEstimate: o.row_estimate || undefined,
    columns: (o.columns ?? []).map((c) => ({
      name: c.name,
      // An empty type from the catalog stays empty — the badge renders
      // "unknown" rather than inventing a plausible type.
      type: c.type || "",
      // Tri-state on purpose: only an explicit `false` means NOT NULL.
      nullable: c.nullable ?? undefined,
      primaryKey: c.primary_key ?? false,
    })),
  };
}

/** An object name matches whether the catalog qualified it with a schema. */
export function matchesObjectName(candidate: string, target: string): boolean {
  if (candidate === target) return true;
  const tail = (s: string) => s.split(".").pop() ?? s;
  return tail(candidate) === tail(target) && (candidate.includes(".") || target.includes("."));
}

/**
 * Merge an expanded object's columns into the listed objects.
 *
 * Returns the previous array unchanged when the expansion carried no columns,
 * so a connector that cannot introspect columns leaves the tree honest instead
 * of collapsing it to an empty column list that reads as "no columns".
 */
export function mergeExpandedObject(
  prev: SchemaObject[],
  objectName: string,
  expanded: QuerySchemaObjectInfo | undefined,
): SchemaObject[] {
  if (!expanded || (expanded.columns ?? []).length === 0) return prev;
  const mapped = toSchemaObject(expanded);
  let hit = false;
  const next = prev.map((o) => {
    if (!matchesObjectName(o.name, objectName)) return o;
    hit = true;
    return {
      ...o,
      rowEstimate: mapped.rowEstimate ?? o.rowEstimate,
      columns: mapped.columns,
    };
  });
  return hit ? next : prev;
}

/** First object in a schema response that actually carries columns. */
export function firstExpanded(
  objects: QuerySchemaObjectInfo[],
): QuerySchemaObjectInfo | undefined {
  return objects.find((o) => (o.columns ?? []).length > 0);
}
