import { resolveCatalogIdToType } from "./connectorTypes.js";

/** Catalog tile that may be a hosted/edition alias of one driver. */
export interface AliasAwareTile {
  id: string;
  is_hosted_alias?: boolean;
  alias_of?: string | null;
  driver_type?: string;
}

/**
 * One tile per driver. Snowflake on AWS / Azure / GCP / Standard / Enterprise
 * all use the same login — showing them as separate products loses trust.
 */
export function collapseHostedAliasTiles<T extends AliasAwareTile>(items: T[]): T[] {
  const groups = new Map<string, T[]>();
  for (const item of items) {
    const driver = String(item.alias_of || item.driver_type || resolveCatalogIdToType(item.id) || item.id)
      .toLowerCase()
      .trim();
    const list = groups.get(driver) || [];
    list.push(item);
    groups.set(driver, list);
  }
  const keep = new Set<string>();
  for (const [driver, group] of groups) {
    if (group.length === 1) {
      keep.add(group[0].id);
      continue;
    }
    const canonical =
      group.find((g) => g.id.toLowerCase() === driver) ||
      group.find((g) => !g.is_hosted_alias) ||
      group[0];
    keep.add(canonical.id);
  }
  return items.filter((item) => keep.has(item.id));
}
