/**
 * Catalog tile clickability — one owner.
 *
 * Marketplace tiles must not open a save/test form for Planned brands just
 * because the static JSON still says `status: "live"` or `beta`. Transfer
 * pickers additionally require a real side (transfer_ready / source_only /
 * connect_only). A Certified driver whose package is missing on this host is
 * an environment gap: operators may still save credentials; Execute fails closed.
 */
export type CatalogTileEligibility = {
  transfer_ready?: boolean;
  connect_only?: boolean;
  source_ready?: boolean;
  dest_ready?: boolean;
  certification_tier?: string;
  effective_status?: string;
  status?: string;
  environment_gap?: boolean;
};

export function catalogTileSelectable(
  item: CatalogTileEligibility,
  requireAvailable = false,
): boolean {
  const tier = item.certification_tier || "";
  if (requireAvailable) {
    return Boolean(item.transfer_ready || item.connect_only || tier === "source_only");
  }
  if (item.environment_gap) return true;
  if (tier === "planned" || item.effective_status === "planned") return false;
  return Boolean(
    item.transfer_ready ||
      item.connect_only ||
      tier === "source_only" ||
      tier === "certified" ||
      item.source_ready ||
      item.dest_ready,
  );
}

export function catalogTileBlocked(
  item: CatalogTileEligibility,
  requireAvailable = false,
): boolean {
  return !catalogTileSelectable(item, requireAvailable);
}
