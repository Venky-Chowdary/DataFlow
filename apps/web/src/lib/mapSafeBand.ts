/**
 * Map safe-band grouping — collapse proven preserve / safe-normalize rows so
 * operators focus on Issues (risk, cast, specialty, pending schema).
 *
 * Classification is fail-closed: any ambiguity → attention (never invent "safe").
 */
import {
  assumeTimezoneAwaitingZone,
  hasCreateNewTypeRisk,
  isIntentionalOmit,
  isSafeNormalizeMapping,
  isSpecialtyLogicalType,
  mappingAckTier,
  mappingRequiresRiskAck,
  type EditableMapping,
} from "./mapping";
import type { IndexedMapping } from "./columnWorkbench";

export type MapBandId = "attention" | "safe" | "ready" | "omitted";

export interface MapBandPartition {
  attention: IndexedMapping[];
  safe: IndexedMapping[];
  ready: IndexedMapping[];
  omitted: IndexedMapping[];
}

/** True when the row is lossless Approve-tier and not yet Ready. */
export function isSafeBandMapping(m: EditableMapping): boolean {
  if (isIntentionalOmit(m)) return false;
  if (m.approved) return false;
  if (m.assignmentStrategy === "pending_dest_schema") return false;
  if (assumeTimezoneAwaitingZone(m)) return false;
  if (mappingRequiresRiskAck(m)) return false;
  if (hasCreateNewTypeRisk(m)) return false;
  if (m.typeNarrowing) return false;
  if (m.isPii) return false;
  if (isSpecialtyLogicalType(m.inferredType) || isSpecialtyLogicalType(m.destType)) {
    return false;
  }
  if (
    m.structDerived
    || m.structPolicy === "flatten_top_level_keys"
    || m.structPolicy === "flatten_deep"
    || m.structPolicy === "explode_rows"
    || m.structPolicy === "normalize_child_table"
    || m.structPolicy === "hybrid_json_and_child"
  ) {
    return false;
  }
  const fid = (m.fidelity || "").toLowerCase();
  if (fid === "lossy_cast" || fid === "mutate" || fid === "cast") return false;
  if (mappingAckTier(m) !== "approve") return false;
  // Preserve / lossless / safe normalize / empty fidelity with Approve tier.
  if (fid === "preserve" || fid === "lossless" || !fid || isSafeNormalizeMapping(m)) {
    return true;
  }
  return false;
}

export function mappingMapBand(m: EditableMapping): MapBandId {
  if (isIntentionalOmit(m)) return "omitted";
  if (m.approved) return "ready";
  if (isSafeBandMapping(m)) return "safe";
  return "attention";
}

export function partitionMapBands(items: IndexedMapping[]): MapBandPartition {
  const out: MapBandPartition = {
    attention: [],
    safe: [],
    ready: [],
    omitted: [],
  };
  for (const item of items) {
    out[mappingMapBand(item.mapping)].push(item);
  }
  return out;
}

/** Collapse safe band when it would drown Issues (enterprise Map density). */
export function shouldCollapseSafeBand(
  partition: MapBandPartition,
  minSafe = 2,
): boolean {
  return partition.safe.length >= minSafe;
}

export function mapBandLabel(id: MapBandId, count: number): string {
  switch (id) {
    case "attention":
      return count === 1 ? "1 column needs attention" : `${count} columns need attention`;
    case "safe":
      return count === 1
        ? "1 safe mapping (preserve / normalize)"
        : `${count} safe mappings (preserve / normalize)`;
    case "ready":
      return count === 1 ? "1 ready (approved)" : `${count} ready (approved)`;
    case "omitted":
      return count === 1 ? "1 omitted" : `${count} omitted`;
    default:
      return `${count}`;
  }
}
