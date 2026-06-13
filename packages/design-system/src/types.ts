export interface GateItem {
  id: string;
  label: string;
  status: "pass" | "block" | "skip" | "pending";
  message: string;
  durationMs?: number;
}

export interface MappingRow {
  source: string;
  target: string;
  confidence: number;
  reasoning?: string;
  needsReview?: boolean;
  userOverride?: boolean;
}

export interface StatusTile {
  id: string;
  label: string;
  status: "active" | "warning" | "broken" | "idle";
  count?: number;
}
