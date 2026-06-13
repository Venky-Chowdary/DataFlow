/** Domain model for a single column mapping with review state. */

export interface ColumnMappingEntry {
  source: string;
  target: string;
  confidence: number;
  reasoning: string;
  userOverride: boolean;
  semanticRole?: string;
}

export type MappingFilter = "all" | "review" | "auto";

export interface MappingReviewStats {
  total: number;
  autoMapped: number;
  needsReview: number;
  overridden: number;
}

const DEFAULT_THRESHOLD = 0.85;

/** Categorizes mappings — at scale only low-confidence columns need human review. */
export class ColumnMappingReview {
  readonly threshold: number;

  constructor(
    private readonly entries: ColumnMappingEntry[],
    threshold = DEFAULT_THRESHOLD
  ) {
    this.threshold = threshold;
  }

  needsReview(entry: ColumnMappingEntry): boolean {
    return !entry.userOverride && entry.confidence < this.threshold;
  }

  get stats(): MappingReviewStats {
    const needsReview = this.entries.filter((e) => this.needsReview(e)).length;
    const overridden = this.entries.filter((e) => e.userOverride).length;
    return {
      total: this.entries.length,
      autoMapped: this.entries.length - needsReview - overridden,
      needsReview,
      overridden,
    };
  }

  filter(kind: MappingFilter): ColumnMappingEntry[] {
    if (kind === "review") return this.entries.filter((e) => this.needsReview(e));
    if (kind === "auto") return this.entries.filter((e) => !this.needsReview(e));
    return this.entries;
  }

  /** Review-first ordering — doubt columns surface immediately even with 1M columns. */
  sorted(): ColumnMappingEntry[] {
    return [...this.entries].sort((a, b) => {
      const ar = this.needsReview(a) ? 0 : 1;
      const br = this.needsReview(b) ? 0 : 1;
      if (ar !== br) return ar - br;
      return b.confidence - a.confidence;
    });
  }

  displayRows(filter: MappingFilter = "all") {
    const rows = filter === "all" ? this.sorted() : this.filter(filter);
    return rows.map((e) => ({
      source: e.source,
      target: e.target,
      confidence: e.userOverride ? 1 : e.confidence,
      reasoning: e.userOverride ? "Confirmed by you" : e.reasoning,
      needsReview: this.needsReview(e),
      userOverride: e.userOverride,
    }));
  }
}

export function fromApiMappings(
  mappings: { source: string; target: string; confidence: number; reasoning: string; user_override?: boolean }[]
): ColumnMappingReview {
  return new ColumnMappingReview(
    mappings.map((m) => ({
      source: m.source,
      target: m.target,
      confidence: m.confidence,
      reasoning: m.reasoning,
      userOverride: !!m.user_override,
    }))
  );
}
