import { DtIcon } from "../DtIcon";

export type SourceKind = "file" | "database" | "cloud";

interface SourceKindTilesProps {
  value: SourceKind;
  onChange: (kind: SourceKind) => void;
  /** Hide the mindset strip once a source is already loaded — saves vertical chrome. */
  hideHint?: boolean;
}

const OPTIONS: {
  id: SourceKind;
  label: string;
  desc: string;
  icon: string;
  mindset: string;
}[] = [
  {
    id: "file",
    label: "File",
    desc: "CSV, JSON, JSONL, TSV, Parquet",
    icon: "upload",
    mindset: "Drop a file and preview its structure immediately.",
  },
  {
    id: "database",
    label: "Database",
    desc: "PostgreSQL, MySQL, MongoDB, SQLite, DuckDB, Snowflake…",
    icon: "database",
    mindset: "Pick a saved connector, then choose a table or collection.",
  },
  {
    id: "cloud",
    label: "Cloud storage",
    desc: "S3, GCS, MinIO, R2, Wasabi, Backblaze",
    icon: "connectors",
    mindset: "Connect object storage and select a path or prefix.",
  },
];

export function SourceKindTiles({ value, onChange, hideHint = false }: SourceKindTilesProps) {
  const active = OPTIONS.find((o) => o.id === value) ?? OPTIONS[0];

  return (
    <div className="df2-source-kind-wrap">
      <div className="df2-source-kind-grid" role="radiogroup" aria-label="Source type">
        {OPTIONS.map((opt) => (
          <button
            key={opt.id}
            type="button"
            role="radio"
            aria-checked={value === opt.id}
            className={`df2-source-kind-tile ${value === opt.id ? "active" : ""}`}
            title={`${opt.label} — ${opt.desc}`}
            onClick={() => onChange(opt.id)}
          >
            <span className="df2-source-kind-icon" aria-hidden>
              <DtIcon name={opt.icon} size={18} />
            </span>
            <span className="df2-source-kind-copy">
              <strong>{opt.label}</strong>
            </span>
          </button>
        ))}
      </div>
      {!hideHint && (
        <p className="df2-source-kind-hint" title={active.mindset}>
          <DtIcon name="sparkle" size={13} />
          <span>{active.mindset}</span>
        </p>
      )}
    </div>
  );
}
