import { DtIcon } from "../DtIcon";

export type SourceKind = "file" | "database" | "cloud";

interface SourceKindTilesProps {
  value: SourceKind;
  onChange: (kind: SourceKind) => void;
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
    desc: "PostgreSQL, MySQL, MongoDB, Snowflake…",
    icon: "database",
    mindset: "Pick a saved connector, then choose a table or collection.",
  },
  {
    id: "cloud",
    label: "Cloud storage",
    desc: "Amazon S3, GCS, Azure Blob",
    icon: "connectors",
    mindset: "Connect object storage and select a path or prefix.",
  },
];

export function SourceKindTiles({ value, onChange }: SourceKindTilesProps) {
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
            onClick={() => onChange(opt.id)}
          >
            <span className="df2-source-kind-icon">
              <DtIcon name={opt.icon} size={22} />
            </span>
            <strong>{opt.label}</strong>
            <span>{opt.desc}</span>
          </button>
        ))}
      </div>
      <p className="df2-source-kind-hint">
        <DtIcon name="sparkle" size={14} />
        {active.mindset}
      </p>
    </div>
  );
}
