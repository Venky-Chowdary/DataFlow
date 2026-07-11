import { DtIcon } from "../DtIcon";

interface CadenceTilesProps {
  value: "hourly" | "daily" | "weekly";
  onChange: (v: "hourly" | "daily" | "weekly") => void;
}

const OPTIONS = [
  { id: "hourly" as const, label: "Hourly", desc: "Every 60 minutes", icon: "activity" },
  { id: "daily" as const, label: "Daily", desc: "Once per day", icon: "clock" },
  { id: "weekly" as const, label: "Weekly", desc: "Once per week", icon: "calendar" },
];

export function CadenceTiles({ value, onChange }: CadenceTilesProps) {
  return (
    <div className="df2-cadence-grid" role="radiogroup" aria-label="Sync cadence">
      {OPTIONS.map((opt) => (
        <button
          key={opt.id}
          type="button"
          role="radio"
          aria-checked={value === opt.id}
          className={`df2-cadence-tile ${value === opt.id ? "active" : ""}`}
          onClick={() => onChange(opt.id)}
        >
          <DtIcon name={opt.icon} size={18} />
          <strong>{opt.label}</strong>
          <span>{opt.desc}</span>
        </button>
      ))}
    </div>
  );
}
