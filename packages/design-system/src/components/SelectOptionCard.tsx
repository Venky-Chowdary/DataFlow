interface SelectOptionCardProps {
  selected: boolean;
  title: string;
  hint: string;
  onSelect: () => void;
}

export function SelectOptionCard({ selected, title, hint, onSelect }: SelectOptionCardProps) {
  return (
    <button
      type="button"
      className={["df-option-card", selected ? "df-option-card--selected" : ""].filter(Boolean).join(" ")}
      onClick={onSelect}
      aria-pressed={selected}
    >
      <span className="df-option-radio" aria-hidden />
      <span>
        <div className="df-option-title">{title}</div>
        <div className="df-option-hint">{hint}</div>
      </span>
    </button>
  );
}
