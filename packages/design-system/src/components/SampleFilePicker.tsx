export interface SampleFileItem {
  id: string;
  label: string;
  description: string;
  filename: string;
  format: string;
}

interface SampleFilePickerProps {
  samples: SampleFileItem[];
  onSelect: (filename: string) => void;
  disabled?: boolean;
}

export function SampleFilePicker({ samples, onSelect, disabled }: SampleFilePickerProps) {
  return (
    <div className="df-sample-picker">
      <div className="df-sample-picker-head">
        <span className="df-sample-picker-title">Sample test files</span>
        <span className="df-sample-picker-meta">Click to load for testing</span>
      </div>
      <div className="df-sample-picker-grid">
        {samples.map((s) => (
          <button
            key={s.id}
            type="button"
            className="df-sample-card"
            disabled={disabled}
            onClick={() => onSelect(s.filename)}
          >
            <span className="df-sample-card-format">{s.format.toUpperCase()}</span>
            <span className="df-sample-card-label">{s.label}</span>
            <span className="df-sample-card-desc">{s.description}</span>
          </button>
        ))}
      </div>
    </div>
  );
}
