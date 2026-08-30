import { Button } from "../ui/Button";
import { DtIcon } from "../DtIcon";
import type { NumberLocaleValidateAction } from "../../lib/validateHonestyControls";

interface Props {
  action: NumberLocaleValidateAction | null | undefined;
  onOpenAdvanced?: () => void;
  kind?: "number" | "date";
}

const COPY = {
  number: {
    label: "Number locale",
    badge: "set US or EU",
    rowHint: "Auto refuses a lone 1,234 / 1.234",
    cta: "Set number locale",
    title: "Open Destination → Advanced and set Number locale to US or EU",
    className: "df2-vd-number-locale",
  },
  assumed_us: {
    label: "Number locale",
    badge: "assumed US",
    rowHint: "read as US (1,234.56) — switch to EU if this export is European",
    cta: "Switch to EU",
    title: "Open Destination → Advanced and set Number locale to EU",
    className: "df2-vd-number-locale",
  },
  date: {
    label: "Date locale",
    badge: "set DMY or MDY",
    rowHint: "Auto refuses 01/02/2024 (Jan 2 vs Feb 1)",
    cta: "Set date locale",
    title: "Open Destination → Advanced and set Date locale to DMY or MDY",
    className: "df2-vd-date-locale",
  },
} as const;

/** One root cause → Destination → Advanced. Auto will not guess locale. */
export function NumberLocalePanel({ action, onOpenAdvanced, kind = "number" }: Props) {
  if (!action) return null;
  const copy =
    kind === "number" && action.decision === "assumed_us"
      ? COPY.assumed_us
      : COPY[kind];
  const cols = action.columns.slice(0, 6);

  return (
    <section
      className={`df2-load-history has-findings ${copy.className}`}
      aria-label={copy.label}
    >
      <header className="df2-load-history-head">
        <DtIcon name="alert" size={15} />
        <strong>{copy.label}</strong>
        <span className="df2-load-history-badge">{copy.badge}</span>
      </header>
      <p className="df2-load-history-warn">{action.message}</p>
      {cols.length > 0 ? (
        <ul className="df2-load-history-list">
          {cols.map((col) => (
            <li key={col}>
              <code>{col}</code>
              {` — ${copy.rowHint}`}
            </li>
          ))}
        </ul>
      ) : null}
      {onOpenAdvanced ? (
        <div className={`${copy.className}-cta`}>
          <Button
            size="sm"
            variant="secondary"
            leadingIcon={<DtIcon name="settings" size={14} />}
            onClick={onOpenAdvanced}
            title={copy.title}
          >
            {copy.cta}
          </Button>
        </div>
      ) : null}
    </section>
  );
}

export function DateLocalePanel(props: Omit<Props, "kind">) {
  return <NumberLocalePanel {...props} kind="date" />;
}
