import { Button } from "../ui/Button";
import { DtIcon } from "../DtIcon";
import type { NumberLocaleValidateAction } from "../../lib/validateHonestyControls";

interface Props {
  action: NumberLocaleValidateAction | null | undefined;
  onOpenAdvanced?: () => void;
}

/** One root cause → Destination → Advanced. Auto will not guess 1,234. */
export function NumberLocalePanel({ action, onOpenAdvanced }: Props) {
  if (!action) return null;
  const cols = action.columns.slice(0, 6);

  return (
    <section
      className="df2-load-history has-findings df2-vd-number-locale"
      aria-label="Number locale"
    >
      <header className="df2-load-history-head">
        <DtIcon name="alert" size={15} />
        <strong>Number locale</strong>
        <span className="df2-load-history-badge">set US or EU</span>
      </header>
      <p className="df2-load-history-warn">{action.message}</p>
      {cols.length > 0 ? (
        <ul className="df2-load-history-list">
          {cols.map((col) => (
            <li key={col}>
              <code>{col}</code>
              {" — Auto refuses a lone 1,234 / 1.234"}
            </li>
          ))}
        </ul>
      ) : null}
      {onOpenAdvanced ? (
        <div className="df2-vd-number-locale-cta">
          <Button
            size="sm"
            variant="secondary"
            leadingIcon={<DtIcon name="settings" size={14} />}
            onClick={onOpenAdvanced}
            title="Open Destination → Advanced and set Number locale to US or EU"
          >
            Set number locale
          </Button>
        </div>
      ) : null}
    </section>
  );
}
