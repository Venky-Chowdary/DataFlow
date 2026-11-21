/**
 * "Explain" on a finding (UI pass 2, item 6): a small button that opens a
 * popover with three short answers from the check or verdict catalog (what it
 * checks, what this result means, what to do). Deterministic text, marked as
 * such. Radix Popover: Escape closes and focus returns to the button.
 */
import * as Popover from "@radix-ui/react-popover";
import { useId } from "react";
import type { Explanation } from "../../lib/explainCatalog";
import { DtIcon } from "../DtIcon";

export function ExplainPopover({
  explanation,
  label,
  className,
}: {
  explanation: Explanation;
  /** Accessible name of the button, e.g. "Explain Schema contract". */
  label?: string;
  className?: string;
}) {
  const titleId = useId();
  return (
    <Popover.Root>
      <Popover.Trigger asChild>
        <button
          type="button"
          className={["df2-explain-trigger", className].filter(Boolean).join(" ")}
          aria-label={label ?? `Explain ${explanation.title}`}
        >
          <DtIcon name="help" size={14} chrome="product" />
          <span aria-hidden="true">Explain</span>
        </button>
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Content
          className="df2-explain"
          side="bottom"
          align="start"
          sideOffset={6}
          collisionPadding={16}
          aria-labelledby={titleId}
        >
          <p id={titleId} className="df2-explain-title">{explanation.title}</p>
          <dl className="df2-explain-list">
            <div>
              <dt>What it means</dt>
              <dd>{explanation.what}</dd>
            </div>
            <div>
              <dt>This result</dt>
              <dd>{explanation.result}</dd>
            </div>
            <div>
              <dt>What to do</dt>
              <dd>{explanation.next}</dd>
            </div>
          </dl>
          <p className="df2-explain-source">{explanation.source}</p>
          <Popover.Close className="df2-explain-close" aria-label="Close explanation">
            <DtIcon name="x" size={14} chrome="product" />
          </Popover.Close>
        </Popover.Content>
      </Popover.Portal>
    </Popover.Root>
  );
}
