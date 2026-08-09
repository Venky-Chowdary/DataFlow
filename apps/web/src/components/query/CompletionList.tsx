import { useEffect, useRef } from "react";
import type { Completion } from "../../lib/sqlIntel";

/**
 * Autocomplete dropdown for the query editor.
 *
 * Presentation only — ranking lives in `sqlIntel.buildCompletions` so it stays
 * unit testable. Positioned absolutely by the caller against the caret.
 */

const KIND_GLYPH: Record<Completion["kind"], string> = {
  table: "T",
  column: "C",
  alias: "A",
  keyword: "K",
  function: "ƒ",
  schema: "S",
  snippet: "▤",
};

const KIND_LABEL: Record<Completion["kind"], string> = {
  table: "table",
  column: "column",
  alias: "alias",
  keyword: "keyword",
  function: "function",
  schema: "schema",
  snippet: "snippet",
};

export interface CompletionListProps {
  items: Completion[];
  activeIndex: number;
  left: number;
  top: number;
  onPick: (item: Completion) => void;
  onHover: (index: number) => void;
}

export function CompletionList({
  items,
  activeIndex,
  left,
  top,
  onPick,
  onHover,
}: CompletionListProps) {
  const listRef = useRef<HTMLUListElement>(null);

  // Keep the keyboard-selected row visible without stealing focus from the
  // textarea — the editor keeps focus so typing continues to filter.
  useEffect(() => {
    const el = listRef.current?.children[activeIndex] as HTMLElement | undefined;
    el?.scrollIntoView({ block: "nearest" });
  }, [activeIndex]);

  if (items.length === 0) return null;

  return (
    <div className="df2-qe-complete" style={{ left, top }} role="presentation">
      <ul
        ref={listRef}
        className="df2-qe-complete-list"
        role="listbox"
        aria-label="Query completions"
      >
        {items.map((item, i) => (
          <li
            key={`${item.kind}:${item.label}:${i}`}
            role="option"
            aria-selected={i === activeIndex}
            className="df2-qe-complete-item"
            data-active={i === activeIndex}
            data-kind={item.kind}
            onMouseEnter={() => onHover(i)}
            // mousedown, not click: click fires after the textarea blurs and
            // the popup has already been torn down.
            onMouseDown={(e) => {
              e.preventDefault();
              onPick(item);
            }}
          >
            <span className="df2-qe-complete-kind" title={KIND_LABEL[item.kind]} aria-hidden>
              {KIND_GLYPH[item.kind]}
            </span>
            <span className="df2-qe-complete-label">{item.label}</span>
            {item.detail && <span className="df2-qe-complete-detail">{item.detail}</span>}
          </li>
        ))}
      </ul>
    </div>
  );
}
