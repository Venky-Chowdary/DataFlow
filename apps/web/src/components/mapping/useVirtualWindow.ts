import { useCallback, useEffect, useRef, useState } from "react";

/**
 * Fixed-row-height windowing: render only the rows in view (plus overscan).
 *
 * A 1,000+ column destination would otherwise put thousands of option rows in
 * the DOM for every open picker. Rows have one height by design (the CSS sets
 * it), so the window is pure arithmetic — no measurement, no dependency.
 */
export function virtualRange(
  count: number,
  rowHeight: number,
  scrollTop: number,
  viewport: number,
  overscan = 6,
): { start: number; end: number } {
  if (count <= 0 || rowHeight <= 0) return { start: 0, end: 0 };
  const first = Math.floor(Math.max(0, scrollTop) / rowHeight);
  const visible = Math.ceil(Math.max(viewport, rowHeight) / rowHeight);
  const start = Math.max(0, Math.min(first, count - 1) - overscan);
  const end = Math.min(count, first + visible + overscan);
  return { start, end };
}

/**
 * ``ref`` is a callback ref for the scrolling element: the element may mount
 * after the component (a portaled listbox), so the listener attaches when it
 * appears, not when the hook first runs.
 */
export function useVirtualWindow(count: number, rowHeight: number, fallbackViewport = 320) {
  const [el, setEl] = useState<HTMLElement | null>(null);
  const elRef = useRef<HTMLElement | null>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewport, setViewport] = useState(fallbackViewport);

  const ref = useCallback((node: HTMLElement | null) => {
    elRef.current = node;
    setEl(node);
  }, []);

  useEffect(() => {
    if (!el) return;
    const measure = () => setViewport(el.clientHeight || fallbackViewport);
    measure();
    setScrollTop(el.scrollTop);
    const onScroll = () => setScrollTop(el.scrollTop);
    el.addEventListener("scroll", onScroll, { passive: true });
    const RO = (globalThis as { ResizeObserver?: typeof ResizeObserver }).ResizeObserver;
    const ro = RO ? new RO(measure) : null;
    ro?.observe(el);
    return () => {
      el.removeEventListener("scroll", onScroll);
      ro?.disconnect();
    };
  }, [el, fallbackViewport]);

  /** Bring row ``index`` into view (keyboard navigation over a long list). */
  const scrollToIndex = useCallback(
    (index: number) => {
      const node = elRef.current;
      if (!node || index < 0) return;
      const top = index * rowHeight;
      const bottom = top + rowHeight;
      const view = node.clientHeight || fallbackViewport;
      if (top < node.scrollTop) node.scrollTop = top;
      else if (bottom > node.scrollTop + view) node.scrollTop = bottom - view;
      setScrollTop(node.scrollTop);
    },
    [rowHeight, fallbackViewport],
  );

  const { start, end } = virtualRange(count, rowHeight, scrollTop, viewport);
  return {
    ref,
    start,
    end,
    totalHeight: count * rowHeight,
    offsetTop: start * rowHeight,
    /** Spacer below the rendered rows so the scrollbar spans the whole list. */
    tailHeight: Math.max(0, (count - end) * rowHeight),
    scrollToIndex,
  };
}
