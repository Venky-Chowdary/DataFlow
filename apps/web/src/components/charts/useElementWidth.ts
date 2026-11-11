import { useEffect, useRef, useState } from "react";

/**
 * Track an element's rendered width so an SVG chart draws in real pixels.
 *
 * Scaling a fixed viewBox to the card would scale its text with it: axis labels
 * grow on a wide card and shrink on a phone. Measuring keeps type at its set
 * size. Before layout (server render, tests) the fallback width is used.
 */
export function useElementWidth<T extends Element>(fallback = 360) {
  const ref = useRef<T>(null);
  const [width, setWidth] = useState(fallback);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const measure = () => {
      const w = el.getBoundingClientRect().width;
      if (w > 0) setWidth(Math.round(w));
    };
    measure();
    if (typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  return [ref, width] as const;
}
