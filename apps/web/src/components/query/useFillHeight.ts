import { useEffect, useRef } from "react";

/**
 * Size an element to the space left under it in its scroll container, so a
 * workspace can own its scrolling (one region per pane) and the page itself
 * never scrolls.
 *
 * Publishes `--df2-qw-fill-h` on the element. The stylesheet decides where the
 * height applies (the Query workspace uses it at >= 1024px only; phones keep
 * the page scroll). Measured, not guessed: the app chrome above it (top bar,
 * banners, gutters) and the padding below it change across breakpoints.
 */
export function useFillHeight<T extends HTMLElement>() {
  const ref = useRef<T | null>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    let scroller: HTMLElement | null = el.parentElement;
    while (scroller && !/(auto|scroll)/.test(getComputedStyle(scroller).overflowY)) {
      scroller = scroller.parentElement;
    }
    let frame = 0;
    const publish = () => {
      frame = 0;
      const rect = el.getBoundingClientRect();
      if (rect.width === 0 && rect.height === 0) return; // hidden screen: keep the last value
      let visibleBottom = document.documentElement.clientHeight;
      if (scroller) {
        const box = scroller.getBoundingClientRect();
        visibleBottom = Math.min(visibleBottom, box.top + scroller.clientTop + scroller.clientHeight);
      }
      // Whatever sits under the element inside the scroll content (its margin,
      // each ancestor's padding / border / margin, later siblings) needs room too.
      const px = (v: string) => Number.parseFloat(v) || 0;
      let contentBelow = px(getComputedStyle(el).marginBottom);
      for (let node: HTMLElement | null = el; node && node !== scroller; node = node.parentElement) {
        for (let sib = node.nextElementSibling; sib; sib = sib.nextElementSibling) {
          const sibStyle = getComputedStyle(sib);
          if (sibStyle.position === "absolute" || sibStyle.position === "fixed") continue;
          contentBelow += (sib as HTMLElement).offsetHeight + px(sibStyle.marginTop) + px(sibStyle.marginBottom);
        }
        const parent: HTMLElement | null = node.parentElement;
        if (!parent) break;
        const ps = getComputedStyle(parent);
        contentBelow += px(ps.paddingBottom) + px(ps.borderBottomWidth);
        if (parent !== scroller) contentBelow += px(ps.marginBottom);
      }
      const height = Math.floor(visibleBottom - rect.top - contentBelow);
      const next = `${Math.max(320, height)}px`;
      if (el.style.getPropertyValue("--df2-qw-fill-h") !== next) el.style.setProperty("--df2-qw-fill-h", next);
    };
    const schedule = () => {
      if (!frame) frame = window.requestAnimationFrame(publish);
    };
    publish();
    window.addEventListener("resize", schedule);
    const observer = typeof ResizeObserver === "undefined" ? null : new ResizeObserver(schedule);
    observer?.observe(el);
    if (scroller) observer?.observe(scroller);
    return () => {
      window.removeEventListener("resize", schedule);
      observer?.disconnect();
      if (frame) window.cancelAnimationFrame(frame);
    };
  }, []);

  return ref;
}
