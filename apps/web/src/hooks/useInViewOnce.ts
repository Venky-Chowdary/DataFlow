import { useEffect, useRef, useState } from "react";

/**
 * True once the element has entered the viewport, then stays true. Used by
 * marketing panels that assemble or change state the first time they are
 * seen. Without IntersectionObserver (or under reduced motion) it reports
 * true at once, so the final state is what renders.
 */
export function useInViewOnce<T extends Element = HTMLElement>(rootMargin = "0px 0px -15% 0px") {
  const ref = useRef<T | null>(null);
  const [inView, setInView] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    let reduce = false;
    try {
      reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    } catch {
      reduce = true;
    }
    if (reduce || typeof IntersectionObserver === "undefined") {
      setInView(true);
      return;
    }
    const io = new IntersectionObserver(
      (entries) => {
        if (entries.some((e) => e.isIntersecting)) {
          setInView(true);
          io.disconnect();
        }
      },
      { rootMargin },
    );
    io.observe(el);
    return () => io.disconnect();
  }, [rootMargin]);

  return { ref, inView };
}
