import { useEffect, useRef, useState } from "react";

/**
 * Observe whether an element is near the viewport. Used to pause marketing
 * canvas / cinema timers when off-screen — the main cause of landing scroll lag.
 */
export function useInView<T extends HTMLElement = HTMLDivElement>(
  rootMargin = "120px 0px",
  threshold = 0.05,
) {
  const ref = useRef<T>(null);
  const [inView, setInView] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el || typeof IntersectionObserver === "undefined") {
      setInView(true);
      return;
    }
    const io = new IntersectionObserver(
      ([entry]) => setInView(Boolean(entry?.isIntersecting)),
      { root: null, rootMargin, threshold },
    );
    io.observe(el);
    return () => io.disconnect();
  }, [rootMargin, threshold]);

  return { ref, inView };
}
