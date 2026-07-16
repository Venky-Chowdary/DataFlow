import { useEffect, useRef, useState } from "react";

/** Fade/slide sections in when they enter the viewport (Devin-style scroll reveals). */
export function useRevealOnScroll<T extends HTMLElement = HTMLDivElement>(threshold = 0.08) {
  const ref = useRef<T>(null);
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;

    const prefersReduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (prefersReduced) {
      setVisible(true);
      return;
    }

    const reveal = () => setVisible(true);

    // Already on-screen (hash nav / short pages) — reveal immediately
    const rect = el.getBoundingClientRect();
    if (rect.top < window.innerHeight * 0.92 && rect.bottom > 0) {
      reveal();
      return;
    }

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          reveal();
          observer.disconnect();
        }
      },
      { threshold, rootMargin: "0px 0px -8% 0px" },
    );
    observer.observe(el);

    // Safety: never leave content invisible
    const failsafe = window.setTimeout(reveal, 1800);
    return () => {
      observer.disconnect();
      window.clearTimeout(failsafe);
    };
  }, [threshold]);

  return { ref, visible, className: visible ? "lp-reveal lp-reveal--in" : "lp-reveal" };
}
