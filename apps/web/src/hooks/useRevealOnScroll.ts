import { useEffect, useRef, useState } from "react";

function scrollRootFor(el: HTMLElement): Element | null {
  // Marketing pages scroll on window — not the app shell
  if (el.closest(".lp")) return null;

  let node: HTMLElement | null = el.parentElement;
  while (node) {
    if (node.classList.contains("df2-content")) return node;
    const { overflowY } = getComputedStyle(node);
    if (/(auto|scroll|overlay)/.test(overflowY) && node.scrollHeight > node.clientHeight + 1) {
      return node;
    }
    node = node.parentElement;
  }
  return null;
}

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

    // Marketing pages: reveal on mount — window scroll, no app shell
    if (el.closest(".lp")) {
      const rect = el.getBoundingClientRect();
      if (rect.top < window.innerHeight * 0.98) {
        reveal();
        return;
      }
    }

    const root = scrollRootFor(el);
    const rootHeight = root instanceof Element ? root.clientHeight : window.innerHeight;

    // Already on-screen (hash nav / short pages / in-app scroll host) — reveal immediately
    const rect = el.getBoundingClientRect();
    const rootRect = root instanceof Element ? root.getBoundingClientRect() : { top: 0, bottom: window.innerHeight };
    if (rect.top < rootRect.bottom - rootHeight * 0.08 && rect.bottom > rootRect.top) {
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
      { threshold, root: root ?? undefined, rootMargin: "0px 0px -6% 0px" },
    );
    observer.observe(el);

    // Safety: never leave content invisible when scroll host or route changes
    const failsafe = window.setTimeout(reveal, 1200);
    const scrollHost = root ?? window;
    const onScroll = () => {
      const r = el.getBoundingClientRect();
      const hostRect = root instanceof Element ? root.getBoundingClientRect() : { top: 0, bottom: window.innerHeight };
      if (r.top < hostRect.bottom - 24 && r.bottom > hostRect.top + 24) reveal();
    };
    scrollHost.addEventListener("scroll", onScroll, { passive: true });
    return () => {
      observer.disconnect();
      scrollHost.removeEventListener("scroll", onScroll);
      window.clearTimeout(failsafe);
    };
  }, [threshold]);

  return { ref, visible, className: visible ? "lp-reveal lp-reveal--in" : "lp-reveal" };
}
