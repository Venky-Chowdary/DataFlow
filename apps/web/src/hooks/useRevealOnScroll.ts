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

/**
 * Fade/slide sections in when they enter the viewport.
 * Marketing (`.lp`) never auto-reveals via a short timeout — that made the
 * landing feel static. Below-fold blocks wait for IntersectionObserver.
 */
export function useRevealOnScroll<T extends HTMLElement = HTMLDivElement>(threshold = 0.12) {
  const ref = useRef<T>(null);
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;

    let cancelled = false;
    const reveal = () => {
      if (!cancelled) setVisible(true);
    };

    const prefersReduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (prefersReduced) {
      reveal();
      return;
    }

    const bind = () => {
      if (!el.isConnected) return;

      const isMarketing = Boolean(el.closest(".lp"));
      const root = scrollRootFor(el);
      const rootHeight = root instanceof Element ? root.clientHeight : window.innerHeight;
      const rootRect = root instanceof Element ? root.getBoundingClientRect() : { top: 0, bottom: window.innerHeight };
      const rect = el.getBoundingClientRect();

      // Only auto-reveal if already meaningfully in view on first paint
      // (hero / first band). Below-fold content waits for scroll.
      const alreadyInView = isMarketing
        ? rect.top < window.innerHeight * 0.82 && rect.bottom > 48
        : rect.top < rootRect.bottom - rootHeight * 0.08 && rect.bottom > rootRect.top;

      if (alreadyInView) {
        reveal();
        return undefined;
      }

      const observer = new IntersectionObserver(
        ([entry]) => {
          if (entry.isIntersecting) {
            reveal();
            observer.disconnect();
          }
        },
        {
          threshold,
          root: root ?? undefined,
          // Reveal a bit before fully centered — modern product-site feel
          rootMargin: isMarketing ? "0px 0px -12% 0px" : "0px 0px -4% 0px",
        },
      );
      observer.observe(el);

      // Long failsafe only for non-marketing app shell (lazy panes / overflow bugs).
      // Marketing must stay scroll-driven.
      const failsafe = isMarketing
        ? undefined
        : window.setTimeout(reveal, 12_000);

      const scrollHost = root ?? window;
      const onScroll = () => {
        const r = el.getBoundingClientRect();
        const hostRect = root instanceof Element ? root.getBoundingClientRect() : { top: 0, bottom: window.innerHeight };
        const trigger = isMarketing ? hostRect.bottom - hostRect.top * 0.18 : hostRect.bottom - 24;
        if (r.top < trigger && r.bottom > hostRect.top + 24) reveal();
      };
      scrollHost.addEventListener("scroll", onScroll, { passive: true });

      return () => {
        observer.disconnect();
        scrollHost.removeEventListener("scroll", onScroll);
        if (failsafe != null) window.clearTimeout(failsafe);
      };
    };

    let cleanup: (() => void) | undefined;
    const raf = window.requestAnimationFrame(() => {
      cleanup = bind();
    });

    return () => {
      cancelled = true;
      window.cancelAnimationFrame(raf);
      cleanup?.();
    };
  }, [threshold]);

  return { ref, visible, className: visible ? "lp-reveal lp-reveal--in" : "lp-reveal" };
}
