import { useEffect, useRef, useState } from "react";

/**
 * A header that stays pinned to the top of the page scroll container.
 *
 * `stuck` turns true once the content has scrolled under it (a zero-height
 * sentinel above the header has left the scroll container), so the header can
 * show its divider only when it is actually floating. The header's height is
 * published as `--df2-qw-sticky-h` on the header's parent, so panels below can
 * pin themselves just under it instead of under a guessed offset.
 */
export function useStickyHeader<THeader extends HTMLElement>() {
  const headerRef = useRef<THeader | null>(null);
  const sentinelRef = useRef<HTMLDivElement | null>(null);
  const [stuck, setStuck] = useState(false);

  useEffect(() => {
    const sentinel = sentinelRef.current;
    if (!sentinel || typeof IntersectionObserver === "undefined") return;
    const observer = new IntersectionObserver(
      ([entry]) => setStuck(!entry.isIntersecting),
      { threshold: 0 },
    );
    observer.observe(sentinel);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    const header = headerRef.current;
    const host = header?.parentElement;
    if (!header || !host) return;
    // Sticky offsets are measured inside the scroll container's padding; pin
    // flush with its top edge so no scrolled content shows above the header.
    let scroller: HTMLElement | null = host;
    while (scroller && !/(auto|scroll)/.test(getComputedStyle(scroller).overflowY)) scroller = scroller.parentElement;
    const inset = scroller ? Number.parseFloat(getComputedStyle(scroller).paddingTop) || 0 : 0;
    host.style.setProperty("--df2-qw-sticky-top", `${-inset}px`);
    const publish = () => host.style.setProperty(
      "--df2-qw-sticky-h",
      `${Math.max(0, Math.ceil(header.getBoundingClientRect().height) - inset)}px`,
    );
    publish();
    if (typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver(publish);
    observer.observe(header);
    return () => observer.disconnect();
  }, []);

  return { headerRef, sentinelRef, stuck };
}
