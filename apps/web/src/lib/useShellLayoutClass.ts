import { useEffect } from "react";

/**
 * Put `className` on the app shell's scroll boxes (.df2-content and
 * .df2-content-inner) while the screen that asks for it is the visible one.
 *
 * Screens stay mounted behind `hidden` when the operator moves on, so a class
 * added on mount and removed on unmount stayed on the shell for every other
 * screen (Transfer Studio's overflow-hidden layout broke the Query page's
 * pinned header). The class follows the screen's `hidden` attribute instead.
 */
export function useShellLayoutClass(className: string, anchorSelector: string): void {
  useEffect(() => {
    const content = document.querySelector(".df2-content");
    const inner = document.querySelector(".df2-content-inner");
    const anchor = document.querySelector(anchorSelector);
    const screen = anchor?.closest<HTMLElement>(".df2-screen-keep") ?? null;
    const apply = () => {
      const on = !screen || !screen.hidden;
      content?.classList.toggle(className, on);
      inner?.classList.toggle(className, on);
    };
    apply();
    const observer = screen && typeof MutationObserver !== "undefined"
      ? new MutationObserver(apply)
      : null;
    if (screen) observer?.observe(screen, { attributes: true, attributeFilter: ["hidden"] });
    return () => {
      observer?.disconnect();
      content?.classList.remove(className);
      inner?.classList.remove(className);
    };
  }, [className, anchorSelector]);
}
