/**
 * The Transfer Studio step footer controls: one Back, one primary, one place.
 *
 * Every step (Source, Destination, Transform, Map, Validate) uses these so the
 * primary is always the rightmost control of the footer, at the same height,
 * radius and type size, with a trailing arrow icon (never a "→" character).
 * When the primary is disabled, the footer says why in visible text next to
 * it (`StepFooterReason`), not only in a tooltip.
 */
import { useEffect, type ReactNode } from "react";
import { DtIcon } from "../DtIcon";
import { Button, type ButtonProps } from "../ui/Button";

export function StepBackButton({
  label = "Back",
  ...rest
}: Omit<ButtonProps, "variant" | "children" | "leadingIcon"> & { label?: string }) {
  return (
    <Button
      {...rest}
      className={`df2-step-back${rest.className ? ` ${rest.className}` : ""}`}
      leadingIcon={<DtIcon name="arrow-left" size={16} />}
    >
      {label}
    </Button>
  );
}

export function StepPrimaryButton({
  children,
  busy = false,
  loadingLabel,
  arrow = true,
  leadingIcon,
  ...rest
}: Omit<ButtonProps, "variant" | "loading" | "trailingIcon"> & {
  children: ReactNode;
  /** Shows the inline spinner and `loadingLabel`, keeping the button's width and place. */
  busy?: boolean;
  /** Continue buttons carry a trailing arrow; actions such as Run transfer do not. */
  arrow?: boolean;
}) {
  return (
    <Button
      {...rest}
      variant="primary"
      loading={busy}
      loadingLabel={loadingLabel}
      leadingIcon={leadingIcon}
      className={`df2-step-primary${rest.className ? ` ${rest.className}` : ""}`}
      trailingIcon={arrow && !busy ? <DtIcon name="arrow-right" size={16} /> : undefined}
    >
      {children}
    </Button>
  );
}

/** Why the primary is disabled, in one short visible line beside it. */
export function StepFooterReason({ id, children }: { id?: string; children: ReactNode }) {
  if (!children) return null;
  return (
    <span className="df2-step-footer-reason" id={id} role="status">
      {children}
    </span>
  );
}

/** The toast region's distance from the viewport bottom (read by Toast.tsx). */
const TOAST_OFFSET_VARS = ["--df-toast-offset-bottom", "--df-toast-offset-bottom-mobile"] as const;
/** Space kept between the footer's top edge and the lowest toast. */
const TOAST_FOOTER_GAP = 12;
/**
 * The step footer and the strips docked on top of it (Validate's data contract
 * and CDC retention rows): everything a notification must leave readable.
 */
const STEP_FOOTER_SELECTOR =
  ".df2-wizard-footer, .df2-validate-footer-contract, .df2-validate-footer-cdc";

/**
 * Keeps the toast region above the step footer, so a notification never covers
 * Back, the primary, the reason beside it or a strip docked on the footer. The
 * footer's height changes with the step and the width (it wraps to two or three
 * rows on phones), so it is measured, not guessed: whenever a footer mounts,
 * resizes or moves, the distance from its top edge to the viewport bottom
 * becomes the region's bottom offset (desktop and phone alike).
 * With no footer on screen the region returns to its usual place.
 */
export function useStepFooterToastClearance(selector = STEP_FOOTER_SELECTOR) {
  useEffect(() => {
    if (typeof ResizeObserver === "undefined" || typeof MutationObserver === "undefined") return;
    const host = document.body;
    let frame = 0;
    let observed: HTMLElement[] = [];
    let applied = "";

    const apply = (value: string) => {
      if (value === applied) return;
      applied = value;
      for (const name of TOAST_OFFSET_VARS) {
        if (value) host.style.setProperty(name, value);
        else host.style.removeProperty(name);
      }
    };

    const measure = () => {
      frame = 0;
      const footers = Array.from(document.querySelectorAll<HTMLElement>(selector));
      if (footers.length !== observed.length || footers.some((footer, index) => footer !== observed[index])) {
        resize.disconnect();
        for (const footer of footers) resize.observe(footer);
        observed = footers;
      }
      const viewportHeight = window.innerHeight;
      let clearance = 0;
      for (const footer of footers) {
        const box = footer.getBoundingClientRect();
        // Hidden (the studio stays mounted behind other screens) or scrolled away.
        if (box.width === 0 || box.height === 0 || box.bottom <= 0 || box.top >= viewportHeight) continue;
        clearance = Math.max(clearance, viewportHeight - box.top);
      }
      apply(clearance > 0 ? `${Math.ceil(clearance + TOAST_FOOTER_GAP)}px` : "");
    };

    const schedule = () => {
      if (!frame) frame = window.requestAnimationFrame(measure);
    };

    const resize = new ResizeObserver(schedule);
    // Steps swap their footer element; the observer re-binds on the next frame.
    const mutations = new MutationObserver(schedule);
    mutations.observe(host, { childList: true, subtree: true });
    window.addEventListener("resize", schedule);
    window.addEventListener("scroll", schedule, { capture: true, passive: true });
    measure();

    return () => {
      if (frame) window.cancelAnimationFrame(frame);
      resize.disconnect();
      mutations.disconnect();
      window.removeEventListener("resize", schedule);
      window.removeEventListener("scroll", schedule, { capture: true });
      apply("");
    };
  }, [selector]);
}
