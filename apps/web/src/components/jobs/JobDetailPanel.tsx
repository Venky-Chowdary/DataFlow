import { type ReactNode, useEffect, useLayoutEffect, useRef, useState } from "react";
import { Drawer } from "../ui/Drawer";

interface JobDetailPanelProps {
  open: boolean;
  onClose: () => void;
  title: ReactNode;
  subtitle?: ReactNode;
  icon?: ReactNode;
  /** Status badge on the title row. */
  headerExtra?: ReactNode;
  children: ReactNode;
}

/**
 * The job detail as the app's standard right-side Drawer (same header, close
 * button, width and Escape as the Connectors / Schedules / Contracts detail
 * drawers). It mounts inside the Jobs screen's own wrapper (the app keeps
 * screens mounted and hides the inactive ones), so the job detail content
 * keeps the `.df2-app` rules it was designed under and a hidden screen hides
 * its drawer. Hiding the screen also closes the drawer, which releases its
 * overlay and scroll lock whatever navigation switched screens.
 */
export function JobDetailPanel({ open, onClose, title, subtitle, icon, headerExtra, children }: JobDetailPanelProps) {
  const anchorRef = useRef<HTMLSpanElement>(null);
  const [container, setContainer] = useState<Element | null>(null);

  const closeRef = useRef(onClose);
  useEffect(() => { closeRef.current = onClose; }, [onClose]);

  useLayoutEffect(() => {
    const anchor = anchorRef.current;
    setContainer(anchor?.closest(".df2-screen-keep") ?? anchor?.closest(".df2-app") ?? null);
  }, []);

  useEffect(() => {
    const screen = anchorRef.current?.closest<HTMLElement>(".df2-screen-keep");
    if (!open || !screen || typeof MutationObserver === "undefined") return;
    if (screen.hidden) {
      closeRef.current();
      return;
    }
    const observer = new MutationObserver(() => {
      if (screen.hidden) closeRef.current();
    });
    observer.observe(screen, { attributes: true, attributeFilter: ["hidden"] });
    return () => observer.disconnect();
  }, [open]);

  return (
    <>
      <span ref={anchorRef} hidden />
      <Drawer
        open={open}
        onClose={onClose}
        title={title}
        subtitle={subtitle}
        icon={icon}
        headerExtra={headerExtra}
        ariaLabel="Job detail"
        className="df2-jobs-detail-drawer"
        container={container}
      >
        {children}
      </Drawer>
    </>
  );
}
