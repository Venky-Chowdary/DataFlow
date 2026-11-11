import { type ReactNode, useLayoutEffect, useRef, useState } from "react";
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
 * drawers). It mounts inside the app shell rather than on <body>, so today's
 * job detail content keeps the `.df2-app` rules it was designed under.
 */
export function JobDetailPanel({ open, onClose, title, subtitle, icon, headerExtra, children }: JobDetailPanelProps) {
  const anchorRef = useRef<HTMLSpanElement>(null);
  const [container, setContainer] = useState<Element | null>(null);

  useLayoutEffect(() => {
    setContainer(anchorRef.current?.closest(".df2-app") ?? null);
  }, []);

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
