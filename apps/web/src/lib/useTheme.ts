import { useEffect, useState } from "react";
import {
  THEME_CHANGED_EVENT,
  applyTheme,
  bootTheme,
  readThemePreference,
  resolveTheme,
  type ResolvedTheme,
  type ThemePreference,
} from "./theme";

/**
 * Keep `<html data-theme>` in lockstep with Settings Appearance.
 * One hook, one owner — AppShell boots it; Settings writes it.
 */
export function useTheme() {
  const [preference, setPreference] = useState<ThemePreference>(readThemePreference);
  const [resolved, setResolved] = useState<ResolvedTheme>(() => resolveTheme(preference));

  useEffect(() => {
    const next = bootTheme();
    setPreference(readThemePreference());
    setResolved(next);
  }, []);

  useEffect(() => {
    const sync = () => {
      const pref = readThemePreference();
      setPreference(pref);
      setResolved(resolveTheme(pref));
    };
    window.addEventListener(THEME_CHANGED_EVENT, sync);
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    const onSystem = () => {
      if (readThemePreference() === "system") applyTheme("system");
    };
    mq.addEventListener("change", onSystem);
    return () => {
      window.removeEventListener(THEME_CHANGED_EVENT, sync);
      mq.removeEventListener("change", onSystem);
    };
  }, []);

  const setTheme = (next: ThemePreference) => {
    const resolvedNext = applyTheme(next);
    setPreference(next);
    setResolved(resolvedNext);
  };

  return { preference, resolved, setTheme };
}
