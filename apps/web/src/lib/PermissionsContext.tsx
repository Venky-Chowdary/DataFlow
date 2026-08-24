import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";

import { ApiError, fetchEffectiveIdentity, fetchWorkspaces, type EffectiveIdentity } from "./api";
import { refusalSentence } from "./permissionCopy";
import { WORKSPACE_CHANGED_EVENT, getActiveWorkspaceId, setActiveWorkspaceId } from "./workspace";

/**
 * The authority the API stated, so a control can refuse before it is pressed.
 *
 * The client used to render every write control for every signed-in account and
 * let the button discover the refusal — a viewer saw an enabled "New connection"
 * and got a raw 403 (or worse, silence). Authority is not something the browser
 * may decide, so this is read from ``GET /auth/me``: the same resolution the
 * request gate applies, for the workspace the browser currently names.
 *
 * Gating here is *courtesy*, not enforcement. The API refuses regardless; this
 * only makes the refusal legible in advance.
 */

export const PERMISSIONS = {
  connectorWrite: "connector.write",
  jobRun: "job.run",
  jobManage: "job.manage",
  jobPlan: "job.plan",
  scheduleManage: "schedule.manage",
  scheduleAuthorize: "schedule.authorize",
  workspaceManage: "workspace.manage",
  memberInvite: "member.invite",
} as const;

export type PermissionName = (typeof PERMISSIONS)[keyof typeof PERMISSIONS];

export interface PermissionsValue {
  identity: EffectiveIdentity | null;
  loading: boolean;
  /** Why identity could not be read, when it could not — never a silent default. */
  error: string;
  /** True until the API has answered; controls stay enabled so nothing flickers shut. */
  unknown: boolean;
  can: (permission: PermissionName) => boolean;
  role: string;
  refresh: () => Promise<void>;
  /** One sentence explaining a refusal, for a tooltip or a disabled control. */
  denialReason: (permission: PermissionName) => string;
}

const PermissionsContext = createContext<PermissionsValue | null>(null);

export function PermissionsProvider({
  children,
  signedIn,
}: {
  children: ReactNode;
  /** Identity is only read for a signed-in session; the public site has none. */
  signedIn: boolean;
}) {
  const [identity, setIdentity] = useState<EffectiveIdentity | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const refresh = useCallback(async () => {
    if (!signedIn) {
      setIdentity(null);
      setError("");
      return;
    }
    setLoading(true);
    try {
      const next = await fetchEffectiveIdentity();
      setIdentity(next);
      setError("");
      // The workspace the API resolved becomes the one the browser names, so
      // every later request is decided in the same workspace this answer
      // described instead of being re-resolved per request.
      if (next.workspace_id && !getActiveWorkspaceId()) setActiveWorkspaceId(next.workspace_id);
      else if (!next.workspace_id && !getActiveWorkspaceId()) {
        // The API only resolves an unnamed workspace when the account has
        // exactly one membership. With two, every workspace-scoped read ran
        // against no workspace at all — the Enterprise tab reported a saved
        // tenant as missing — so the client names one and the switcher moves it.
        const { workspaces } = await fetchWorkspaces().catch(() => ({ workspaces: [] }));
        if (workspaces.length) setActiveWorkspaceId(workspaces[0].id);
      }
    } catch (err) {
      setIdentity(null);
      // A 401 is handled by the shell (back to sign-in). Anything else is
      // reported as itself: an unknown authority is not "no authority".
      setError(err instanceof ApiError && err.status === 401 ? "" : err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [signedIn]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  useEffect(() => {
    const onWorkspaceChanged = () => void refresh();
    window.addEventListener(WORKSPACE_CHANGED_EVENT, onWorkspaceChanged);
    return () => window.removeEventListener(WORKSPACE_CHANGED_EVENT, onWorkspaceChanged);
  }, [refresh]);

  const value = useMemo<PermissionsValue>(() => {
    const unknown = !identity;
    const can = (permission: PermissionName) => {
      // Unknown authority does not disable the UI: the API is still the gate,
      // and disabling on a failed read would look like a permission problem.
      if (unknown) return true;
      return identity.permissions.includes(permission);
    };
    const role = identity?.effective_role ?? "";
    // One sentence for both halves of a refusal: the one a control shows before
    // it is pressed, and the one the API's 403 renders as.
    const denialReason = (permission: PermissionName) => refusalSentence(permission, role);
    return { identity, loading, error, unknown, can, role, refresh, denialReason };
  }, [error, identity, loading, refresh]);

  return <PermissionsContext.Provider value={value}>{children}</PermissionsContext.Provider>;
}

/**
 * Whether a write control may act, and the sentence to show when it may not.
 *
 * ``reason`` is empty when allowed, so it can be passed straight to ``title``.
 */
export function useWriteGate(permission: PermissionName): { allowed: boolean; reason: string } {
  const { can, denialReason } = usePermissions();
  const allowed = can(permission);
  return { allowed, reason: allowed ? "" : denialReason(permission) };
}

export function usePermissions(): PermissionsValue {
  const ctx = useContext(PermissionsContext);
  if (ctx) return ctx;
  // Outside the provider (isolated component tests, storybook) nothing is known,
  // so nothing is pre-refused — the API stays the gate.
  return {
    identity: null,
    loading: false,
    error: "",
    unknown: true,
    can: () => true,
    role: "",
    refresh: async () => {},
    denialReason: () => "You don't have permission to do this.",
  };
}
