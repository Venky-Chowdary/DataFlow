import { useCallback, useEffect, useState } from "react";
import { DtIcon } from "../../components/DtIcon";
import { EmptyState } from "../../components/ui/EmptyState";
import { Button } from "../../components/ui/Button";
import { SectionLoader } from "../../components/LoadingState";
import { useToast } from "../../components/Toast";
import { useConfirm } from "../../components/ui/ConfirmDialog";
import {
  addWorkspaceMember,
  createWorkspace,
  fetchPlatformUsers,
  fetchWorkspaceInvites,
  fetchWorkspaceMembers,
  fetchWorkspaces,
  removeWorkspaceMember,
  resendWorkspaceInvite,
  resetPlatformUserPassword,
  revokeWorkspaceInvite,
  sendWorkspaceInvite,
  updatePlatformUser,
  updateWorkspaceMemberRole,
  type PlatformUser,
  type Workspace,
  type WorkspaceInvite,
  type WorkspaceMember,
  type WorkspaceRole,
} from "../../lib/api";
import {
  concreteWorkspaceId,
  getActiveWorkspaceId,
  isKnownWorkspaceChoice,
  landingWorkspaceId,
  notifyWorkspaceDirectory,
  setActiveWorkspaceId,
  useActiveWorkspaceId,
} from "../../lib/workspace";
import { useVisibleRefresh } from "../../lib/visibleRefresh";
import { PermissionNotice } from "../../components/PermissionNotice";
import { PERMISSIONS, useWriteGate } from "../../lib/PermissionsContext";
import { isLastWorkspaceAdmin, LAST_ADMIN_PROTECTED } from "../../lib/workspaceAdmin";
import { INVITE_DEFAULTS, inviteExpiryLabel, isInviteExpired, type InviteMode } from "../../lib/teamInvite";
import { IssuedPasswordNotice } from "../../components/team/IssuedPasswordNotice";

const ROLE_LABELS: Record<string, string> = {
  admin: "Admin",
  editor: "Editor",
  viewer: "Viewer",
};

const ACCOUNT_LABELS: Record<string, string> = {
  active: "Can sign in",
  disabled: "Sign-in disabled",
  provisioned: "Environment account",
  no_account: "No login yet",
};

function memberInitials(name?: string, email?: string): string {
  const src = (name || email || "?").trim();
  const parts = src.split(/[\s@.]+/).filter(Boolean);
  if (parts.length >= 2) return `${parts[0][0] ?? ""}${parts[1][0] ?? ""}`.toUpperCase();
  return src.slice(0, 2).toUpperCase();
}

function accountTone(status?: string): string {
  if (status === "active") return "df2-badge-live";
  if (status === "disabled") return "df2-badge-warn";
  return "df2-badge-muted";
}

function relativeTime(iso?: string): string {
  if (!iso) return "—";
  const then = new Date(iso).getTime();
  if (!Number.isFinite(then)) return "—";
  const delta = Date.now() - then;
  if (delta < 60_000) return "Just now";
  if (delta < 3_600_000) return `${Math.floor(delta / 60_000)}m ago`;
  if (delta < 86_400_000) return `${Math.floor(delta / 3_600_000)}h ago`;
  return new Date(iso).toLocaleDateString();
}

export function TeamSettings() {
  const { toast } = useToast();
  const { confirm } = useConfirm();
  // Two authorities, because the API has two: creating a workspace and granting
  // admin is workspace.manage, adding or removing a non-admin member is
  // member.invite. Both are the workspace admin's (C6): an editor manages no
  // members, so the member tools read as read-only for one.
  const manage = useWriteGate(PERMISSIONS.workspaceManage);
  const membership = useWriteGate(PERMISSIONS.memberInvite);
  // Members belong to a workspace; a platform admin's All / Unassigned view is
  // not one, so the member tools ask for a workspace there (D218).
  const activeWorkspace = concreteWorkspaceId(useActiveWorkspaceId());
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [platformAdmin, setPlatformAdmin] = useState(false);
  const [members, setMembers] = useState<WorkspaceMember[]>([]);
  const [invites, setInvites] = useState<WorkspaceInvite[]>([]);
  const [inviteMode, setInviteMode] = useState<InviteMode>(INVITE_DEFAULTS.mode);
  const [busyInvite, setBusyInvite] = useState<string | null>(null);
  const [users, setUsers] = useState<PlatformUser[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string>("");
  const [updatedAt, setUpdatedAt] = useState<number | null>(null);
  const [newWorkspaceName, setNewWorkspaceName] = useState("");
  const [creatingWorkspace, setCreatingWorkspace] = useState(false);
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteName, setInviteName] = useState("");
  const [inviteRole, setInviteRole] = useState<WorkspaceRole>("viewer");
  const [createLogin, setCreateLogin] = useState<boolean>(INVITE_DEFAULTS.createLogin);
  const [inviting, setInviting] = useState(false);
  const [removingEmail, setRemovingEmail] = useState<string | null>(null);
  const [roleChangeEmail, setRoleChangeEmail] = useState<string | null>(null);
  const [busyAccount, setBusyAccount] = useState<string | null>(null);
  // A one-time password is shown once, in the operator's own session, and never
  // stored client-side — an admin has to hand it over before leaving the screen.
  const [issuedPassword, setIssuedPassword] = useState<{ email: string; password: string } | null>(null);

  const loadWorkspaces = useCallback(async () => {
    const { workspaces: rows, platformAdmin: isAdmin, defaultWorkspaceId, views } = await fetchWorkspaces();
    setWorkspaces(rows);
    setPlatformAdmin(isAdmin);
    const current = getActiveWorkspaceId();
    if (rows.length && !isKnownWorkspaceChoice(current, rows, views)) {
      setActiveWorkspaceId(landingWorkspaceId(rows, defaultWorkspaceId, views));
    }
    return isAdmin;
  }, []);

  const loadUsers = useCallback(async (isAdmin: boolean) => {
    if (!isAdmin) {
      setUsers([]);
      return;
    }
    setUsers(await fetchPlatformUsers().catch(() => []));
  }, []);

  const refreshMembers = useCallback(async (workspaceId: string) => {
    if (!workspaceId) {
      setMembers([]);
      setInvites([]);
      return;
    }
    try {
      setMembers(await fetchWorkspaceMembers(workspaceId));
      // Pending invitations are secondary: an unreadable list must not hide members.
      setInvites(await fetchWorkspaceInvites(workspaceId).catch(() => []));
      setLoadError("");
      setUpdatedAt(Date.now());
    } catch (err) {
      // An unreadable member list is not an empty workspace.
      setMembers([]);
      setLoadError(err instanceof Error ? err.message : "Could not read this workspace's members.");
    }
  }, []);

  const refreshAll = useCallback(async () => {
    try {
      const isAdmin = await loadWorkspaces();
      await Promise.all([loadUsers(isAdmin), refreshMembers(concreteWorkspaceId(getActiveWorkspaceId()))]);
    } catch (err) {
      setLoadError(err instanceof Error ? err.message : "Could not load team settings.");
    }
  }, [loadWorkspaces, loadUsers, refreshMembers]);

  useEffect(() => {
    setLoading(true);
    refreshAll().finally(() => setLoading(false));
  }, [refreshAll]);

  useEffect(() => {
    void refreshMembers(activeWorkspace);
  }, [activeWorkspace, refreshMembers]);

  useVisibleRefresh(refreshAll, 8_000, !loading);

  /** Refuse in words rather than sending a request that will be refused. */
  const refuse = (reason: string) => {
    toast({ title: "No write permission", message: reason, tone: "warning" });
    return false;
  };
  const refuseLastAdmin = () => {
    toast({ title: "Last admin protected", message: LAST_ADMIN_PROTECTED, tone: "warning" });
    return false;
  };
  // Granting admin needs workspace.manage; a principal scoped to member.invite
  // alone may add non-admins only, so the option says why instead of failing.
  const canGrantAdmin = manage.allowed;

  const addWorkspace = async () => {
    if (!manage.allowed) return void refuse(manage.reason);
    const name = newWorkspaceName.trim();
    if (!name) return;
    setCreatingWorkspace(true);
    try {
      const ws = await createWorkspace(name);
      setNewWorkspaceName("");
      setActiveWorkspaceId(ws.id);
      notifyWorkspaceDirectory();
      await refreshAll();
      toast({ title: "Workspace created", message: `${ws.name} is ready — you are its admin.`, tone: "success" });
    } catch (err) {
      toast({
        title: "Could not create workspace",
        message: err instanceof Error ? err.message : "Request failed.",
        tone: "error",
      });
    } finally {
      setCreatingWorkspace(false);
    }
  };

  const invite = async () => {
    if (!membership.allowed) return void refuse(membership.reason);
    if (inviteRole === "admin" && !canGrantAdmin) return void refuse(manage.reason);
    const email = inviteEmail.trim();
    if (!activeWorkspace) {
      toast({
        title: "Choose a workspace first",
        message: "Create a workspace above — members belong to a workspace, not to the deployment.",
        tone: "error",
      });
      return;
    }
    if (!email) {
      toast({ title: "Email required", message: "Enter the member's email address.", tone: "error" });
      return;
    }
    setInviting(true);
    if (inviteMode === "email") {
      try {
        const sent = await sendWorkspaceInvite(activeWorkspace, email, inviteRole, inviteName.trim());
        setInviteEmail("");
        setInviteName("");
        toast({
          title: "Invitation sent",
          message: `${sent.email} gets a link to join as ${ROLE_LABELS[sent.role]}. ${inviteExpiryLabel(sent.expires_at)}.`,
          tone: "success",
        });
        await refreshMembers(activeWorkspace);
      } catch (err) {
        toast({
          title: "Invitation not sent",
          message: err instanceof Error ? err.message : "Could not send the invitation.",
          tone: "error",
        });
      } finally {
        setInviting(false);
      }
      return;
    }
    try {
      const result = await addWorkspaceMember(activeWorkspace, email, inviteRole, {
        // Minting a login is the workspace admin's (RB3); an editor adds members only.
        createAccount: createLogin && canGrantAdmin,
        name: inviteName.trim(),
      });
      setInviteEmail("");
      setInviteName("");
      setCreateLogin(INVITE_DEFAULTS.createLogin);
      if (result.temporaryPassword) {
        setIssuedPassword({ email: result.membership.email, password: result.temporaryPassword });
        toast({
          title: "Member added with a login",
          message: `Reveal the one-time password below and give it to ${result.membership.email}.`,
          tone: "success",
        });
      } else {
        toast({
          title: "Member added",
          message: result.hasAccount
            ? `${result.membership.email} is now ${ROLE_LABELS[result.membership.role]}.`
            : `${result.membership.email} is ${ROLE_LABELS[result.membership.role]} but has no login yet.`,
          tone: result.hasAccount ? "success" : "info",
        });
      }
      notifyWorkspaceDirectory();
      await refreshAll();
    } catch (err) {
      toast({
        title: "Add member failed",
        message: err instanceof Error ? err.message : "Could not add member.",
        tone: "error",
      });
    } finally {
      setInviting(false);
    }
  };

  const resendInvite = async (inv: WorkspaceInvite) => {
    if (!membership.allowed) return void refuse(membership.reason);
    if (!activeWorkspace) return;
    setBusyInvite(inv.id);
    try {
      const next = await resendWorkspaceInvite(activeWorkspace, inv.id);
      toast({ title: "Invitation re-sent", message: `The earlier link to ${next.email} no longer works.`, tone: "success" });
      await refreshMembers(activeWorkspace);
    } catch (err) {
      toast({ title: "Resend failed", message: err instanceof Error ? err.message : "Request failed.", tone: "error" });
    } finally {
      setBusyInvite(null);
    }
  };

  const revokeInvite = async (inv: WorkspaceInvite) => {
    if (!membership.allowed) return void refuse(membership.reason);
    if (!activeWorkspace) return;
    const ok = await confirm({
      title: `Revoke the invitation to ${inv.email}?`,
      message: "The link in their email stops working. You can invite them again later.",
      confirmLabel: "Revoke invitation",
      cancelLabel: "Keep invitation",
      tone: "danger",
    });
    if (!ok) return;
    setBusyInvite(inv.id);
    try {
      await revokeWorkspaceInvite(activeWorkspace, inv.id);
      toast({ title: "Invitation revoked", tone: "info" });
      await refreshMembers(activeWorkspace);
    } catch (err) {
      toast({ title: "Revoke failed", message: err instanceof Error ? err.message : "Request failed.", tone: "error" });
    } finally {
      setBusyInvite(null);
    }
  };

  const remove = async (email: string) => {
    if (!membership.allowed) return void refuse(membership.reason);
    if (isLastWorkspaceAdmin(members, email)) return void refuseLastAdmin();
    if (!activeWorkspace) return;
    const ok = await confirm({
      title: `Remove ${email}?`,
      message: "They will lose access to this workspace’s connectors, pipelines, and jobs. The login itself is kept.",
      confirmLabel: "Remove member",
      cancelLabel: "Keep member",
      tone: "danger",
    });
    if (!ok) return;
    setRemovingEmail(email);
    try {
      await removeWorkspaceMember(activeWorkspace, email);
      toast({ title: "Member removed", tone: "info" });
      notifyWorkspaceDirectory();
      await refreshAll();
    } catch (err) {
      toast({
        title: "Remove failed",
        message: err instanceof Error ? err.message : "Could not remove member.",
        tone: "error",
      });
    } finally {
      setRemovingEmail(null);
    }
  };

  const changeRole = async (email: string, role: WorkspaceRole) => {
    if (!membership.allowed) return void refuse(membership.reason);
    if (role === "admin" && !canGrantAdmin) return void refuse(manage.reason);
    if (role !== "admin" && isLastWorkspaceAdmin(members, email)) {
      return void refuseLastAdmin();
    }
    if (!activeWorkspace) return;
    setRoleChangeEmail(email);
    try {
      const next = await updateWorkspaceMemberRole(activeWorkspace, email, role);
      toast({ title: `${email} is now ${ROLE_LABELS[next.role] ?? next.role}`, tone: "success" });
      await refreshMembers(activeWorkspace);
    } catch (err) {
      toast({
        title: "Role change failed",
        message: err instanceof Error ? err.message : "Could not change role.",
        tone: "error",
      });
      await refreshMembers(activeWorkspace);
    } finally {
      setRoleChangeEmail(null);
    }
  };

  const setStatus = async (user: PlatformUser, status: "active" | "disabled") => {
    setBusyAccount(user.email);
    try {
      await updatePlatformUser(user.email, { status });
      toast({ title: status === "active" ? "Sign-in enabled" : "Sign-in disabled", tone: "info" });
      await loadUsers(platformAdmin);
      await refreshMembers(activeWorkspace);
    } catch (err) {
      toast({ title: "Update failed", message: err instanceof Error ? err.message : "Request failed.", tone: "error" });
    } finally {
      setBusyAccount(null);
    }
  };

  const resetPassword = async (user: PlatformUser) => {
    setBusyAccount(user.email);
    try {
      const password = await resetPlatformUserPassword(user.email);
      if (password) setIssuedPassword({ email: user.email, password });
      toast({ title: "One-time password issued", message: `${user.email} must change it at next sign-in.`, tone: "success" });
      await loadUsers(platformAdmin);
    } catch (err) {
      toast({ title: "Reset failed", message: err instanceof Error ? err.message : "Request failed.", tone: "error" });
    } finally {
      setBusyAccount(null);
    }
  };

  if (loading) {
    return (
      <section className="df2-settings-section">
        <SectionLoader title="Loading team" hint="Reading workspaces, members, and accounts…" />
      </section>
    );
  }

  const workspaceName = workspaces.find((w) => w.id === activeWorkspace)?.name;

  return (
    <section className="df2-settings-section">
      <div className="df2-settings-section-head">
        <div>
          <h2>Team &amp; access</h2>
          <p>
            Workspaces isolate connectors, jobs, and audit. Members hold a role inside a workspace;
            a login lets them sign in at all. Switching workspace here is live for every Settings tab
            and the rest of the product.
          </p>
        </div>
        {updatedAt ? (
          <span className="df2-badge df2-badge-muted" data-testid="team-live-stamp">
            Live · {relativeTime(new Date(updatedAt).toISOString())}
          </span>
        ) : null}
      </div>
      <div className="df2-settings-section-body df2-team-body">
        {loadError && <div className="df2-team-error">{loadError}</div>}

        <PermissionNotice
          allowed={membership.allowed}
          reason={membership.reason}
          what="Membership is read-only for you."
        />

        {issuedPassword && (
          <IssuedPasswordNotice
            email={issuedPassword.email}
            password={issuedPassword.password}
            onDone={() => setIssuedPassword(null)}
          />
        )}

        <div className="df2-team-block">
          <div className="df2-team-block-head">
            <div>
              <h3>Workspace</h3>
              <p>The workspace every later request is decided in — including General, SSO, and notifications.</p>
            </div>
          </div>
          <div className="df2-team-workspace-grid">
            <div className="df2-settings-field">
              <label htmlFor="df2-team-workspace">Active workspace</label>
              <select
                id="df2-team-workspace"
                className="df2-select"
                data-testid="team-workspace-select"
                value={activeWorkspace}
                onChange={(e) => setActiveWorkspaceId(e.target.value)}
                disabled={workspaces.length === 0}
              >
                {workspaces.length === 0 && <option value="">No workspace yet</option>}
                {workspaces.length > 0 && !activeWorkspace && (
                  <option value="" disabled>
                    Choose a workspace
                  </option>
                )}
                {workspaces.map((w) => (
                  <option key={w.id} value={w.id}>
                    {w.name}
                    {typeof w.member_count === "number" ? ` — ${w.member_count} member(s)` : ""}
                  </option>
                ))}
              </select>
            </div>
            {platformAdmin && (
              <div className="df2-team-create">
                <div className="df2-settings-field">
                  <label htmlFor="df2-team-new-workspace">Create workspace</label>
                  <input
                    id="df2-team-new-workspace"
                    className="df2-input"
                    placeholder="Client or business unit name"
                    value={newWorkspaceName}
                    onChange={(e) => setNewWorkspaceName(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") void addWorkspace();
                    }}
                  />
                </div>
                <Button
                  variant="secondary"
                  disabled={creatingWorkspace || !newWorkspaceName.trim() || !manage.allowed}
                  title={manage.reason || undefined}
                  loading={creatingWorkspace}
                  loadingLabel="Creating…"
                  leadingIcon={<DtIcon name="plus" size={14} />}
                  onClick={() => void addWorkspace()}
                >
                  Create
                </Button>
              </div>
            )}
          </div>
        </div>

        {workspaces.length === 0 ? (
          <EmptyState
            compact
            icon="connectors"
            title="No workspace yet"
            description={
              platformAdmin
                ? "Create a workspace above, then add members to it."
                : "A platform administrator has to create a workspace and add you to it."
            }
          />
        ) : (
          <>
            {/* Invite and pending-invite tools exist only for someone who can use
                them; the read-only notice above explains why they are absent. */}
            {membership.allowed && (
            <div className="df2-team-block" data-testid="team-invite-form">
              <div className="df2-team-block-head">
                <div>
                  <h3>Invite a member</h3>
                  <p>
                    {workspaceName ? `Bring someone into ${workspaceName}.` : "Bring someone into this workspace."}{" "}
                    Viewer / Editor / Admin are the product roles — there is no Owner role.
                  </p>
                </div>
              </div>
              <fieldset className="df2-team-mode" data-testid="team-invite-mode" aria-label="How to add them">
                <label className="df2-team-mode-option">
                  <input
                    type="radio"
                    name="df2-team-invite-mode"
                    value="email"
                    checked={inviteMode === "email"}
                    onChange={() => setInviteMode("email")}
                  />
                  <div>
                    <strong>Email an invitation</strong>
                    <span>They get a single-use link and join when they accept. Nothing is granted before that.</span>
                  </div>
                </label>
                <label className="df2-team-mode-option">
                  <input
                    type="radio"
                    name="df2-team-invite-mode"
                    value="direct"
                    checked={inviteMode === "direct"}
                    onChange={() => setInviteMode("direct")}
                  />
                  <div>
                    <strong>Add directly</strong>
                    <span>Membership now, no email — for existing logins or deployments without mail.</span>
                  </div>
                </label>
              </fieldset>
              <div className="df2-team-invite-grid">
                <div className="df2-settings-field">
                  <label htmlFor="df2-team-email">Email address</label>
                  <input
                    id="df2-team-email"
                    className="df2-input"
                    type="email"
                    placeholder="colleague@company.com"
                    value={inviteEmail}
                    onChange={(e) => setInviteEmail(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") void invite();
                    }}
                  />
                </div>
                <div className="df2-settings-field">
                  <label htmlFor="df2-team-name">Full name</label>
                  <input
                    id="df2-team-name"
                    className="df2-input"
                    placeholder="Optional"
                    value={inviteName}
                    onChange={(e) => setInviteName(e.target.value)}
                  />
                </div>
                <div className="df2-settings-field">
                  <label htmlFor="df2-team-role">Role</label>
                  <select
                    id="df2-team-role"
                    className="df2-select"
                    value={inviteRole}
                    onChange={(e) => setInviteRole(e.target.value as WorkspaceRole)}
                  >
                    <option value="viewer">Viewer — read jobs, connectors and proofs</option>
                    <option value="editor">Editor — run transfers, edit connectors and schedules</option>
                    <option value="admin" disabled={!canGrantAdmin}>
                      Admin — full workspace access incl. roles{canGrantAdmin ? "" : " (workspace admin only)"}
                    </option>
                  </select>
                </div>
              </div>
              <div className="df2-team-invite-footer">
                {inviteMode === "email" ? (
                  <span className="df2-cell-meta">The link expires in 7 days and works once.</span>
                ) : (
                <label className="df2-team-check" htmlFor="df2-team-create-login">
                  <input
                    id="df2-team-create-login"
                    type="checkbox"
                    checked={createLogin && canGrantAdmin}
                    disabled={!canGrantAdmin}
                    onChange={(e) => setCreateLogin(e.target.checked)}
                  />
                  Create a login (one-time password){canGrantAdmin ? "" : " — workspace admin only"}
                </label>
                )}
                <Button
                  variant="primary"
                  disabled={inviting || !membership.allowed}
                  title={membership.reason || undefined}
                  loading={inviting}
                  loadingLabel={inviteMode === "email" ? "Sending…" : "Adding…"}
                  leadingIcon={<DtIcon name="plus" size={14} />}
                  onClick={() => void invite()}
                >
                  {inviteMode === "email" ? "Send invite" : "Add member"}
                </Button>
              </div>
            </div>
            )}

            {membership.allowed && invites.length > 0 && (
              <div className="df2-team-block" data-testid="team-invite-block">
                <div className="df2-team-block-head">
                  <div>
                    <h3>Pending invitations</h3>
                    <p>Resend sends a fresh link and retires the old one. Revoke stops the link working.</p>
                  </div>
                  <span className="df2-cell-meta">{invites.length} pending</span>
                </div>
                <div className="df2-team-list" data-testid="team-invite-list">
                  <div className="df2-team-list-head df2-team-list-head--invites">
                    <span>Person</span>
                    <span>Role</span>
                    <span>Link</span>
                    <span>Actions</span>
                  </div>
                  {invites.map((inv) => (
                    <div className="df2-team-member-row df2-team-member-row--invites" data-testid="team-invite-row" key={inv.id}>
                      <div className="df2-team-identity">
                        <span className="df2-team-avatar" aria-hidden>
                          {memberInitials(inv.name, inv.email)}
                        </span>
                        <div className="df2-team-identity-text">
                          <strong>{inv.name || inv.email}</strong>
                          <span>{inv.name ? `${inv.email} · ` : ""}invited by {inv.invited_by_name || inv.invited_by}</span>
                        </div>
                      </div>
                      <span className="df2-badge df2-badge-muted">{ROLE_LABELS[inv.role] ?? inv.role}</span>
                      <span className={`df2-cell-meta${isInviteExpired(inv.expires_at) ? " df2-text-warn" : ""}`}>
                        {inviteExpiryLabel(inv.expires_at)} · sent {relativeTime(inv.last_sent_at)}
                        {inv.sent_count > 1 ? ` (${inv.sent_count}×)` : ""}
                      </span>
                      <div className="df2-team-row-actions">
                        <Button
                          size="sm"
                          disabled={busyInvite === inv.id || !membership.allowed}
                          title={membership.reason || undefined}
                          loading={busyInvite === inv.id}
                          onClick={() => void resendInvite(inv)}
                        >
                          Resend
                        </Button>
                        <Button
                          size="sm"
                          variant="danger"
                          disabled={busyInvite === inv.id || !membership.allowed}
                          title={membership.reason || undefined}
                          onClick={() => void revokeInvite(inv)}
                        >
                          Revoke
                        </Button>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            <div className="df2-team-block">
              <div className="df2-team-block-head">
                <div>
                  <h3>Members</h3>
                  <p>Role changes apply immediately. Remove keeps the login but drops this workspace.</p>
                </div>
                <span className="df2-cell-meta">{members.length} member{members.length === 1 ? "" : "s"}</span>
              </div>
              {members.length === 0 ? (
                <EmptyState
                  compact
                  icon="connectors"
                  title="No members yet"
                  description="Add colleagues to this workspace as admin, editor, or viewer."
                />
              ) : (
                <div className="df2-team-list" data-testid="team-member-list">
                  <div className="df2-team-list-head">
                    <span>Person</span>
                    <span>Role</span>
                    <span>Login</span>
                    <span>Added</span>
                    <span>Actions</span>
                  </div>
                  {members.map((m) => {
                    const lastAdmin = isLastWorkspaceAdmin(members, m.email);
                    return (
                    <div className="df2-team-member-row" data-testid="team-member-row" key={m.email}>
                      <div className="df2-team-identity">
                        <span className="df2-team-avatar" aria-hidden>
                          {memberInitials(m.name, m.email)}
                        </span>
                        <div className="df2-team-identity-text">
                          <strong>{m.name || m.email}</strong>
                          {m.name ? <span>{m.email}</span> : null}
                        </div>
                      </div>
                      <select
                        className="df2-select df2-team-role-select"
                        aria-label={`Role for ${m.email}`}
                        value={m.role}
                        disabled={roleChangeEmail === m.email || !membership.allowed || lastAdmin}
                        title={
                          lastAdmin
                            ? LAST_ADMIN_PROTECTED
                            : membership.reason || undefined
                        }
                        onChange={(e) => void changeRole(m.email, e.target.value as WorkspaceRole)}
                      >
                        <option value="viewer">Viewer</option>
                        <option value="editor">Editor</option>
                        <option value="admin" disabled={!canGrantAdmin && m.role !== "admin"}>
                          Admin
                        </option>
                      </select>
                      <span className={`df2-badge ${accountTone(m.account_status)}`}>
                        {ACCOUNT_LABELS[m.account_status ?? "no_account"]}
                      </span>
                      <span className="df2-cell-meta">{relativeTime(m.added_at)}</span>
                      <Button
                        variant="danger"
                        size="sm"
                        disabled={removingEmail === m.email || !membership.allowed || lastAdmin}
                        title={
                          lastAdmin
                            ? LAST_ADMIN_PROTECTED
                            : membership.reason || undefined
                        }
                        loading={removingEmail === m.email}
                        loadingLabel="Removing…"
                        onClick={() => void remove(m.email)}
                      >
                        Remove
                      </Button>
                    </div>
                    );
                  })}
                </div>
              )}
            </div>
          </>
        )}

        {platformAdmin && (
          <div className="df2-team-block">
            <div className="df2-team-block-head">
              <div>
                <h3>Platform logins</h3>
                <p>
                  Every account that can sign in to this deployment. Disabling a login blocks sign-in
                  immediately and keeps the audit trail.
                </p>
              </div>
              <span className="df2-cell-meta">{users.length} login{users.length === 1 ? "" : "s"}</span>
            </div>
            {users.length === 0 ? (
              <EmptyState
                compact
                icon="connectors"
                title="No stored logins"
                description="Members you add with “Create a login” appear here, alongside any environment-provisioned admin."
              />
            ) : (
              <div className="df2-team-list" data-testid="team-login-list">
                <div className="df2-team-list-head df2-team-list-head--logins">
                  <span>Person</span>
                  <span>Platform</span>
                  <span>Status</span>
                  <span>Workspaces</span>
                  <span>Last sign-in</span>
                  <span>Actions</span>
                </div>
                {users.map((u) => (
                  <div className="df2-team-member-row df2-team-member-row--logins" data-testid="team-login-row" key={u.email}>
                    <div className="df2-team-identity">
                      <span className="df2-team-avatar" aria-hidden>
                        {memberInitials(u.name, u.email)}
                      </span>
                      <div className="df2-team-identity-text">
                        <strong>{u.name || u.email}</strong>
                        {u.name ? <span>{u.email}</span> : null}
                      </div>
                    </div>
                    <span className={`df2-badge ${u.role === "admin" ? "df2-badge-live" : "df2-badge-muted"}`}>
                      {u.role === "admin" ? "Platform admin" : "Member"}
                    </span>
                    <span className={`df2-badge ${u.status === "active" ? "df2-badge-live" : "df2-badge-warn"}`}>
                      {u.status === "active" ? "Active" : "Disabled"}
                    </span>
                    <span className="df2-cell-meta">{u.workspaces?.length ?? 0}</span>
                    <span className="df2-cell-meta">{u.last_login_at ? relativeTime(u.last_login_at) : "Never"}</span>
                    <div className="df2-team-row-actions">
                      <Button
                        size="sm"
                        disabled={busyAccount === u.email}
                        loading={busyAccount === u.email}
                        onClick={() => void resetPassword(u)}
                      >
                        Reset password
                      </Button>
                      <Button
                        size="sm"
                        variant={u.status === "active" ? "danger" : "secondary"}
                        disabled={busyAccount === u.email}
                        onClick={() => void setStatus(u, u.status === "active" ? "disabled" : "active")}
                      >
                        {u.status === "active" ? "Disable" : "Enable"}
                      </Button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </section>
  );
}
