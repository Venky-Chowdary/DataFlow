import { useCallback, useEffect, useState } from "react";
import { DtIcon } from "../../components/DtIcon";
import { EmptyState } from "../../components/ui/EmptyState";
import { SectionLoader } from "../../components/LoadingState";
import { useToast } from "../../components/Toast";
import { useConfirm } from "../../components/ui/ConfirmDialog";
import {
  addWorkspaceMember,
  createWorkspace,
  fetchPlatformUsers,
  fetchWorkspaceMembers,
  fetchWorkspaces,
  removeWorkspaceMember,
  resetPlatformUserPassword,
  updatePlatformUser,
  updateWorkspaceMemberRole,
  type PlatformUser,
  type Workspace,
  type WorkspaceMember,
  type WorkspaceRole,
} from "../../lib/api";
import { getActiveWorkspaceId, setActiveWorkspaceId } from "../../lib/workspace";
import { PermissionNotice } from "../../components/PermissionNotice";
import { PERMISSIONS, useWriteGate } from "../../lib/PermissionsContext";

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

export function TeamSettings() {
  const { toast } = useToast();
  const { confirm } = useConfirm();
  // Two authorities, because the API has two: creating a workspace is workspace
  // administration, while bringing a peer into the workspace you already work in
  // is member.invite — which an editor holds. Gating both on workspace.manage
  // disabled a control the API would have honoured for an editor.
  const manage = useWriteGate(PERMISSIONS.workspaceManage);
  const membership = useWriteGate(PERMISSIONS.memberInvite);
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [platformAdmin, setPlatformAdmin] = useState(false);
  // The workspace being administered here is also the workspace every other
  // request is decided in: authority is per workspace, so a page that showed one
  // workspace's members while the API answered for another would gate wrongly.
  const [selectedWorkspace, setSelectedWorkspace] = useState<string>(() => getActiveWorkspaceId());
  const [members, setMembers] = useState<WorkspaceMember[]>([]);
  const [users, setUsers] = useState<PlatformUser[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string>("");
  const [newWorkspaceName, setNewWorkspaceName] = useState("");
  const [creatingWorkspace, setCreatingWorkspace] = useState(false);
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteName, setInviteName] = useState("");
  const [inviteRole, setInviteRole] = useState<WorkspaceRole>("viewer");
  const [createLogin, setCreateLogin] = useState(true);
  const [inviting, setInviting] = useState(false);
  const [removingEmail, setRemovingEmail] = useState<string | null>(null);
  const [roleChangeEmail, setRoleChangeEmail] = useState<string | null>(null);
  const [busyAccount, setBusyAccount] = useState<string | null>(null);
  // A one-time password is shown once, in the operator's own session, and never
  // stored client-side — an admin has to hand it over before leaving the screen.
  const [issuedPassword, setIssuedPassword] = useState<{ email: string; password: string } | null>(null);

  const loadWorkspaces = useCallback(async () => {
    const { workspaces: rows, platformAdmin: isAdmin } = await fetchWorkspaces();
    setWorkspaces(rows);
    setPlatformAdmin(isAdmin);
    setSelectedWorkspace((current) => (current && rows.some((w) => w.id === current) ? current : (rows[0]?.id ?? "")));
    return isAdmin;
  }, []);

  const loadUsers = useCallback(async (isAdmin: boolean) => {
    if (!isAdmin) {
      setUsers([]);
      return;
    }
    setUsers(await fetchPlatformUsers().catch(() => []));
  }, []);

  useEffect(() => {
    setLoading(true);
    loadWorkspaces()
      .then(async (isAdmin) => {
        await loadUsers(isAdmin);
        setLoadError("");
      })
      .catch((err) => setLoadError(err instanceof Error ? err.message : "Could not load team settings."))
      .finally(() => setLoading(false));
  }, [loadWorkspaces, loadUsers]);

  const refreshMembers = useCallback(async (workspaceId: string) => {
    if (!workspaceId) {
      setMembers([]);
      return;
    }
    try {
      setMembers(await fetchWorkspaceMembers(workspaceId));
      setLoadError("");
    } catch (err) {
      // An unreadable member list is not an empty workspace.
      setMembers([]);
      setLoadError(err instanceof Error ? err.message : "Could not read this workspace's members.");
    }
  }, []);

  useEffect(() => {
    // Administering a workspace here also chooses it for the rest of the client,
    // so every later request is decided by this workspace's membership role.
    setActiveWorkspaceId(selectedWorkspace);
    void refreshMembers(selectedWorkspace);
  }, [selectedWorkspace, refreshMembers]);

  /** Refuse in words rather than sending a request that will be refused. */
  const refuse = (reason: string) => {
    toast({ title: "No write permission", message: reason, tone: "warning" });
    return false;
  };
  // Granting admin is the one membership change an editor may not make, and the
  // store refuses it — so the option says why instead of failing on submit.
  const canGrantAdmin = manage.allowed;

  const addWorkspace = async () => {
    if (!manage.allowed) return void refuse(manage.reason);
    const name = newWorkspaceName.trim();
    if (!name) return;
    setCreatingWorkspace(true);
    try {
      const ws = await createWorkspace(name);
      setNewWorkspaceName("");
      await loadWorkspaces();
      setSelectedWorkspace(ws.id);
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
    if (!selectedWorkspace) {
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
    try {
      const result = await addWorkspaceMember(selectedWorkspace, email, inviteRole, {
        createAccount: createLogin,
        name: inviteName.trim(),
      });
      setInviteEmail("");
      setInviteName("");
      if (result.temporaryPassword) {
        setIssuedPassword({ email: result.membership.email, password: result.temporaryPassword });
        toast({
          title: "Member added with a login",
          message: `Give ${result.membership.email} the one-time password shown below.`,
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
      await Promise.all([refreshMembers(selectedWorkspace), loadUsers(platformAdmin), loadWorkspaces()]);
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

  const remove = async (email: string) => {
    if (!membership.allowed) return void refuse(membership.reason);
    if (!selectedWorkspace) return;
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
      await removeWorkspaceMember(selectedWorkspace, email);
      toast({ title: "Member removed", tone: "info" });
      await Promise.all([refreshMembers(selectedWorkspace), loadWorkspaces()]);
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
    if (!selectedWorkspace) return;
    setRoleChangeEmail(email);
    try {
      const membership = await updateWorkspaceMemberRole(selectedWorkspace, email, role);
      toast({ title: `${email} is now ${ROLE_LABELS[membership.role] ?? membership.role}`, tone: "success" });
      await refreshMembers(selectedWorkspace);
    } catch (err) {
      toast({
        title: "Role change failed",
        message: err instanceof Error ? err.message : "Could not change role.",
        tone: "error",
      });
      await refreshMembers(selectedWorkspace);
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
      await refreshMembers(selectedWorkspace);
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

  return (
    <section className="df2-settings-section">
      <div className="df2-settings-section-head">
        <div>
          <h2>Team &amp; access</h2>
          <p>Workspaces isolate one client's connectors, jobs and audit trail. Members hold a role inside a workspace; a login lets them sign in at all.</p>
        </div>
      </div>
      <div className="df2-settings-section-body">
        {loadError && <div className="df2-team-error df2-mb-md">{loadError}</div>}

        <PermissionNotice
          allowed={membership.allowed}
          reason={membership.reason}
          what="Membership is read-only for you."
        />

        {issuedPassword && (
          <div className="df2-team-secret df2-mb-md">
            <strong>One-time password for {issuedPassword.email}</strong>
            <code className="df2-team-secret-value">{issuedPassword.password}</code>
            <span>Shown once. They will be asked to change it at first sign-in.</span>
            <button type="button" className="df2-btn df2-btn-sm" onClick={() => setIssuedPassword(null)}>
              Done
            </button>
          </div>
        )}

        <div className="df2-team-toolbar df2-mb-md">
          <div className="df2-settings-field">
            <label htmlFor="df2-team-workspace">Workspace</label>
            <select
              id="df2-team-workspace"
              className="df2-select"
              value={selectedWorkspace}
              onChange={(e) => setSelectedWorkspace(e.target.value)}
              disabled={workspaces.length === 0}
            >
              {workspaces.length === 0 && <option value="">No workspace yet</option>}
              {workspaces.map((w) => (
                <option key={w.id} value={w.id}>
                  {w.name}
                  {typeof w.member_count === "number" ? ` — ${w.member_count} member(s)` : ""}
                </option>
              ))}
            </select>
          </div>
          {platformAdmin && (
            <>
              <div className="df2-settings-field">
                <label htmlFor="df2-team-new-workspace">New workspace</label>
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
              <button
                type="button"
                className="df2-btn"
                disabled={creatingWorkspace || !newWorkspaceName.trim() || !manage.allowed}
                title={manage.reason || undefined}
                onClick={() => void addWorkspace()}
              >
                <DtIcon name="plus" size={14} />
                {creatingWorkspace ? "Creating…" : "Create workspace"}
              </button>
            </>
          )}
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
            <div className="df2-team-toolbar df2-mb-md">
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
                  <option value="editor">Editor — run transfers, edit connectors, add non-admin members</option>
                  <option value="admin" disabled={!canGrantAdmin}>
                    Admin — full workspace access incl. roles{canGrantAdmin ? "" : " (workspace admin only)"}
                  </option>
                </select>
              </div>
              <label className="df2-team-check" htmlFor="df2-team-create-login">
                <input
                  id="df2-team-create-login"
                  type="checkbox"
                  checked={createLogin}
                  onChange={(e) => setCreateLogin(e.target.checked)}
                />
                Create a login (one-time password)
              </label>
              <button
                type="button"
                className="df2-btn df2-btn-primary"
                disabled={inviting || !membership.allowed}
                title={membership.reason || undefined}
                onClick={() => void invite()}
              >
                <DtIcon name="plus" size={14} />
                {inviting ? "Adding…" : "Add member"}
              </button>
            </div>

            {members.length === 0 ? (
              <EmptyState
                compact
                icon="connectors"
                title="No members yet"
                description="Add colleagues to this workspace as admin, editor, or viewer."
              />
            ) : (
              <div className="df2-settings-table-wrap">
                <table className="df2-settings-logs-table">
                  <thead>
                    <tr>
                      <th>Email</th>
                      <th>Role</th>
                      <th>Login</th>
                      <th>Added</th>
                      <th style={{ width: 120 }}>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {members.map((m) => (
                      <tr key={m.email}>
                        <td>
                          {m.email}
                          {m.name ? <span className="df2-muted"> · {m.name}</span> : null}
                        </td>
                        <td>
                          <select
                            className="df2-select df2-team-role-select"
                            aria-label={`Role for ${m.email}`}
                            value={m.role}
                            disabled={roleChangeEmail === m.email || !membership.allowed}
                            title={membership.reason || undefined}
                            onChange={(e) => void changeRole(m.email, e.target.value as WorkspaceRole)}
                          >
                            <option value="viewer">Viewer</option>
                            <option value="editor">Editor</option>
                            <option value="admin" disabled={!canGrantAdmin && m.role !== "admin"}>
                              Admin
                            </option>
                          </select>
                        </td>
                        <td>{ACCOUNT_LABELS[m.account_status ?? "no_account"]}</td>
                        <td>{m.added_at ? new Date(m.added_at).toLocaleString() : "—"}</td>
                        <td>
                          <button
                            type="button"
                            className="df2-btn df2-btn-sm df2-btn-danger"
                            disabled={removingEmail === m.email || !membership.allowed}
                            title={membership.reason || undefined}
                            onClick={() => void remove(m.email)}
                          >
                            {removingEmail === m.email ? "Removing…" : "Remove"}
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </>
        )}

        {platformAdmin && (
          <div className="df2-team-subsection">
            <h3>Logins</h3>
            <p className="df2-muted">
              Every account that can sign in to this deployment. Disabling a login blocks sign-in immediately and keeps
              the audit trail.
            </p>
            {users.length === 0 ? (
              <EmptyState
                compact
                icon="connectors"
                title="No stored logins"
                description="Members you add with “Create a login” appear here, alongside any environment-provisioned admin."
              />
            ) : (
              <div className="df2-settings-table-wrap">
                <table className="df2-settings-logs-table">
                  <thead>
                    <tr>
                      <th>Email</th>
                      <th>Platform role</th>
                      <th>Status</th>
                      <th>Workspaces</th>
                      <th>Last sign-in</th>
                      <th style={{ width: 200 }}>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {users.map((u) => (
                      <tr key={u.email}>
                        <td>
                          {u.email}
                          {u.name ? <span className="df2-muted"> · {u.name}</span> : null}
                        </td>
                        <td>
                          <span className={`df2-badge ${u.role === "admin" ? "df2-badge-live" : "df2-badge-muted"}`}>
                            {u.role === "admin" ? "Platform admin" : "Member"}
                          </span>
                        </td>
                        <td>{u.status === "active" ? "Active" : "Disabled"}</td>
                        <td>{u.workspaces?.length ?? 0}</td>
                        <td>{u.last_login_at ? new Date(u.last_login_at).toLocaleString() : "Never"}</td>
                        <td>
                          <button
                            type="button"
                            className="df2-btn df2-btn-sm"
                            disabled={busyAccount === u.email}
                            onClick={() => void resetPassword(u)}
                          >
                            Reset password
                          </button>{" "}
                          <button
                            type="button"
                            className={`df2-btn df2-btn-sm ${u.status === "active" ? "df2-btn-danger" : ""}`}
                            disabled={busyAccount === u.email}
                            onClick={() => void setStatus(u, u.status === "active" ? "disabled" : "active")}
                          >
                            {u.status === "active" ? "Disable" : "Enable"}
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}
      </div>
    </section>
  );
}
