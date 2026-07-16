import { useEffect, useState } from "react";
import { DtIcon } from "../../components/DtIcon";
import { EmptyState } from "../../components/EmptyState";
import { SectionLoader } from "../../components/LoadingState";
import { useToast } from "../../components/Toast";
import { addWorkspaceMember, fetchWorkspaceMembers, fetchWorkspaces, removeWorkspaceMember, type Workspace, type WorkspaceMember } from "../../lib/api";

const ROLE_LABELS: Record<string, string> = {
  owner: "Owner",
  editor: "Editor",
  viewer: "Viewer",
};

export function TeamSettings() {
  const { toast } = useToast();
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [selectedWorkspace, setSelectedWorkspace] = useState<string>("");
  const [members, setMembers] = useState<WorkspaceMember[]>([]);
  const [loading, setLoading] = useState(false);
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteRole, setInviteRole] = useState<"viewer" | "editor" | "owner">("viewer");
  const [inviting, setInviting] = useState(false);
  const [removingEmail, setRemovingEmail] = useState<string | null>(null);

  useEffect(() => {
    fetchWorkspaces()
      .then((w) => {
        setWorkspaces(w);
        if (w.length && !selectedWorkspace) setSelectedWorkspace(w[0].id);
      })
      .catch(() => setWorkspaces([]));
  }, []);

  useEffect(() => {
    if (!selectedWorkspace) return;
    setLoading(true);
    fetchWorkspaceMembers(selectedWorkspace)
      .then(setMembers)
      .catch(() => setMembers([]))
      .finally(() => setLoading(false));
  }, [selectedWorkspace]);

  const invite = async () => {
    if (!selectedWorkspace || !inviteEmail.trim()) return;
    setInviting(true);
    try {
      await addWorkspaceMember(selectedWorkspace, inviteEmail.trim(), inviteRole);
      toast({ title: "Member added", message: `${inviteEmail} is now a ${inviteRole}.`, tone: "success" });
      setInviteEmail("");
      setMembers(await fetchWorkspaceMembers(selectedWorkspace));
    } catch (err) {
      toast({ title: "Invite failed", message: err instanceof Error ? err.message : "Could not add member.", tone: "error" });
    } finally {
      setInviting(false);
    }
  };

  const remove = async (email: string) => {
    if (!selectedWorkspace) return;
    if (!window.confirm(`Remove ${email} from this workspace?`)) return;
    setRemovingEmail(email);
    try {
      await removeWorkspaceMember(selectedWorkspace, email);
      toast({ title: "Member removed", tone: "info" });
      setMembers(await fetchWorkspaceMembers(selectedWorkspace));
    } catch (err) {
      toast({ title: "Remove failed", message: err instanceof Error ? err.message : "Could not remove member.", tone: "error" });
    } finally {
      setRemovingEmail(null);
    }
  };

  return (
    <section className="df2-settings-section">
      <div className="df2-settings-section-head">
        <div>
          <h2>Team members</h2>
          <p>Manage who can configure connectors, run transfers, and view audit logs.</p>
        </div>
      </div>
      <div className="df2-settings-section-body">
        {workspaces.length > 1 && (
          <div className="df2-settings-field df2-mb-md">
            <label>Workspace</label>
            <select className="df2-select" value={selectedWorkspace} onChange={(e) => setSelectedWorkspace(e.target.value)}>
              {workspaces.map((w) => (
                <option key={w.id} value={w.id}>{w.name}</option>
              ))}
            </select>
          </div>
        )}

        <div className="df2-api-key-toolbar df2-mb-md">
          <div className="df2-settings-field">
            <label>Email address</label>
            <input
              className="df2-input"
              type="email"
              placeholder="colleague@company.com"
              value={inviteEmail}
              onChange={(e) => setInviteEmail(e.target.value)}
              onKeyDown={(e) => { if (e.key === "Enter") void invite(); }}
            />
          </div>
          <div className="df2-settings-field">
            <label>Role</label>
            <select className="df2-select" value={inviteRole} onChange={(e) => setInviteRole(e.target.value as "viewer" | "editor" | "owner")}>
              <option value="viewer">Viewer — view jobs and connectors</option>
              <option value="editor">Editor — run transfers and edit connectors</option>
              <option value="owner">Owner — full workspace access</option>
            </select>
          </div>
          <button
            type="button"
            className="df2-btn df2-btn-primary"
            disabled={inviting || !inviteEmail.trim()}
            onClick={() => void invite()}
          >
            <DtIcon name="plus" size={14} />
            {inviting ? "Adding…" : "Add member"}
          </button>
        </div>

        {loading ? (
          <SectionLoader title="Loading members" hint="Fetching workspace members…" />
        ) : members.length === 0 ? (
          <EmptyState
            compact
            icon="connectors"
            title="No members yet"
            description="Add colleagues to this workspace and assign owner, editor, or viewer roles."
          />
        ) : (
          <div className="df2-settings-table-wrap">
            <table className="df2-settings-logs-table">
              <thead>
                <tr>
                  <th>Email</th>
                  <th>Role</th>
                  <th>Added</th>
                  <th style={{ width: 120 }}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {members.map((m) => (
                  <tr key={m.email}>
                    <td>{m.email}</td>
                    <td><span className={`df2-badge ${m.role === "owner" ? "df2-badge-live" : m.role === "editor" ? "df2-badge-run" : "df2-badge-muted"}`}>{ROLE_LABELS[m.role] || m.role}</span></td>
                    <td>{m.added_at ? new Date(m.added_at).toLocaleString() : "—"}</td>
                    <td>
                      <button
                        type="button"
                        className="df2-btn df2-btn-sm df2-btn-danger"
                        disabled={removingEmail === m.email}
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
      </div>
    </section>
  );
}
