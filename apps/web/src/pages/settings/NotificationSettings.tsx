import { useEffect, useState } from "react";
import { DtIcon } from "../../components/DtIcon";
import { EmptyState } from "../../components/EmptyState";
import { SectionLoader } from "../../components/LoadingState";
import { useToast } from "../../components/Toast";
import {
  createNotificationChannel,
  deleteNotificationChannel,
  fetchNotificationChannels,
  fetchWorkspaces,
  testNotificationChannel,
  updateNotificationChannel,
  type NotificationChannel,
  type Workspace,
} from "../../lib/api";

const KIND_LABELS: Record<string, { label: string; icon: string; fields: string[] }> = {
  slack: { label: "Slack", icon: "message", fields: ["webhook_url"] },
  teams: { label: "Microsoft Teams", icon: "message", fields: ["webhook_url"] },
  email: { label: "Email", icon: "mail", fields: ["recipients", "smtp_host", "smtp_port", "smtp_user", "smtp_password", "from"] },
  servicenow: { label: "ServiceNow", icon: "ticket", fields: ["url", "auth_mode", "username", "password", "token"] },
  webhook: { label: "Generic Webhook", icon: "globe", fields: ["url", "headers"] },
};

const FIELD_LABELS: Record<string, string> = {
  webhook_url: "Webhook URL",
  recipients: "Recipients (comma separated)",
  smtp_host: "SMTP host",
  smtp_port: "SMTP port",
  smtp_user: "SMTP user",
  smtp_password: "SMTP password",
  from: "From address",
  url: "Endpoint URL",
  auth_mode: "Auth mode",
  username: "Username",
  password: "Password",
  token: "Token / API key",
  headers: "Headers (JSON)",
};

export function NotificationSettings() {
  const { toast } = useToast();
  const [channels, setChannels] = useState<NotificationChannel[]>([]);
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState<string | null>(null);
  const [selectedWorkspace, setSelectedWorkspace] = useState<string>("");
  const [kind, setKind] = useState<"slack" | "teams" | "email" | "servicenow" | "webhook">("slack");
  const [label, setLabel] = useState("");
  const [configJson, setConfigJson] = useState("{}");

  useEffect(() => {
    fetchWorkspaces()
      .then((w) => {
        setWorkspaces(w);
        if (w.length && !selectedWorkspace) setSelectedWorkspace(w[0].id);
      })
      .catch(() => setWorkspaces([]));
  }, []);

  useEffect(() => {
    loadChannels();
  }, [selectedWorkspace]);

  const loadChannels = () => {
    setLoading(true);
    fetchNotificationChannels(selectedWorkspace || undefined)
      .then((data) => setChannels(data.channels))
      .catch(() => setChannels([]))
      .finally(() => setLoading(false));
  };

  const parseConfig = (): Record<string, unknown> | null => {
    try {
      return JSON.parse(configJson || "{}") as Record<string, unknown>;
    } catch {
      toast({ title: "Invalid JSON config", tone: "error" });
      return null;
    }
  };

  const add = async () => {
    const cfg = parseConfig();
    if (!cfg) return;
    setSaving(true);
    try {
      await createNotificationChannel({
        workspace_id: selectedWorkspace,
        kind,
        label: label || KIND_LABELS[kind].label,
        enabled: true,
        config: cfg,
      });
      toast({ title: "Channel added", tone: "success" });
      setLabel("");
      setConfigJson("{}");
      loadChannels();
    } catch (err) {
      toast({ title: "Could not add channel", message: err instanceof Error ? err.message : "Save failed.", tone: "error" });
    } finally {
      setSaving(false);
    }
  };

  const toggle = async (channel: NotificationChannel) => {
    try {
      await updateNotificationChannel(channel.id, { enabled: !channel.enabled });
      setChannels((prev) => prev.map((c) => (c.id === channel.id ? { ...c, enabled: !c.enabled } : c)));
    } catch (err) {
      toast({ title: "Update failed", message: err instanceof Error ? err.message : "", tone: "error" });
    }
  };

  const remove = async (id: string) => {
    if (!window.confirm("Delete this notification channel?")) return;
    try {
      await deleteNotificationChannel(id);
      setChannels((prev) => prev.filter((c) => c.id !== id));
    } catch (err) {
      toast({ title: "Delete failed", message: err instanceof Error ? err.message : "", tone: "error" });
    }
  };

  const test = async (id: string) => {
    setTesting(id);
    try {
      const result = await testNotificationChannel(id);
      toast({
        title: result.success ? "Test message sent" : "Test failed",
        message: result.success ? "Check the destination for the alert." : JSON.stringify(result.detail).slice(0, 200),
        tone: result.success ? "success" : "error",
      });
    } catch (err) {
      toast({ title: "Test failed", message: err instanceof Error ? err.message : "", tone: "error" });
    } finally {
      setTesting(null);
    }
  };

  const fields = KIND_LABELS[kind].fields;

  const setExample = () => {
    const examples: Record<string, string> = {
      slack: JSON.stringify({ webhook_url: "https://hooks.slack.com/services/..." }, null, 2),
      teams: JSON.stringify({ webhook_url: "https://company.webhook.office.com/webhookb2/..." }, null, 2),
      email: JSON.stringify({ recipients: "ops@company.com", smtp_host: "smtp.company.com", smtp_port: 587, smtp_user: "", smtp_password: "", from: "dataflow@company.com" }, null, 2),
      servicenow: JSON.stringify({ url: "https://company.service-now.com/api/now/table/incident", auth_mode: "basic", username: "", password: "" }, null, 2),
      webhook: JSON.stringify({ url: "https://company.com/webhook/dataflow", headers: { "Authorization": "Bearer token" } }, null, 2),
    };
    setConfigJson(examples[kind]);
  };

  return (
    <section className="df2-settings-section">
      <div className="df2-settings-section-head">
        <div>
          <h2>Notification channels</h2>
          <p>Route job failure and quarantine alerts to Slack, Teams, email, ServiceNow, or a webhook.</p>
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

        <div className="df2-settings-grid df2-mb-md" style={{ alignItems: "end" }}>
          <div className="df2-settings-field">
            <label>Channel type</label>
            <select className="df2-select" value={kind} onChange={(e) => { setKind(e.target.value as typeof kind); setConfigJson("{}"); }}>
              <option value="slack">Slack</option>
              <option value="teams">Microsoft Teams</option>
              <option value="email">Email (SMTP)</option>
              <option value="servicenow">ServiceNow</option>
              <option value="webhook">Generic Webhook</option>
            </select>
          </div>
          <div className="df2-settings-field">
            <label>Label</label>
            <input className="df2-input" value={label} onChange={(e) => setLabel(e.target.value)} placeholder={KIND_LABELS[kind].label} />
          </div>
          <div className="df2-settings-field df2-settings-field--full">
            <label>
              Config (JSON)
              <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" style={{ marginLeft: 8 }} onClick={setExample}>Insert example</button>
            </label>
            <textarea className="df2-input" rows={8} value={configJson} onChange={(e) => setConfigJson(e.target.value)} />
          </div>
        </div>
        <p className="df2-settings-hint df2-mb-md">
          Required fields: {fields.map((f) => FIELD_LABELS[f] || f).join(", ")}.
        </p>
        <button type="button" className="df2-btn df2-btn-primary df2-mb-md" disabled={saving} onClick={() => void add()}>
          <DtIcon name="plus" size={14} /> {saving ? "Saving…" : "Add channel"}
        </button>

        {loading ? (
          <SectionLoader title="Loading channels" hint="Fetching notification channels…" />
        ) : channels.length === 0 ? (
          <EmptyState compact icon="bell" title="No channels yet" description="Add a channel to receive job alerts and quarantine notifications." />
        ) : (
          <div className="df2-settings-table-wrap">
            <table className="df2-settings-logs-table">
              <thead>
                <tr>
                  <th>Channel</th>
                  <th>Type</th>
                  <th>Status</th>
                  <th style={{ width: 180 }}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {channels.map((c) => (
                  <tr key={c.id}>
                    <td>{c.label}</td>
                    <td>{KIND_LABELS[c.kind]?.label || c.kind}</td>
                    <td>
                      <button
                        type="button"
                        role="switch"
                        aria-checked={c.enabled}
                        className={`df2-switch ${c.enabled ? "on" : ""}`}
                        onClick={() => void toggle(c)}
                      >
                        <span className="df2-switch-thumb" />
                      </button>
                    </td>
                    <td>
                      <div style={{ display: "flex", gap: 8 }}>
                        <button type="button" className="df2-btn df2-btn-sm" disabled={testing === c.id} onClick={() => void test(c.id)}>{testing === c.id ? "Testing…" : "Test"}</button>
                        <button type="button" className="df2-btn df2-btn-sm df2-btn-danger" onClick={() => void remove(c.id)}>Delete</button>
                      </div>
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
