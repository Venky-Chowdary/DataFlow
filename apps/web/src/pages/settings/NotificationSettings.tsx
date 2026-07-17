import { useEffect, useMemo, useState } from "react";
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

type ChannelKind = "slack" | "teams" | "email" | "servicenow" | "webhook";

const KIND_META: Record<ChannelKind, { label: string; description: string }> = {
  slack: { label: "Slack", description: "Post to a Slack incoming webhook when a job fails or rows are quarantined." },
  teams: { label: "Microsoft Teams", description: "Post to a Teams incoming webhook when a job fails or rows are quarantined." },
  email: { label: "Email", description: "Send alerts to one or more email addresses. Uses the platform mailer by default; optionally provide your own SMTP." },
  servicenow: { label: "ServiceNow", description: "Create or update an incident record via the Table API." },
  webhook: { label: "Generic Webhook", description: "POST a JSON payload to any HTTPS endpoint." },
};

export function NotificationSettings() {
  const { toast } = useToast();
  const [channels, setChannels] = useState<NotificationChannel[]>([]);
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState<string | null>(null);
  const [selectedWorkspace, setSelectedWorkspace] = useState<string>("");

  const [kind, setKind] = useState<ChannelKind>("email");
  const [label, setLabel] = useState("");

  // Common fields
  const [recipients, setRecipients] = useState("");
  const [webhookUrl, setWebhookUrl] = useState("");
  const [endpointUrl, setEndpointUrl] = useState("");
  const [headers, setHeaders] = useState("");

  // Email optional custom SMTP
  const [useCustomSmtp, setUseCustomSmtp] = useState(false);
  const [smtpHost, setSmtpHost] = useState("");
  const [smtpPort, setSmtpPort] = useState("");
  const [smtpUser, setSmtpUser] = useState("");
  const [smtpPassword, setSmtpPassword] = useState("");
  const [fromAddress, setFromAddress] = useState("");

  // ServiceNow auth
  const [authMode, setAuthMode] = useState<"basic" | "oauth">("basic");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [token, setToken] = useState("");

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

  const resetForm = () => {
    setLabel("");
    setRecipients("");
    setWebhookUrl("");
    setEndpointUrl("");
    setHeaders("");
    setSmtpHost("");
    setSmtpPort("");
    setSmtpUser("");
    setSmtpPassword("");
    setFromAddress("");
    setUsername("");
    setPassword("");
    setToken("");
    setUseCustomSmtp(false);
  };

  const buildConfig = useMemo(() => {
    const cfg: Record<string, unknown> = {};
    if (kind === "email") {
      cfg.recipients = recipients;
      if (useCustomSmtp) {
        cfg.smtp_host = smtpHost;
        cfg.smtp_port = smtpPort ? Number(smtpPort) : 0;
        cfg.smtp_user = smtpUser;
        cfg.smtp_password = smtpPassword;
        cfg.from = fromAddress;
      }
    } else if (kind === "slack" || kind === "teams") {
      cfg.webhook_url = webhookUrl;
    } else if (kind === "servicenow") {
      cfg.url = endpointUrl;
      cfg.auth_mode = authMode;
      if (authMode === "basic") {
        cfg.username = username;
        cfg.password = password;
      } else {
        cfg.token = token;
      }
    } else if (kind === "webhook") {
      cfg.url = endpointUrl;
      if (headers.trim()) {
        try {
          cfg.headers = JSON.parse(headers);
        } catch {
          // ignore invalid headers; backend will skip
        }
      }
    }
    return cfg;
  }, [kind, recipients, webhookUrl, endpointUrl, headers, useCustomSmtp, smtpHost, smtpPort, smtpUser, smtpPassword, fromAddress, authMode, username, password, token]);

  const canAdd = useMemo(() => {
    if (kind === "email") return recipients.trim().length > 0;
    if (kind === "slack" || kind === "teams") return webhookUrl.trim().length > 0;
    if (kind === "servicenow" || kind === "webhook") return endpointUrl.trim().length > 0;
    return false;
  }, [kind, recipients, webhookUrl, endpointUrl]);

  const add = async () => {
    setSaving(true);
    try {
      await createNotificationChannel({
        workspace_id: selectedWorkspace,
        kind,
        label: label.trim() || KIND_META[kind].label,
        enabled: true,
        config: buildConfig,
      });
      toast({ title: "Channel added", tone: "success" });
      resetForm();
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

  const summaryFor = (channel: NotificationChannel) => {
    const cfg = channel.config as Record<string, unknown>;
    if (channel.kind === "email") return String(cfg.recipients || cfg.to || "").slice(0, 60);
    if (channel.kind === "slack" || channel.kind === "teams") return String(cfg.webhook_url || "").slice(0, 60);
    return String(cfg.url || "").slice(0, 60);
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

        <div className="df2-settings-channel-form df2-mb-md">
          <div className="df2-settings-grid df2-settings-grid--channel">
          <div className="df2-settings-field">
            <label>Channel type</label>
            <select className="df2-select" value={kind} onChange={(e) => { setKind(e.target.value as ChannelKind); resetForm(); }}>
              <option value="email">Email</option>
              <option value="slack">Slack</option>
              <option value="teams">Microsoft Teams</option>
              <option value="servicenow">ServiceNow</option>
              <option value="webhook">Generic Webhook</option>
            </select>
            <p className="df2-settings-hint">{KIND_META[kind].description}</p>
          </div>
          <div className="df2-settings-field">
            <label>Label</label>
            <input className="df2-input" value={label} onChange={(e) => setLabel(e.target.value)} placeholder={KIND_META[kind].label} />
          </div>

          {kind === "email" && (
            <div className="df2-settings-field df2-settings-field--full">
              <label>Email recipients</label>
              <input className="df2-input" value={recipients} onChange={(e) => setRecipients(e.target.value)} placeholder="ops@company.com, security@company.com" />
            </div>
          )}

          {(kind === "slack" || kind === "teams") && (
            <div className="df2-settings-field df2-settings-field--full">
              <label>Incoming webhook URL</label>
              <input className="df2-input" value={webhookUrl} onChange={(e) => setWebhookUrl(e.target.value)} placeholder={kind === "slack" ? "https://hooks.slack.com/services/..." : "https://company.webhook.office.com/webhookb2/..."} />
            </div>
          )}

          {(kind === "servicenow" || kind === "webhook") && (
            <div className="df2-settings-field df2-settings-field--full">
              <label>Endpoint URL</label>
              <input className="df2-input" value={endpointUrl} onChange={(e) => setEndpointUrl(e.target.value)} placeholder={kind === "servicenow" ? "https://company.service-now.com/api/now/table/incident" : "https://company.com/webhook/dataflow"} />
            </div>
          )}

          {kind === "webhook" && (
            <div className="df2-settings-field df2-settings-field--full">
              <label>Headers (optional JSON)</label>
              <textarea className="df2-input" rows={3} value={headers} onChange={(e) => setHeaders(e.target.value)} placeholder={'{ "Authorization": "Bearer token" }'} />
            </div>
          )}

          {kind === "servicenow" && (
            <>
              <div className="df2-settings-field">
                <label>Authentication</label>
                <select className="df2-select" value={authMode} onChange={(e) => setAuthMode(e.target.value as typeof authMode)}>
                  <option value="basic">Username / Password</option>
                  <option value="oauth">OAuth token</option>
                </select>
              </div>
              {authMode === "basic" ? (
                <>
                  <div className="df2-settings-field">
                    <label>Username</label>
                    <input className="df2-input" value={username} onChange={(e) => setUsername(e.target.value)} />
                  </div>
                  <div className="df2-settings-field">
                    <label>Password</label>
                    <input type="password" className="df2-input" value={password} onChange={(e) => setPassword(e.target.value)} />
                  </div>
                </>
              ) : (
                <div className="df2-settings-field df2-settings-field--full">
                  <label>OAuth token</label>
                  <input type="password" className="df2-input" value={token} onChange={(e) => setToken(e.target.value)} />
                </div>
              )}
            </>
          )}

          {kind === "email" && (
            <div className="df2-settings-field df2-settings-field--full df2-settings-field--inline">
              <label className="df2-switch-label" style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
                <input type="checkbox" checked={useCustomSmtp} onChange={(e) => setUseCustomSmtp(e.target.checked)} />
                Use custom SMTP (optional; platform mailer is used otherwise)
              </label>
            </div>
          )}
          </div>

          {kind === "email" && useCustomSmtp && (
            <div className="df2-settings-smtp-block">
              <p className="df2-settings-smtp-label">Custom SMTP server</p>
              <div className="df2-settings-grid df2-settings-grid--smtp">
              <div className="df2-settings-field">
                <label>SMTP host</label>
                <input className="df2-input" value={smtpHost} onChange={(e) => setSmtpHost(e.target.value)} placeholder="smtp.company.com" />
              </div>
              <div className="df2-settings-field">
                <label>SMTP port</label>
                <input className="df2-input" value={smtpPort} onChange={(e) => setSmtpPort(e.target.value)} placeholder="587" />
              </div>
              <div className="df2-settings-field">
                <label>SMTP user</label>
                <input className="df2-input" value={smtpUser} onChange={(e) => setSmtpUser(e.target.value)} />
              </div>
              <div className="df2-settings-field">
                <label>SMTP password</label>
                <input type="password" className="df2-input" value={smtpPassword} onChange={(e) => setSmtpPassword(e.target.value)} />
              </div>
              <div className="df2-settings-field df2-settings-field--full">
                <label>From address</label>
                <input className="df2-input" value={fromAddress} onChange={(e) => setFromAddress(e.target.value)} placeholder="dataflow@company.com" />
              </div>
              </div>
            </div>
          )}

          <div className="df2-settings-channel-form-actions">
            <button type="button" className="df2-btn df2-btn-primary" disabled={!canAdd || saving} onClick={() => void add()}>
              <DtIcon name="plus" size={14} /> {saving ? "Saving…" : "Add channel"}
            </button>
          </div>
        </div>

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
                  <th>Target</th>
                  <th>Status</th>
                  <th style={{ width: 180 }}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {channels.map((c) => (
                  <tr key={c.id}>
                    <td>
                      <strong>{c.label}</strong>
                      <div className="df2-cell-meta">{KIND_META[c.kind as ChannelKind]?.label || c.kind}</div>
                    </td>
                    <td className="df2-cell-meta" style={{ maxWidth: 240, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{summaryFor(c)}</td>
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
