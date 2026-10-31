import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { STUDIO_CANVAS_KEY, STUDIO_SCHEMA_BROWSER_KEY, writeLocalStudioFlag } from "../lib/studioFlags";
import { DtIcon } from "../components/DtIcon";
import { ChangeEmailCard } from "../components/ChangeEmailCard";
import { TwoFactorCard } from "../components/TwoFactorCard";
import { Button } from "../components/ui/Button";
import { EmptyState } from "../components/ui/EmptyState";
import { SectionLoader } from "../components/LoadingState";
import { FilterTabs } from "../components/ui/FilterTabs";
import { PageFrame } from "../components/ui/PageFrame";
import { PageShell } from "../components/ui/PageShell";
import { useToast } from "../components/Toast";
import { useConfirm } from "../components/ui/ConfirmDialog";
import { AuditChainVerification, AuditExportFormat, AuditRetentionPurgeResult, fetchAuditEvents, exportAuditLog, purgeAuditRetention, verifyAuditChain, fetchAiProviderSettings, fetchModelCapabilities, fetchPilotEngineStatus, PilotEngineChoice, PilotEngineStatus, removeAiProviderKey, testAiProviderKey, updatePilotEngine, fetchSsoSettings, fetchSecurityPosture, fetchTenant, downloadSecurityReport, fetchWorkspaceApiKeys, fetchWorkspaceSettings, fetchWorkspaces, purgeJobRetention, ModelCapabilities, createWorkspaceApiKey, resolveApiBase, revokeWorkspaceApiKey, SecurityPosture, SsoConfig, SsoType, testSsoConfig, updateAiProviderSettings, updateSsoConfig, updateWorkspaceSettings, WorkspaceApiKey, WorkspaceApiKeyLifetime, WorkspaceApiKeyRole,
  fetchSignupMetrics,
  type SignupMetrics,
} from "../lib/api";
import { PERMISSIONS, useWriteGate } from "../lib/PermissionsContext";
import { PermissionNotice } from "../components/PermissionNotice";
import { PlatformSettingsNotice } from "../components/PlatformSettingsNotice";
import { type PlatformLock, platformLock, UNLOCKED } from "../lib/platformSettings";
import { NotificationSettings } from "./settings/NotificationSettings";
import { TeamSettings } from "./settings/TeamSettings";
import { TenantSettings } from "./settings/TenantSettings";
import { chainVerdictTitle, chainVerdictToast } from "../lib/auditChain";
import { auditActorLabel } from "../lib/auditActor";
import { buildSsoPatch, missingForEnable } from "../lib/ssoSettings";
import { providerHasTest, providerTestLabel, providerTestToast } from "../lib/aiProviderTest";
import { ipAllowlistRow, mfaRow, sessionTimeoutRow } from "../lib/postureCopy";
import { purgeConfirmCopy, RETENTION_HINT } from "../lib/jobRetention";
import { connectorEgressState } from "../lib/egressPosture";
import { TENANT_SAVED_EVENT, useActiveWorkspaceId } from "../lib/workspace";
import { useVisibleRefresh } from "../lib/visibleRefresh";
import { useTheme } from "../lib/useTheme";
import type { ThemePreference } from "../lib/theme";
import { auditExportFilename, API_KEY_LIFETIME_OPTIONS, filterWorkspaceApiKeys } from "../lib/iamAdmin";
import { ServiceAccountsPanel } from "./settings/ServiceAccountsPanel";
import { ScimSettings } from "./settings/ScimSettings";

const TABS = [
  { id: "general", label: "General", desc: "Workspace defaults", icon: "settings" },
  { id: "security", label: "Security", desc: "Policies & compliance", icon: "shield" },
  { id: "enterprise", label: "Enterprise", desc: "Tenant, BYOK, residency", icon: "shield" },
  { id: "auth", label: "SSO", desc: "Identity providers", icon: "gate" },
  { id: "team", label: "Team", desc: "Members & roles", icon: "connectors" },
  { id: "notifications", label: "Notifications", desc: "Alerts & integrations", icon: "bell" },
  { id: "models", label: "AI Models", desc: "Provider routing", icon: "sparkle" },
  { id: "api", label: "API Keys", desc: "Programmatic access", icon: "zap" },
  { id: "logs", label: "Audit Logs", desc: "Activity trail", icon: "activity" },
] as const;

type TabId = (typeof TABS)[number]["id"];

type AuditLog = {
  id: string;
  time: string;
  actor: string;
  action: string;
  resource: string;
  level: "info" | "success" | "warn" | "error";
};

export function SettingsPage({ onOpenConnectors }: { onOpenConnectors?: () => void }) {
  const { toast } = useToast();
  const { confirm } = useConfirm();
  const { preference: themePreference, setTheme } = useTheme();
  const [tab, setTab] = useState<TabId>("general");
  const [orgName, setOrgName] = useState("Datawrap");
  const [timezone, setTimezone] = useState("UTC");
  const [retention, setRetention] = useState("90");
  const [studioSchemaBrowser, setStudioSchemaBrowser] = useState(false);
  const [studioCanvas, setStudioCanvas] = useState(false);
  const [settingsLoading, setSettingsLoading] = useState(true);
  const [settingsSaving, setSettingsSaving] = useState(false);
  /** Why General could not be read. Empty means the values below are the API's. */
  const [settingsError, setSettingsError] = useState("");
  const [settingsLoaded, setSettingsLoaded] = useState(false);
  const [workspaceLabel, setWorkspaceLabel] = useState("");
  const [logFilter, setLogFilter] = useState<"all" | AuditLog["level"]>("all");
  const [auditEvents, setAuditEvents] = useState<AuditLog[]>([]);
  const [auditLoading, setAuditLoading] = useState(false);
  const [auditExportFormat, setAuditExportFormat] = useState<AuditExportFormat>("csv");
  /** Last chain verification result. Null means nobody has asked yet — which is not "intact". */
  const [chainReport, setChainReport] = useState<AuditChainVerification | null>(null);
  const [chainVerifying, setChainVerifying] = useState(false);
  const [retentionResult, setRetentionResult] = useState<AuditRetentionPurgeResult | null>(null);
  const [retentionLoading, setRetentionLoading] = useState(false);
  const [retentionError, setRetentionError] = useState("");
  const [modelCapabilities, setModelCapabilities] = useState<ModelCapabilities | null>(null);
  const [modelCapabilitiesLoaded, setModelCapabilitiesLoaded] = useState(false);
  const [modelError, setModelError] = useState("");
  const [engineError, setEngineError] = useState("");
  const [ssoConfigs, setSsoConfigs] = useState<Record<SsoType, SsoConfig> | null>(null);
  /** Why SSO state is unknown. "Not configured" must not stand in for "not readable". */
  const [ssoError, setSsoError] = useState("");
  const [ssoEditor, setSsoEditor] = useState<SsoType | null>(null);
  const [ssoDraft, setSsoDraft] = useState<SsoConfig | null>(null);
  const [ssoSaving, setSsoSaving] = useState(false);
  const [ssoDomainsText, setSsoDomainsText] = useState("");
  const [ssoClearSecret, setSsoClearSecret] = useState(false);
  const [aiEditor, setAiEditor] = useState<string | null>(null);
  const [aiDraft, setAiDraft] = useState({ api_key: "", model: "", base_url: "", enabled: true });
  const [aiSaving, setAiSaving] = useState(false);
  const [aiTesting, setAiTesting] = useState<string | null>(null);
  const [aiRemoving, setAiRemoving] = useState<string | null>(null);
  const [engineStatus, setEngineStatus] = useState<PilotEngineStatus | null>(null);
  // General, SSO and AI providers are each one document for the whole
  // deployment: only a platform admin changes them; a workspace admin reads.
  const [generalLock, setGeneralLock] = useState<PlatformLock>(UNLOCKED);
  const [ssoLock, setSsoLock] = useState<PlatformLock>(UNLOCKED);
  const [aiLock, setAiLock] = useState<PlatformLock>(UNLOCKED);
  const [engineSaving, setEngineSaving] = useState(false);
  const [apiKeys, setApiKeys] = useState<WorkspaceApiKey[]>([]);
  const [apiKeysLoading, setApiKeysLoading] = useState(false);
  const [apiKeysError, setApiKeysError] = useState("");
  const [apiKeyGenerating, setApiKeyGenerating] = useState(false);
  const [revokingKeyId, setRevokingKeyId] = useState<string | null>(null);
  const [newKeyName, setNewKeyName] = useState("Production key");
  const [newKeyRole, setNewKeyRole] = useState<WorkspaceApiKeyRole>("editor");
  const [newKeyLifetime, setNewKeyLifetime] = useState<WorkspaceApiKeyLifetime>("90d");
  const [revealedKey, setRevealedKey] = useState<string | null>(null);
  const [posture, setPosture] = useState<SecurityPosture | null>(null);
  const [signupMetrics, setSignupMetrics] = useState<SignupMetrics | null>(null);
  const [postureLoading, setPostureLoading] = useState(false);
  /** undefined = not read yet; null = this workspace has no tenant. */
  const [egressTenant, setEgressTenant] = useState<{ workspace_id?: string; egress_allowlist?: string[] } | null | undefined>(undefined);
  const postureRequest = useRef(0);
  const workspaceManage = useWriteGate(PERMISSIONS.workspaceManage);
  // The key store is deployment-wide, so keys are IAM administration (iam.manage).
  const iamManage = useWriteGate(PERMISSIONS.iamManage);
  // Retention purges the platform-wide trail, so it is a platform admin's (D220).
  const [platformAdmin, setPlatformAdmin] = useState(false);
  const retentionAllowed = workspaceManage.allowed && platformAdmin;
  const retentionReason = !workspaceManage.allowed
    ? workspaceManage.reason
    : "Audit retention acts on the platform-wide trail: platform administrators only.";
  const retentionPurgeDisabled = !retentionAllowed
    || retentionLoading
    || !retentionResult?.dry_run
    || retentionResult.legal_hold
    || retentionResult.retention_days === null
    || (retentionResult.would_remove ?? 0) === 0;
  /** Only edit what was actually read: an unread value must not be re-saved. */
  const generalEditable = workspaceManage.allowed && settingsLoaded && !generalLock.locked;
  const generalTitle = (generalLock.locked ? generalLock.reason : workspaceManage.reason) || undefined;
  const activeWorkspace = useActiveWorkspaceId();
  const generalDirtyRef = useRef(false);

  const loadPosture = useCallback(() => {
    const scope = activeWorkspace;
    const requestId = ++postureRequest.current;
    if (!scope) {
      setPostureLoading(false);
      setEgressTenant(undefined);
      setPosture(null);
      return;
    }
    setPostureLoading(true);
    Promise.all([
      fetchSecurityPosture(scope),
      fetchTenant(scope),
    ])
      .then(([nextPosture, tenant]) => {
        if (postureRequest.current !== requestId) return;
        setPosture(nextPosture);
        setEgressTenant(tenant);
      })
      .catch(() => {
        if (postureRequest.current !== requestId) return;
        setPosture(null);
        setEgressTenant(undefined);
      })
      .finally(() => {
        if (postureRequest.current !== requestId) return;
        setPostureLoading(false);
      });
  }, [activeWorkspace]);

  const loadGeneral = useCallback((force = false) => {
    if (!force && generalDirtyRef.current) return;
    setSettingsLoading(true);
    fetchWorkspaceSettings()
      .then((ws) => {
        if (!force && generalDirtyRef.current) return;
        setOrgName(ws.org_name);
        setTimezone(ws.timezone);
        setRetention(String(ws.retention_days));
        setStudioSchemaBrowser(ws.studio_schema_browser === true);
        setStudioCanvas(ws.studio_canvas === true);
        setGeneralLock(platformLock(ws));
        setSettingsLoaded(true);
        setSettingsError("");
        generalDirtyRef.current = false;
      })
      .catch((err: unknown) => {
        // A refusal or an outage is shown as itself. This used to swallow the
        // error and display "Datawrap / UTC / 90", i.e. settings nobody saved.
        setSettingsLoaded(false);
        setSettingsError(err instanceof Error ? err.message : "Could not load workspace settings.");
      })
      .finally(() => setSettingsLoading(false));
    fetchWorkspaces()
      .then(({ workspaces: rows, platformAdmin: isPlatformAdmin }) => {
        const current = rows.find((w) => w.id === activeWorkspace);
        setWorkspaceLabel(current?.name || "");
        setPlatformAdmin(isPlatformAdmin);
      })
      .catch(() => {
        setWorkspaceLabel("");
        setPlatformAdmin(false);
      });
  }, [activeWorkspace]);

  const loadModelsAndSso = useCallback(() => {
    fetchModelCapabilities()
      .then((caps) => {
        setModelCapabilities(caps);
        setModelError("");
      })
      .catch((err: unknown) => {
        // A refusal or an outage is not "the local engine is active": inventing
        // a provider list here told admins the wrong routing.
        setModelCapabilities(null);
        setModelError(err instanceof Error ? err.message : "Could not read AI routing configuration.");
      })
      .finally(() => setModelCapabilitiesLoaded(true));
    fetchPilotEngineStatus()
      .then((status) => {
        setEngineStatus(status);
        setAiLock(platformLock(status));
        setEngineError("");
      })
      .catch((err: unknown) => {
        setEngineStatus(null);
        setEngineError(err instanceof Error ? err.message : "Could not read the Pilot engine setting.");
      });
    fetchSsoSettings()
      .then((sso) => {
        setSsoConfigs(sso.providers);
        setSsoLock(platformLock(sso));
        setSsoError("");
      })
      .catch((err: unknown) => {
        setSsoConfigs(null);
        setSsoError(err instanceof Error ? err.message : "Could not read identity provider settings.");
      });
  }, []);

  const loadApiKeys = useCallback(() => {
    setApiKeysLoading(true);
    fetchWorkspaceApiKeys()
      .then((keys) => {
        setApiKeys(filterWorkspaceApiKeys(keys));
        setApiKeysError("");
      })
      .catch((err: unknown) => {
        // An empty list means "no keys". A refusal means "you may not look".
        setApiKeys([]);
        setApiKeysError(err instanceof Error ? err.message : "Could not list API keys.");
      })
      .finally(() => setApiKeysLoading(false));
  }, []);

  const loadAudit = useCallback(() => {
    setAuditLoading(true);
    fetchAuditEvents(100, logFilter === "all" ? undefined : logFilter)
      .then((events) =>
        setAuditEvents(
          events.map((ev) => ({
            id: ev.id,
            time: new Date(ev.time).toLocaleString(),
            actor: auditActorLabel(ev),
            action: ev.action,
            resource: ev.resource,
            level: (ev.level === "success" ? "success" : ev.level === "warn" ? "warn" : ev.level === "error" ? "error" : "info") as AuditLog["level"],
          })),
        ),
      )
      .catch(() => setAuditEvents([]))
      .finally(() => setAuditLoading(false));
  }, [logFilter]);

  useEffect(() => {
    generalDirtyRef.current = false;
    loadGeneral(true);
    loadModelsAndSso();
  }, [activeWorkspace, loadGeneral, loadModelsAndSso]);

  useEffect(() => {
    if (tab === "security") { loadPosture(); fetchSignupMetrics().then(setSignupMetrics).catch(() => setSignupMetrics(null)); }
  }, [tab, activeWorkspace, loadPosture]);

  useEffect(() => {
    const onTenantSaved = () => loadPosture();
    window.addEventListener(TENANT_SAVED_EVENT, onTenantSaved);
    return () => window.removeEventListener(TENANT_SAVED_EVENT, onTenantSaved);
  }, [loadPosture]);

  useEffect(() => {
    if (tab !== "api") return;
    loadApiKeys();
  }, [tab, activeWorkspace, loadApiKeys]);

  useEffect(() => {
    if (tab !== "logs") return;
    loadAudit();
  }, [tab, activeWorkspace, loadAudit]);

  useVisibleRefresh(() => loadGeneral(false), 15_000, tab === "general");
  useVisibleRefresh(loadPosture, 20_000, tab === "security");
  useVisibleRefresh(loadApiKeys, 15_000, tab === "api");
  useVisibleRefresh(loadAudit, 10_000, tab === "logs");
  useVisibleRefresh(loadModelsAndSso, 20_000, tab === "models" || tab === "auth");

  const openSsoEditor = (type: SsoType) => {
    const cfg = ssoConfigs?.[type];
    const defaultRedirect = `${resolveApiBase()}/auth/sso/${type}/callback`;
    setSsoDraft({
      enabled: cfg?.enabled ?? false,
      entity_id: cfg?.entity_id ?? "",
      sso_url: cfg?.sso_url ?? "",
      x509_cert: "",
      email_attribute: cfg?.email_attribute ?? "email",
      issuer: cfg?.issuer ?? "",
      client_id: cfg?.client_id ?? "",
      client_secret: "",
      redirect_uri: cfg?.redirect_uri || defaultRedirect,
      scopes: cfg?.scopes ?? "openid email profile",
      tenant_id: cfg?.tenant_id ?? "",
    });
    setSsoDomainsText((cfg?.email_domains ?? []).join(", "));
    setSsoClearSecret(false);
    setSsoEditor(type);
  };

  /**
   * Workspace administration is the gate for every Settings mutation.
   * Refuse in words rather than firing a request the API will reject.
   */
  const mayAdminister = () => {
    if (workspaceManage.allowed) return true;
    toast({ title: "No write permission", message: workspaceManage.reason, tone: "warning" });
    return false;
  };

  /** Deployment-wide settings: workspace administration is not enough. */
  const mayChangePlatform = (lock: PlatformLock) => {
    if (!mayAdminister()) return false;
    if (!lock.locked) return true;
    toast({ title: "Read-only", message: lock.reason, tone: "warning" });
    return false;
  };

  const [retentionBusy, setRetentionBusy] = useState(false);
  const runRetentionPurge = async () => {
    if (!mayAdminister()) return;
    setRetentionBusy(true);
    try {
      const dry = await purgeJobRetention({ dry_run: true });
      const copy = purgeConfirmCopy({ ...dry, would_remove: dry.would_remove ?? 0 });
      if (copy.nothing) {
        toast({ title: copy.title, message: copy.message, tone: "success" });
        return;
      }
      const ok = await confirm({ title: copy.title, message: copy.message, confirmLabel: "Delete jobs", cancelLabel: "Keep them", tone: "danger" });
      if (!ok) return;
      const done = await purgeJobRetention({ dry_run: false, confirm_token: dry.confirm_token });
      toast({ title: "Old jobs purged", message: `${done.removed ?? 0} finished jobs deleted.`, tone: "success" });
    } catch (err) {
      toast({ title: "Purge failed", message: err instanceof Error ? err.message : "", tone: "error" });
    } finally {
      setRetentionBusy(false);
    }
  };

  const saveSsoConfig = async () => {
    if (!ssoEditor || !ssoDraft) return;
    if (!mayChangePlatform(ssoLock)) return;
    if (ssoDraft.enabled) {
      const missing = missingForEnable(ssoEditor, ssoDraft, ssoConfigs?.[ssoEditor], ssoClearSecret);
      if (missing.length) {
        toast({
          title: "Provider can't be enabled yet",
          message: `Fill in ${missing.join(", ")}, or turn the provider off to save a draft.`,
          tone: "warning",
        });
        return;
      }
    }
    setSsoSaving(true);
    try {
      const result = await updateSsoConfig(ssoEditor, buildSsoPatch(ssoDraft, ssoDomainsText, ssoClearSecret));
      setSsoConfigs((prev) => ({ ...(prev ?? {} as Record<SsoType, SsoConfig>), [ssoEditor]: result.config }));
      toast({
        title: result.validation.ok ? "SSO configured" : "SSO saved — needs attention",
        message: result.validation.message,
        tone: result.validation.ok ? "success" : "warning",
      });
      setSsoEditor(null);
    } catch (err) {
      toast({
        title: "SSO save failed",
        message: err instanceof Error ? err.message : "Could not save SSO settings.",
        tone: "error",
      });
    } finally {
      setSsoSaving(false);
    }
  };

  const openAiEditor = async (provider: string, defaultModel: string) => {
    if (!mayAdminister()) return;
    try {
      const settings = await fetchAiProviderSettings();
      const cfg = settings[provider];
      setAiDraft({
        api_key: "",
        model: cfg?.model ?? defaultModel,
        base_url: cfg?.base_url ?? "http://localhost:11434",
        enabled: cfg?.enabled ?? true,
      });
      setAiEditor(provider);
    } catch {
      setAiDraft({ api_key: "", model: defaultModel, base_url: "http://localhost:11434", enabled: true });
      setAiEditor(provider);
    }
  };

  const saveAiProvider = async () => {
    if (!aiEditor) return;
    if (!mayChangePlatform(aiLock)) return;
    setAiSaving(true);
    try {
      await updateAiProviderSettings(aiEditor, aiDraft);
      const [caps, engine] = await Promise.all([fetchModelCapabilities(), fetchPilotEngineStatus()]);
      setModelCapabilities(caps);
      setEngineStatus(engine);
      toast({
        title: "AI provider updated",
        message:
          engine.engine === "local"
            ? `${aiEditor} saved. Pilot is still on the local engine — ${engine.reason}`
            : `${aiEditor} saved. Pilot now answers through it (${engine.engine}).`,
        tone: "success",
      });
      setAiEditor(null);
    } catch (err) {
      toast({
        title: "Save failed",
        message: err instanceof Error ? err.message : "Could not save AI provider settings.",
        tone: "error",
      });
    } finally {
      setAiSaving(false);
    }
  };

  const testProviderKey = async (provider: string) => {
    if (!mayAdminister()) return;
    setAiTesting(provider);
    try {
      const result = await testAiProviderKey(provider);
      setModelCapabilities(result.capabilities);
      setEngineStatus(await fetchPilotEngineStatus());
      toast({ ...providerTestToast(provider, result), tone: result.ok ? "success" : "error" });
    } catch (err) {
      toast({
        title: "Key test failed",
        message: err instanceof Error ? err.message : "Could not test the saved key.",
        tone: "error",
      });
    } finally {
      setAiTesting(null);
    }
  };

  const removeProviderKey = async (provider: string, label: string) => {
    if (aiRemoving || !mayChangePlatform(aiLock)) return;
    const ok = await confirm({
      title: `Remove the ${label} key?`,
      message: "Pilot goes back to our local engine until you save a key again.",
      confirmLabel: "Remove key",
      cancelLabel: "Keep key",
      tone: "danger",
    });
    if (!ok) return;
    setAiRemoving(provider);
    try {
      await removeAiProviderKey(provider);
      const [caps, engine] = await Promise.all([fetchModelCapabilities(), fetchPilotEngineStatus()]);
      setModelCapabilities(caps);
      setEngineStatus(engine);
      toast({ title: `${label} key removed`, message: "Pilot now answers on our local engine.", tone: "success" });
    } catch (err) {
      toast({
        title: "Could not remove key",
        message: err instanceof Error ? err.message : "The saved key was not removed.",
        tone: "error",
      });
    } finally {
      setAiRemoving(null);
    }
  };

  const saveEngineChoice = async (engine: PilotEngineChoice) => {
    if (!mayChangePlatform(aiLock)) return;
    setEngineSaving(true);
    try {
      const status = await updatePilotEngine(engine);
      setEngineStatus(status);
      setModelCapabilities(await fetchModelCapabilities());
      toast({ title: "Pilot engine updated", message: status.reason, tone: "success" });
    } catch (err) {
      toast({
        title: "Could not change engine",
        message: err instanceof Error ? err.message : "Pilot engine setting was not saved.",
        tone: "error",
      });
    } finally {
      setEngineSaving(false);
    }
  };

  const generateApiKey = async () => {
    if (apiKeyGenerating) return;
    if (!mayAdminister()) return;
    setApiKeyGenerating(true);
    try {
      const created = await createWorkspaceApiKey(
        newKeyName.trim() || "API key",
        newKeyRole,
        newKeyLifetime,
      );
      const optimistic: WorkspaceApiKey = {
        id: created.id,
        name: created.name,
        prefix: created.prefix,
        role: created.role || newKeyRole,
        created_at: created.created_at,
        last_used_at: null,
        expires_at: created.expires_at,
        lifetime: created.lifetime || newKeyLifetime,
        expired: false,
        kind: "api_key",
      };
      setApiKeys((prev) => [optimistic, ...prev.filter((k) => k.id !== created.id)]);
      setRevealedKey(created.key);
      toast({ title: "API key created", message: "Copy it now — it won't be shown again.", tone: "success" });
      fetchWorkspaceApiKeys().then((keys) => setApiKeys(filterWorkspaceApiKeys(keys))).catch(() => {});
    } catch (err) {
      toast({
        title: "Generation failed",
        message: err instanceof Error ? err.message : "Could not create API key.",
        tone: "error",
      });
    } finally {
      setApiKeyGenerating(false);
    }
  };

  const revokeKey = async (keyId: string, keyName: string) => {
    if (revokingKeyId) return;
    if (!mayAdminister()) return;
    const ok = await confirm({
      title: `Revoke “${keyName}”?`,
      message: "Applications using this key will stop working immediately.",
      confirmLabel: "Revoke key",
      cancelLabel: "Keep key",
      tone: "danger",
    });
    if (!ok) return;
    setRevokingKeyId(keyId);
    try {
      await revokeWorkspaceApiKey(keyId);
      setApiKeys((prev) => prev.filter((k) => k.id !== keyId));
      if (revealedKey) setRevealedKey(null);
      toast({ title: "API key revoked", tone: "info" });
    } catch (err) {
      toast({
        title: "Revoke failed",
        message: err instanceof Error ? err.message : "Could not revoke API key.",
        tone: "error",
      });
    } finally {
      setRevokingKeyId(null);
    }
  };

  const saveWorkspaceSettings = async () => {
    // Never write back a value that was never read, and never pretend a
    // refusal was a save.
    if (!mayChangePlatform(generalLock)) return;
    if (!settingsLoaded) {
      toast({
        title: "Settings not loaded",
        message: "These fields are not your saved values. Reload before saving.",
        tone: "error",
      });
      return;
    }
    setSettingsSaving(true);
    try {
      const ws = await updateWorkspaceSettings({
        org_name: orgName,
        timezone,
        retention_days: Number(retention) || 90,
        studio_schema_browser: studioSchemaBrowser,
        studio_canvas: studioCanvas,
      });
      setOrgName(ws.org_name);
      setTimezone(ws.timezone);
      setRetention(String(ws.retention_days));
      setStudioSchemaBrowser(ws.studio_schema_browser === true);
      setStudioCanvas(ws.studio_canvas === true);
      try {
        writeLocalStudioFlag(STUDIO_SCHEMA_BROWSER_KEY, ws.studio_schema_browser === true, localStorage);
        writeLocalStudioFlag(STUDIO_CANVAS_KEY, ws.studio_canvas === true, localStorage);
      } catch {
        // A browser that blocks storage still saved the workspace gate.
      }
      generalDirtyRef.current = false;
      toast({ title: "Settings saved", message: "Organization preferences updated.", tone: "success" });
    } catch (err) {
      toast({
        title: "Save failed",
        message: err instanceof Error ? err.message : "Could not persist workspace settings.",
        tone: "error",
      });
    } finally {
      setSettingsSaving(false);
    }
  };

  const filteredLogs = useMemo(
    () => auditEvents,
    [auditEvents],
  );

  const runChainVerification = async () => {
    setChainVerifying(true);
    try {
      const report = await verifyAuditChain();
      setChainReport(report);
      toast({
        title: report.verified ? "Chain verified" : "Chain verification failed",
        message: chainVerdictToast(report),
        tone: report.verified ? "success" : "error",
      });
    } catch (err) {
      setChainReport(null);
      toast({
        title: "Verification failed",
        message: err instanceof Error ? err.message : "Could not verify the audit chain",
        tone: "error",
      });
    } finally {
      setChainVerifying(false);
    }
  };

  const downloadAuditExport = async (format: AuditExportFormat) => {
    try {
      const blob = await exportAuditLog(format);
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = auditExportFilename(format);
      a.click();
      URL.revokeObjectURL(url);
      toast({
        title: "Audit export ready",
        message: `Workspace-scoped ${format.toUpperCase()} — not a SOC 2 letter.`,
        tone: "success",
      });
    } catch (err) {
      toast({
        title: "Export failed",
        message: err instanceof Error ? err.message : "Could not export audit log",
        tone: "error",
      });
    }
  };

  const previewAuditRetention = async () => {
    if (!retentionAllowed) return;
    setRetentionLoading(true);
    setRetentionError("");
    setRetentionResult(null);
    try {
      setRetentionResult(await purgeAuditRetention(true));
    } catch (err) {
      setRetentionError(err instanceof Error ? err.message : "Could not preview audit retention.");
    } finally {
      setRetentionLoading(false);
    }
  };

  const purgeAuditRetentionNow = async () => {
    if (!retentionAllowed) return;
    if (!await confirm({
      title: "Purge expired audit events?",
      message: "This permanently removes events outside the configured retention window. A retention checkpoint will be recorded.",
      confirmLabel: "Purge events",
      tone: "danger",
    })) return;
    setRetentionLoading(true);
    setRetentionError("");
    setRetentionResult(null);
    try {
      setRetentionResult(await purgeAuditRetention(false));
      await runChainVerification();
    } catch (err) {
      setRetentionError(err instanceof Error ? err.message : "Could not purge audit retention.");
    } finally {
      setRetentionLoading(false);
    }
  };

  const egress = connectorEgressState(activeWorkspace, egressTenant, posture);
  const egressCopy = egress === "on"
    ? "Admin CIDRs may dial on-prem or VPC hosts. Metadata stays refused."
    : egress === "off"
      ? "Off. Private ranges, loopback, and cloud metadata are refused."
      : "Reading this workspace's connector egress.";

  return (
    <PageShell
      wide
      className="df2-page-settings"
      title="Settings"
      description="Workspace security, SSO, team access, and audit controls."
    >
      <PageFrame className="df2-settings-workspace">
        <div className="df2-settings-layout">
          <nav className="df2-settings-nav" role="tablist" aria-label="Settings sections">
            {TABS.map((t) => (
              <button
                key={t.id}
                type="button"
                role="tab"
                aria-selected={tab === t.id}
                className={`df2-settings-nav-item${tab === t.id ? " active" : ""}`}
                onClick={() => setTab(t.id)}
              >
                <span className="df2-settings-nav-icon" aria-hidden>
                  <DtIcon name={t.icon} size={18} />
                </span>
                <span className="df2-settings-nav-text">
                  <span className="df2-settings-nav-label">{t.label}</span>
                  <span className="df2-settings-nav-desc">{t.desc}</span>
                </span>
              </button>
            ))}
          </nav>

          <div className="df2-settings-panel" role="tabpanel" aria-label={TABS.find((t) => t.id === tab)?.label ?? "Settings"}>
            {tab === "general" && (
              <>
                <section className="df2-settings-section" data-testid="settings-appearance">
                  <div className="df2-settings-section-head">
                    <div>
                      <h2>Appearance</h2>
                      <p>Light and dark share one token set. This is this browser — not a workspace admin setting.</p>
                    </div>
                  </div>
                  <div className="df2-settings-section-body">
                    <div className="df2-settings-field">
                      <label id="appearance-theme-label">Theme</label>
                      <FilterTabs<ThemePreference>
                        ariaLabel="Theme"
                        value={themePreference}
                        onChange={setTheme}
                        items={[
                          { id: "light", label: "Light", icon: <DtIcon name="sun" size={13} /> },
                          { id: "dark", label: "Dark", icon: <DtIcon name="moon" size={13} /> },
                          { id: "system", label: "System" },
                        ]}
                      />
                      <p className="df2-label-hint">
                        Teal brand stays. System follows the operating-system color scheme.
                      </p>
                    </div>
                  </div>
                </section>
                <section className="df2-settings-section">
                  <div className="df2-settings-section-head">
                    <div>
                      <h2>Organization profile</h2>
                      <p>Defaults applied across Transfer Studio, jobs, and connectors. Switching workspace in the top bar reloads this profile immediately.</p>
                    </div>
                  </div>
                  <PermissionNotice
                    allowed={workspaceManage.allowed}
                    reason={workspaceManage.reason}
                    what="Workspace settings are read-only for you."
                  />
                  {workspaceManage.allowed && (
                    <PlatformSettingsNotice lock={generalLock} what="The organization profile applies to every workspace." />
                  )}
                  {settingsError && (
                    <div className="df2-permission-notice" role="alert" data-testid="settings-load-error">
                      <DtIcon name="alert" size={16} />
                      <p>
                        <strong>Workspace settings could not be read. </strong>
                        {settingsError} The fields below are not your saved values.
                      </p>
                    </div>
                  )}
                  <div className="df2-settings-section-body">
                    <div className="df2-settings-grid df2-settings-grid--row">
                      <div className="df2-settings-field">
                        <label htmlFor="workspace-name">This workspace</label>
                        <input
                          id="workspace-name"
                          className="df2-input"
                          value={workspaceLabel || (activeWorkspace ? "Unnamed workspace" : "No workspace selected")}
                          disabled
                          readOnly
                          title="The Team workspace selected in the top bar. This is not the organization profile name."
                        />
                        <p className="df2-label-hint">
                          Top-bar workspace name (Team). Rename it under Team — not here.
                        </p>
                      </div>
                      <div className="df2-settings-field">
                        <label htmlFor="org-name">Organization name</label>
                        <input
                          id="org-name"
                          className="df2-input"
                          value={orgName}
                          disabled={!generalEditable}
                          title={generalTitle}
                          onChange={(e) => {
                            generalDirtyRef.current = true;
                            setOrgName(e.target.value);
                          }}
                        />
                        <p className="df2-label-hint">
                          Shown on certificates and exports. This is not the Team workspace name in the top bar.
                        </p>
                      </div>
                      <div className="df2-settings-field">
                        <label htmlFor="timezone">Default timezone</label>
                        <select
                          id="timezone"
                          className="df2-select"
                          value={timezone}
                          disabled={!generalEditable}
                          title={generalTitle}
                          onChange={(e) => {
                            generalDirtyRef.current = true;
                            setTimezone(e.target.value);
                          }}
                        >
                          <option value="UTC">UTC</option>
                          <option value="America/New_York">Eastern Time</option>
                          <option value="America/Los_Angeles">Pacific Time</option>
                          <option value="Europe/London">London</option>
                        </select>
                      </div>
                      <div className="df2-settings-field">
                        <label htmlFor="retention">Job retention (days)</label>
                        <input
                          id="retention"
                          className="df2-input"
                          type="number"
                          value={retention}
                          disabled={!generalEditable}
                          title={generalTitle}
                          onChange={(e) => {
                            generalDirtyRef.current = true;
                            setRetention(e.target.value);
                          }}
                        />
                      </div>
                      <div className="df2-settings-field">
                        <label htmlFor="studio-schema-browser">Schema browser</label>
                        <input
                          id="studio-schema-browser"
                          type="checkbox"
                          checked={studioSchemaBrowser}
                          disabled={!generalEditable}
                          title={generalTitle}
                          onChange={(e) => {
                            generalDirtyRef.current = true;
                            setStudioSchemaBrowser(e.target.checked);
                          }}
                        />
                        <p className="df2-label-hint">
                          Workspace gate for the database tree beside the source table. Saving also opts in this browser. Other browsers stay on the wizard until they save here.
                        </p>
                      </div>
                      <div className="df2-settings-field">
                        <label htmlFor="studio-canvas">Recipe canvas</label>
                        <input
                          id="studio-canvas"
                          type="checkbox"
                          checked={studioCanvas}
                          disabled={!generalEditable}
                          title={generalTitle}
                          onChange={(e) => {
                            generalDirtyRef.current = true;
                            setStudioCanvas(e.target.checked);
                          }}
                        />
                        <p className="df2-label-hint">
                          Workspace gate for the ordered transform canvas. Off keeps the step list. The logo and the rest of the wizard stay as they are.
                        </p>
                      </div>
                      <div className="df2-settings-field">
                        <label>Default destination</label>
                        {onOpenConnectors ? (
                          <button type="button" className="df2-btn df2-btn-ghost" onClick={onOpenConnectors}>
                            Manage on Connectors
                          </button>
                        ) : (
                          <p className="df2-settings-hint">Set the write target on each connector — not a workspace-wide default.</p>
                        )}
                      </div>
                    </div>
                    <p className="df2-settings-hint">{RETENTION_HINT} Write targets stay on each connector.</p>
                    {workspaceManage.allowed && settingsLoaded && (
                      <button type="button" className="df2-btn df2-btn-ghost" data-testid="job-retention-purge" disabled={retentionBusy} onClick={() => void runRetentionPurge()}>
                        {retentionBusy ? "Checking…" : "Purge old jobs…"}
                      </button>
                    )}
                  </div>
                  {!generalLock.locked && (
                    <div className="df2-settings-section-footer">
                      <Button
                        variant="primary"
                        data-testid="settings-general-save"
                        disabled={settingsLoading || settingsSaving || !generalEditable}
                        title={generalTitle}
                        loading={settingsSaving}
                        loadingLabel="Saving…"
                        onClick={() => void saveWorkspaceSettings()}
                      >
                        Save changes
                      </Button>
                    </div>
                  )}
                </section>
              </>
            )}

            {tab === "security" && (
              <section className="df2-settings-section">
                <div className="df2-settings-section-head">
                  <div>
                    <h2>Security & compliance</h2>
                    <p>Posture report from the control plane — badges are observed state, not toggles.</p>
                  </div>
                  <span className={`df2-badge ${posture?.environment === "production" ? "df2-badge-live" : "df2-badge-muted"}`}>
                    {posture?.environment === "production" ? "Production" : "Development"}
                  </span>
                </div>
                <div className="df2-settings-section-body">
                  <TwoFactorCard />
                  {postureLoading && <p className="df2-cell-meta">Loading security posture…</p>}
                  {!postureLoading && posture && (
                    <>
                      <h3 className="df2-settings-subhead">Posture</h3>
                      <div className="df2-settings-policy-grid" style={{ marginBottom: 24 }}>
                        {[
                          { title: "Encryption at rest", desc: "AES-256 for stored connector credentials and job artifacts.", on: posture.encryption_at_rest, pending: false },
                          { title: "Audit logging", desc: "Immutable trail for transfers, configuration, and API access.", on: posture.audit_logging, pending: false },
                          { title: "PII detection", desc: "Sensitive column tagging at ingest and mapping review.", on: posture.pii_detection, pending: false },
                          ipAllowlistRow(posture),
                          { title: "Connector egress", desc: egressCopy, on: egress === "on", pending: egress === "unknown" },
                          sessionTimeoutRow(posture),
                          mfaRow(posture),
                        ].map((item) => (
                          <div key={item.title} className="df2-settings-policy-row" data-testid={item.title === "Connector egress" ? "connector-egress-row" : undefined}>
                            <div>
                              <h3>{item.title}</h3>
                              <p>{item.desc}</p>
                            </div>
                            <span className={`df2-badge ${item.on ? "df2-badge-live" : "df2-badge-muted"}`} data-testid={item.title === "Connector egress" ? "connector-egress-badge" : undefined}>
                              {item.pending ? "Reading" : item.on ? "Enabled" : "Disabled"}
                            </span>
                          </div>
                        ))}
                      </div>

                      {signupMetrics && (
                        <>
                          <h3 className="df2-settings-subhead df2-settings-subhead--spaced">Signup & recovery</h3>
                          <div className="df2-settings-policy-grid" data-testid="signup-metrics-card">
                            {[
                              { title: "Pending signups", value: String(signupMetrics.pending_signups), desc: "Verified-pending accounts awaiting a code" },
                              { title: "Live OTP challenges", value: String(signupMetrics.active_challenges), desc: "Unexpired, unconsumed codes" },
                              { title: "Email sent today", value: `${signupMetrics.email_quota.day_used} / ${signupMetrics.email_quota.day_limit}`, desc: `Send-quota backend: ${signupMetrics.email_quota.backend}` },
                            ].map((row) => (
                              <div key={row.title} className="df2-settings-policy-row">
                                <div>
                                  <h3>{row.title}</h3>
                                  <p>{row.desc}</p>
                                </div>
                                <span className="df2-badge df2-badge-live">{row.value}</span>
                              </div>
                            ))}
                          </div>
                        </>
                      )}

                      <ChangeEmailCard />

                      <h3 className="df2-settings-subhead df2-settings-subhead--spaced">Compliance roadmap</h3>
                      <div className="df2-settings-policy-grid" style={{ marginBottom: 24 }}>
                        {posture.compliance.map((c) => (
                          <div key={c.framework} className="df2-settings-policy-row">
                            <div>
                              <h3>{c.framework}</h3>
                              <p>{c.evidence}</p>
                            </div>
                            <span className={`df2-badge ${c.status === "ready" ? "df2-badge-live" : c.status === "in_progress" ? "df2-badge-warn" : "df2-badge-muted"}`}>
                              {c.status === "ready" ? "Ready" : c.status === "in_progress" ? "In progress" : "Available"}
                            </span>
                          </div>
                        ))}
                      </div>

                      <h3 className="df2-settings-subhead df2-settings-subhead--spaced">BYOK / key management</h3>
                      <div className="df2-settings-policy-grid">
                        <div className="df2-settings-policy-row">
                          <div>
                            <h3>Bring-your-own-key</h3>
                            <p>Customer-managed encryption keys for tenant data.</p>
                          </div>
                          <span className={`df2-badge ${posture.byok.configured ? "df2-badge-live" : "df2-badge-muted"}`}>
                            {posture.byok.configured ? `Active (${posture.byok.active_count} key${posture.byok.active_count === 1 ? "" : "s"})` : "Not configured"}
                          </span>
                        </div>
                        <div className="df2-settings-policy-row">
                          <div>
                            <h3>Data region</h3>
                            <p>Primary region for job data and connector credentials.</p>
                          </div>
                          <span className="df2-badge df2-badge-live">{posture.data_region}</span>
                        </div>
                        <div className="df2-settings-policy-row">
                          <div>
                            <h3>TLS</h3>
                            <p>Minimum TLS version for API and MCP traffic.</p>
                          </div>
                          <span className="df2-badge df2-badge-live">{posture.tls_version}</span>
                        </div>
                      </div>
                    </>
                  )}
                  {!postureLoading && posture && (
                    <div className="df2-settings-section-footer">
                      <button
                        type="button"
                        className="df2-btn df2-btn-secondary"
                        onClick={() => void downloadSecurityReport().then((blob) => {
                          const url = URL.createObjectURL(blob);
                          const a = document.createElement("a");
                          a.href = url;
                          a.download = "datawrap-compliance-report.md";
                          a.click();
                          URL.revokeObjectURL(url);
                        }).catch((err) => toast({ title: "Download failed", message: err instanceof Error ? err.message : "Could not download report", tone: "error" }))}
                      >
                        <DtIcon name="download" size={14} /> Download compliance report
                      </button>
                    </div>
                  )}
                  {!postureLoading && !posture && (
                    <>
                      <EmptyState
                        compact
                        icon="shield"
                        title="Security posture unavailable"
                        description="Start the API to load live compliance status. Policy controls below reflect your workspace defaults."
                      />
                      <h3 className="df2-settings-subhead df2-settings-subhead--spaced">Policy controls</h3>
                      <div className="df2-settings-policy-grid">
                        {[
                          { title: "Encryption at rest", desc: "AES-256 for stored connector credentials and job artifacts." },
                          { title: "Audit logging", desc: "Immutable trail for transfers, configuration, and API access." },
                          { title: "PII detection", desc: "Sensitive column tagging at ingest and mapping review." },
                          { title: "IP allowlisting", desc: "Restrict API and MCP access to approved CIDR ranges." },
                          { title: "Session timeout", desc: "Automatically sign out idle workspace sessions." },
                          { title: "Two-factor authentication", desc: "Require an authenticator-app code at every password sign-in." },
                        ].map((item) => (
                          <div key={item.title} className="df2-settings-policy-row">
                            <div>
                              <h3>{item.title}</h3>
                              <p>{item.desc}</p>
                            </div>
                            <span className="df2-badge df2-badge-muted">API offline</span>
                          </div>
                        ))}
                      </div>
                    </>
                  )}
                </div>
              </section>
            )}

            {tab === "auth" && (
              <>
              <section className="df2-settings-section">
                <div className="df2-settings-section-head">
                  <div>
                    <h2>Authentication & SSO</h2>
                    <p>Connect your identity provider for enterprise single sign-on.</p>
                  </div>
                </div>
                <PermissionNotice
                  allowed={workspaceManage.allowed}
                  reason={workspaceManage.reason}
                  what="Identity provider settings are administered by workspace admins."
                />
                {workspaceManage.allowed && (
                  <PlatformSettingsNotice lock={ssoLock} what="Identity provider settings apply to every workspace." />
                )}
                {ssoError && workspaceManage.allowed && (
                  <div className="df2-permission-notice" role="alert" data-testid="sso-load-error">
                    <DtIcon name="alert" size={16} />
                    <p>
                      <strong>Identity provider state could not be read. </strong>
                      {ssoError} The cards below do not reflect saved configuration.
                    </p>
                  </div>
                )}
                <div className="df2-settings-section-body">
                  <div className="df2-settings-sso-grid">
                    {([
                      { type: "saml" as SsoType, name: "SAML 2.0", desc: "Okta, Azure AD, OneLogin", action: "Configure SAML" },
                      { type: "oidc" as SsoType, name: "OpenID Connect", desc: "Google Workspace, Auth0", action: "Configure OIDC" },
                      { type: "azure_ad" as SsoType, name: "Azure AD", desc: "Microsoft Entra ID", action: "Connect Azure" },
                    ]).map((provider) => {
                      const cfg = ssoConfigs?.[provider.type];
                      const enabled = cfg?.enabled;
                      return (
                      <div key={provider.name} className={`df2-settings-sso-card ${enabled ? "ready" : ""}`}>
                        <h3>{provider.name}</h3>
                        <p>{provider.desc}</p>
                        <span className={`df2-badge ${enabled ? "df2-badge-live" : "df2-badge-muted"}`}>
                          {ssoConfigs ? (enabled ? "Enabled" : "Not configured") : "Unknown"}
                        </span>
                        <button
                          type="button"
                          className="df2-btn df2-btn-sm df2-btn-primary"
                          disabled={!workspaceManage.allowed}
                          title={workspaceManage.reason || undefined}
                          onClick={() => openSsoEditor(provider.type)}
                        >
                          {ssoLock.locked ? `View ${provider.name}` : provider.action}
                        </button>
                      </div>
                    );})}
                  </div>
                  <p className="df2-settings-hint df2-mt-md">
                    Set the callback URL in your IdP to <code>{resolveApiBase()}/auth/sso/&lt;type&gt;/callback</code>.
                    Enabled providers appear on the sign-in page.
                  </p>
                </div>
              </section>
              <ScimSettings onOpenTeamSettings={() => setTab("team")} />
              </>
            )}

            {tab === "team" && <TeamSettings />}

            {tab === "notifications" && <NotificationSettings />}

            {tab === "enterprise" && <TenantSettings />}

            {tab === "models" && (
              <>
                <section className="df2-settings-section">
                  <div className="df2-settings-section-head">
                    <div>
                      <h2>Active model route</h2>
                      <p>Datawrap is a data-movement product. With no key saved, Pilot runs entirely on-box — nothing is sent to OpenAI or Anthropic. Once you save a key, Pilot uses that provider to word its answers until you remove the key; tools, gates and facts still run here. Pick “Our engine only” to keep a key saved but idle.</p>
                    </div>
                  </div>
                  <PlatformSettingsNotice lock={aiLock} what="AI provider settings apply to every workspace." />
                  <div className="df2-settings-section-body">
                    <div className="df2-model-ops">
                      <div>
                        <span className="df2-rail-kicker">Agent mode</span>
                        <h2 className="df2-settings-model-title">{modelCapabilities?.agent_mode ?? "local_tools"}</h2>
                        <p className="df2-settings-hint">
                          {modelCapabilities
                            ? `${modelCapabilities.active_provider} · ${modelCapabilities.active_model}`
                            : "Local deterministic engine active while model status loads."}
                        </p>
                      </div>
                      <div className="df2-settings-field">
                        <label htmlFor="pilot-engine">Pilot engine</label>
                        <select
                          id="pilot-engine"
                          className="df2-select"
                          disabled={engineSaving || !engineStatus || aiLock.locked}
                          title={aiLock.locked ? aiLock.reason : undefined}
                          value={engineStatus?.preference ?? "auto"}
                          onChange={(e) => void saveEngineChoice(e.target.value as PilotEngineChoice)}
                        >
                          <option value="auto">Auto — our engine; uses a saved third-party key if you add one</option>
                          <option value="local">Our engine only</option>
                          <option value="hybrid">Hybrid — our tools, third-party wording (sends evidence off-box)</option>
                          <option value="cloud">Third-party provider (sends evidence off-box)</option>
                        </select>
                        <p className="df2-settings-hint">
                          {engineStatus
                            ? `Resolved: ${engineStatus.engine} — ${engineStatus.reason}`
                            : "Loading which engine Pilot will use…"}
                        </p>
                      </div>
                      <div className="df2-model-route">
                        {(modelCapabilities?.fallback_order ?? ["local", "ollama", "anthropic", "openai"]).map((provider, index) => (
                          <span key={provider}>
                            {index > 0 && <i />}
                            <strong>{provider}</strong>
                          </span>
                        ))}
                      </div>
                    </div>
                  </div>
                </section>

                {(modelError || engineError) && (
                  <div className="df2-permission-notice" role="alert" data-testid="model-load-error">
                    <DtIcon name="alert" size={16} />
                    <p>
                      <strong>Pilot routing could not be read. </strong>
                      {modelError || engineError} What is shown below is not the active routing.
                    </p>
                  </div>
                )}
                {modelCapabilities?.settings_storage && !modelCapabilities.settings_storage.persistent && (
                  <div className="df2-permission-notice" role="alert" data-testid="settings-storage-warning">
                    <DtIcon name="alert" size={16} />
                    <p>
                      <strong>Saved keys may need to be entered again. </strong>
                      {modelCapabilities.settings_storage.reason}
                    </p>
                  </div>
                )}
                <div className="df2-model-grid">
                  {!modelCapabilitiesLoaded ? (
                    <SectionLoader title="Loading model providers" hint="Fetching AI routing configuration…" />
                  ) : !modelCapabilities || modelCapabilities.providers.length === 0 ? (
                    <EmptyState
                      compact
                      icon="sparkle"
                      title="No model providers"
                      description="Configure Anthropic, OpenAI, or Ollama credentials when the API is online."
                    />
                  ) : modelCapabilities.providers.map((provider) => (
                    <article key={provider.provider} className={`df2-model-card ${provider.available ? "ready" : ""}`}>
                      <div className="df2-model-card-head">
                        <div>
                          <h3>{provider.label}</h3>
                          <p>{provider.default_model}</p>
                        </div>
                        <span className={`df2-badge ${provider.available ? "df2-badge-live" : provider.tier === "cloud" ? "df2-badge-run" : "df2-badge-muted"}`}>
                          {provider.available ? "Ready" : provider.status}
                        </span>
                      </div>
                      <p className="df2-model-best">{provider.best_for}</p>
                      {provider.blocked_reason && (
                        <p className="df2-settings-hint">{provider.blocked_reason}</p>
                      )}
                      <div className="df2-model-roles">
                        {provider.roles.slice(0, 4).map((role) => (
                          <span key={role}>{role.replace(/_/g, " ")}</span>
                        ))}
                      </div>
                      {provider.provider !== "local" && (
                        <div className="df2-model-card-actions">
                          <button
                            type="button"
                            className="df2-btn df2-btn-sm df2-btn-primary"
                            onClick={() => void openAiEditor(provider.provider, provider.default_model)}
                          >
                            {aiLock.locked ? "View settings" : provider.available ? "Update credentials" : "Configure"}
                          </button>
                          {providerHasTest(provider) && (
                            <button
                              type="button"
                              className="df2-btn df2-btn-sm"
                              disabled={aiTesting === provider.provider}
                              onClick={() => void testProviderKey(provider.provider)}
                            >
                              {aiTesting === provider.provider ? "Testing…" : providerTestLabel(provider.provider)}
                            </button>
                          )}
                          {!aiLock.locked && provider.tier === "cloud" && provider.key_state && provider.key_state !== "none" && (
                            <button
                              type="button"
                              className="df2-btn df2-btn-sm df2-btn-danger"
                              data-testid={`remove-key-${provider.provider}`}
                              disabled={aiRemoving === provider.provider}
                              onClick={() => void removeProviderKey(provider.provider, provider.label)}
                            >
                              {aiRemoving === provider.provider ? "Removing…" : "Remove key"}
                            </button>
                          )}
                        </div>
                      )}
                    </article>
                  ))}
                </div>
              </>
            )}

            {tab === "api" && (
              <>
              <section className="df2-settings-section">
                <div className="df2-settings-section-head">
                  <div>
                    <h2>API keys</h2>
                    <p>
                      Authenticate programmatic transfers, schedules, and MCP. A key stays valid
                      until the expiration you choose, or until you revoke it. Logging out does
                      not remove it. Viewer reads. Operator runs jobs. Editor creates
                      connectors, transfers, and schedules. Admin can also delete connectors.
                      A key created before roles were stored acts as an editor.
                    </p>
                  </div>
                </div>
                <PermissionNotice
                  allowed={iamManage.allowed}
                  reason={iamManage.reason}
                  what="API keys are administered by platform admins."
                />
                {apiKeysError && iamManage.allowed && (
                  <div className="df2-permission-notice" role="alert" data-testid="api-keys-load-error">
                    <DtIcon name="alert" size={16} />
                    <p>
                      <strong>API keys could not be listed. </strong>
                      {apiKeysError} This is not the same as having no keys.
                    </p>
                  </div>
                )}
                <div className="df2-settings-section-body">
                  <div className="df2-api-key-toolbar">
                    <div className="df2-settings-field">
                      <label htmlFor="api-key-name">Key name</label>
                      <input
                        id="api-key-name"
                        className="df2-input"
                        value={newKeyName}
                        onChange={(e) => setNewKeyName(e.target.value)}
                        placeholder="e.g. Production ETL"
                      />
                    </div>
                    <div className="df2-settings-field">
                      <label htmlFor="api-key-role">Role</label>
                      <select
                        id="api-key-role"
                        className="df2-select"
                        value={newKeyRole}
                        onChange={(e) => setNewKeyRole(e.target.value as WorkspaceApiKeyRole)}
                      >
                        <option value="viewer">Viewer</option>
                        <option value="operator">Operator</option>
                        <option value="editor">Editor</option>
                        <option value="admin">Admin</option>
                      </select>
                    </div>
                    <div className="df2-settings-field">
                      <label htmlFor="api-key-expires">Expiration</label>
                      <select
                        id="api-key-expires"
                        className="df2-select"
                        data-testid="api-key-expires"
                        value={newKeyLifetime}
                        onChange={(e) => setNewKeyLifetime(e.target.value as WorkspaceApiKeyLifetime)}
                      >
                        {API_KEY_LIFETIME_OPTIONS.map((option) => (
                          <option key={option.value} value={option.value}>{option.label}</option>
                        ))}
                      </select>
                    </div>
                    <button
                      type="button"
                      className="df2-btn df2-btn-primary"
                      disabled={apiKeyGenerating || !iamManage.allowed}
                      title={iamManage.reason || undefined}
                      onClick={() => void generateApiKey()}
                    >
                      <DtIcon name="plus" size={14} />
                      {apiKeyGenerating ? "Generating…" : "Generate key"}
                    </button>
                  </div>

                  {revealedKey && (
                    <div className="df2-alert df2-alert-success df2-alert-banner df2-mb-md" role="status">
                      <div className="df2-alert-banner-body">
                        <strong>Copy your new API key</strong>
                        <p className="df2-settings-hint">This is the only time the full key is shown. Store it in your secrets manager.</p>
                        <p className="df2-settings-key-reveal"><code>{revealedKey}</code></p>
                      </div>
                      <div className="df2-alert-banner-actions">
                        <button
                          type="button"
                          className="df2-btn df2-btn-sm df2-btn-primary"
                          onClick={() => { void navigator.clipboard.writeText(revealedKey); toast({ title: "Copied to clipboard", tone: "success" }); }}
                        >
                          Copy
                        </button>
                        <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={() => setRevealedKey(null)}>
                          Dismiss
                        </button>
                      </div>
                    </div>
                  )}

                  {apiKeysLoading ? (
                    <SectionLoader title="Loading API keys" hint="Fetching workspace keys…" />
                  ) : apiKeys.length === 0 ? (
                    <EmptyState
                      compact
                      icon="key"
                      title="No API keys yet"
                      description="Use the toolbar above to authenticate programmatic transfers and MCP calls."
                    />
                  ) : (
                    <div className="df2-api-key-list" role="list" aria-label="Workspace API keys" data-testid="workspace-api-key-list">
                      {apiKeys.map((key) => (
                        <article key={key.id} className="df2-api-key-card" role="listitem">
                          <div className="df2-api-key-card-main">
                            <div className="df2-api-key-card-icon" aria-hidden>
                              <DtIcon name="key" size={18} />
                            </div>
                            <div className="df2-api-key-card-copy">
                              <div className="df2-api-key-card-title">
                                <strong>{key.name}</strong>
                              </div>
                              <div className="df2-api-key-card-meta-row">
                                <span className="df2-api-key-card-meta">Prefix: <code>{key.prefix}…</code></span>
                                <span className="df2-api-key-card-meta">Role: {key.role || "editor"}</span>
                                {key.access_label && (
                                  <span className="df2-api-key-card-meta" data-testid="api-key-access">
                                    Access: {key.access_label}
                                  </span>
                                )}
                                <span className="df2-api-key-card-meta">
                                  Created {key.created_at ? new Date(key.created_at).toLocaleString() : "—"}
                                </span>
                                <span className="df2-api-key-card-meta">
                                  Last used {key.last_used_at ? new Date(key.last_used_at).toLocaleString() : "Never"}
                                </span>
                                <span className="df2-api-key-card-meta">
                                  {key.expired
                                    ? `Expired ${key.expires_at ? new Date(key.expires_at).toLocaleString() : ""}`
                                    : key.expires_at
                                      ? `Expires ${new Date(key.expires_at).toLocaleString()}`
                                      : "No expiration"}
                                </span>
                              </div>
                            </div>
                          </div>
                          <div className="df2-api-key-card-actions">
                            <button
                              type="button"
                              className="df2-btn df2-btn-sm df2-btn-danger"
                              disabled={revokingKeyId === key.id || !iamManage.allowed}
                              title={iamManage.reason || undefined}
                              onClick={() => void revokeKey(key.id, key.name)}
                            >
                              {revokingKeyId === key.id ? "Revoking…" : "Revoke"}
                            </button>
                          </div>
                        </article>
                      ))}
                    </div>
                  )}
                </div>
              </section>
              <ServiceAccountsPanel />
              </>
            )}

            {tab === "logs" && (
              <>
              <section className="df2-settings-section">
                <div className="df2-settings-section-head">
                  <div>
                    <h2>Audit logs</h2>
                    <p>
                      Workspace-scoped events. Export downloads this workspace&apos;s sample — not a SOC 2 or HIPAA letter.
                      Verify walks the platform HMAC chain; findings named here are this workspace&apos;s records.
                    </p>
                  </div>
                  <div className="df2-settings-section-actions df2-iam-audit-actions">
                    <button
                      type="button"
                      className="df2-btn df2-btn-secondary df2-btn-sm"
                      onClick={() => void runChainVerification()}
                      disabled={chainVerifying}
                      title="Re-walk the HMAC chain and report any record that was altered or removed"
                    >
                      <DtIcon name="shield" size={14} /> {chainVerifying ? "Verifying…" : "Verify chain"}
                    </button>
                    <label className="df2-iam-audit-export-format">
                      <span>Format</span>
                      <select
                        className="df2-select"
                        data-testid="audit-export-format"
                        value={auditExportFormat}
                        onChange={(event) => setAuditExportFormat(event.target.value as AuditExportFormat)}
                      >
                        <option value="csv">CSV</option>
                        <option value="json">JSON</option>
                        <option value="ndjson">NDJSON</option>
                        <option value="cef">CEF</option>
                      </select>
                    </label>
                    <button
                      type="button"
                      className="df2-btn df2-btn-secondary df2-btn-sm"
                      data-testid="audit-export"
                      onClick={() => void downloadAuditExport(auditExportFormat)}
                    >
                      <DtIcon name="download" size={14} /> Export
                    </button>
                  </div>
                </div>
                <div className="df2-settings-section-body">
                  {chainReport && (
                    <div
                      className={`df2-chain-verdict ${chainReport.verified ? "is-ok" : "is-broken"}`}
                      role="status"
                    >
                      <strong>{chainVerdictTitle(chainReport)}</strong>
                      {chainReport.findings.length > 0 && (
                        <ul>
                          {chainReport.findings.slice(0, 10).map((f) => (
                            <li key={`${f.kind}-${f.index}`}>
                              <code>{f.kind}</code> at record {f.index}
                              {f.event_id ? ` (${f.event_id})` : ""} — {f.detail}
                            </li>
                          ))}
                        </ul>
                      )}
                      {chainReport.retention_checkpoints.length > 0 && (
                        <p>
                          Retention removed{" "}
                          {chainReport.retention_checkpoints.reduce((n, c) => n + c.removed_count, 0)}{" "}
                          older record(s), accounted for by signed checkpoints.
                        </p>
                      )}
                      <p className="df2-chain-verdict-honesty">{chainReport.honesty}</p>
                    </div>
                  )}
                  <FilterTabs
                    ariaLabel="Filter audit logs"
                    value={logFilter}
                    onChange={setLogFilter}
                    items={([
                      { id: "all", label: "All events" },
                      { id: "info", label: "Info" },
                      { id: "success", label: "Success" },
                      { id: "warn", label: "Warnings" },
                      { id: "error", label: "Errors" },
                    ] as const)}
                  />
                  <div className="df2-settings-logs-table-wrap">
                    {auditLoading ? (
                      <SectionLoader title="Loading audit events" hint="Fetching workspace activity…" />
                    ) : filteredLogs.length === 0 ? (
                      <EmptyState
                        compact
                        icon="activity"
                        title="No audit events yet"
                        description="Transfer plans, preflight runs, MCP calls, and API mutations are recorded here."
                      />
                    ) : (
                    <div className="df2-settings-table-wrap">
                    <table className="df2-settings-logs-table">
                      <thead>
                        <tr>
                          <th>Time</th>
                          <th>Actor</th>
                          <th>Action</th>
                          <th>Resource</th>
                          <th>Level</th>
                        </tr>
                      </thead>
                      <tbody>
                        {filteredLogs.map((log) => (
                          <tr key={log.id}>
                            <td>{log.time}</td>
                            <td>{log.actor}</td>
                            <td>{log.action}</td>
                            <td>{log.resource}</td>
                            <td>
                              <span className={`df2-settings-log-level df2-settings-log-level--${log.level}`}>
                                {log.level}
                              </span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    </div>
                    )}
                  </div>
                </div>
              </section>
              <section className="df2-settings-section" data-testid="audit-retention">
                <div className="df2-settings-section-head">
                  <div>
                    <h2>Retention</h2>
                    <p>Preview expired-event removal before purging. Purges record a retention checkpoint.</p>
                  </div>
                </div>
                <div className="df2-settings-section-body">
                  <PermissionNotice
                    allowed={retentionAllowed}
                    reason={retentionAllowed ? "" : retentionReason}
                    what="Audit retention is a platform administrator action."
                  />
                  <div className="df2-settings-section-actions">
                    <Button
                      variant="secondary"
                      disabled={!retentionAllowed || retentionLoading}
                      title={retentionAllowed ? undefined : retentionReason}
                      data-testid="audit-retention-preview"
                      onClick={() => void previewAuditRetention()}
                    >
                      {retentionLoading ? "Working…" : "Preview purge"}
                    </Button>
                    <Button
                      variant="danger"
                      disabled={retentionPurgeDisabled}
                      title={retentionAllowed ? undefined : retentionReason}
                      data-testid="audit-retention-purge"
                      onClick={() => void purgeAuditRetentionNow()}
                    >
                      Purge now
                    </Button>
                  </div>
                  {retentionError && (
                    <div className="df2-permission-notice" role="alert" data-testid="audit-retention-error">
                      {retentionError}
                    </div>
                  )}
                  {retentionResult && (
                    <div className="df2-settings-policy-row" role="status" data-testid="audit-retention-result">
                      <div>
                        {retentionResult.dry_run ? (
                          <p>Would remove: {retentionResult.would_remove ?? "not reported by the API"}</p>
                        ) : (
                          <p>Removed: {retentionResult.removed}</p>
                        )}
                        <p>Legal hold: {retentionResult.legal_hold ? "Enabled" : "Off"}</p>
                        <p>
                          {retentionResult.retention_days === null
                            ? "Retention is off"
                            : `Retention window: ${retentionResult.retention_days} days`}
                        </p>
                        {retentionResult.legal_hold && <p>Purge is disabled while legal hold is enabled.</p>}
                        {!retentionResult.legal_hold && retentionResult.retention_days === null && (
                          <p>Purge is disabled because retention is off.</p>
                        )}
                      </div>
                    </div>
                  )}
                </div>
              </section>
              </>
            )}
          </div>
        </div>

        {ssoEditor && ssoDraft && (
          <div className="dt-modal-overlay" onClick={() => setSsoEditor(null)} role="presentation">
            <div className="dt-modal dt-modal-lg" onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
              <div className="dt-modal-header">
                <div>
                  <h2 className="dt-modal-title">{ssoLock.locked ? "View" : "Configure"} {ssoEditor === "saml" ? "SAML 2.0" : ssoEditor === "oidc" ? "OpenID Connect" : "Azure AD"}</h2>
                  <p className="dt-modal-subtitle" data-testid={ssoLock.locked ? "sso-read-only-reason" : undefined}>
                    {ssoLock.locked ? `Read-only. ${ssoLock.reason}.` : "Settings are persisted and used for workspace sign-in."}
                  </p>
                </div>
                <button type="button" className="df2-close-btn" onClick={() => setSsoEditor(null)} aria-label="Close">
                  <DtIcon name="x" size={16} />
                </button>
              </div>
              <div className="dt-modal-body">
                <label className="df2-settings-policy-row">
                  <div><h3>Enable provider</h3><p>When enabled, this provider appears on the sign-in page.</p></div>
                  <button type="button" role="switch" aria-checked={ssoDraft.enabled} aria-label="Enable provider" disabled={ssoLock.locked} className={`df2-switch ${ssoDraft.enabled ? "on" : ""}`} onClick={() => setSsoDraft({ ...ssoDraft, enabled: !ssoDraft.enabled })}>
                    <span className="df2-switch-thumb" />
                  </button>
                </label>
                {ssoEditor === "saml" && (
                  <div className="df2-settings-grid df2-mt-md">
                    <div className="df2-settings-field"><label>Entity ID</label><input className="df2-input" aria-label="Entity ID" disabled={ssoLock.locked} value={ssoDraft.entity_id ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, entity_id: e.target.value })} /></div>
                    <div className="df2-settings-field"><label>SSO URL</label><input className="df2-input" aria-label="SSO URL" disabled={ssoLock.locked} value={ssoDraft.sso_url ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, sso_url: e.target.value })} /></div>
                    <div className="df2-settings-field df2-settings-field--full"><label>IdP X.509 certificate</label><textarea className="df2-input" aria-label="IdP X.509 certificate" disabled={ssoLock.locked} rows={4} placeholder="Paste certificate PEM" value={ssoDraft.x509_cert ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, x509_cert: e.target.value })} /></div>
                  </div>
                )}
                {ssoEditor === "oidc" && (
                  <div className="df2-settings-grid df2-mt-md">
                    <div className="df2-settings-field"><label>Issuer URL</label><input className="df2-input" aria-label="Issuer URL" disabled={ssoLock.locked} placeholder="https://accounts.google.com" value={ssoDraft.issuer ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, issuer: e.target.value })} /></div>
                    <div className="df2-settings-field"><label>Client ID</label><input className="df2-input" aria-label="Client ID" disabled={ssoLock.locked} value={ssoDraft.client_id ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, client_id: e.target.value })} /></div>
                    <div className="df2-settings-field"><label>Client secret</label><input className="df2-input" aria-label="Client secret" disabled={ssoLock.locked} type="password" placeholder="Leave blank to keep existing" value={ssoDraft.client_secret ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, client_secret: e.target.value })} /></div>
                    <div className="df2-settings-field df2-settings-field--full"><label>Redirect URI</label><input className="df2-input" aria-label="Redirect URI" disabled={ssoLock.locked} value={ssoDraft.redirect_uri ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, redirect_uri: e.target.value })} /></div>
                  </div>
                )}
                {ssoEditor === "azure_ad" && (
                  <div className="df2-settings-grid df2-mt-md">
                    <div className="df2-settings-field"><label>Tenant ID</label><input className="df2-input" aria-label="Tenant ID" disabled={ssoLock.locked} value={ssoDraft.tenant_id ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, tenant_id: e.target.value })} /></div>
                    <div className="df2-settings-field"><label>Client ID</label><input className="df2-input" aria-label="Client ID" disabled={ssoLock.locked} value={ssoDraft.client_id ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, client_id: e.target.value })} /></div>
                    <div className="df2-settings-field"><label>Client secret</label><input className="df2-input" aria-label="Client secret" disabled={ssoLock.locked} type="password" placeholder="Leave blank to keep existing" value={ssoDraft.client_secret ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, client_secret: e.target.value })} /></div>
                    <div className="df2-settings-field df2-settings-field--full"><label>Redirect URI</label><input className="df2-input" aria-label="Redirect URI" disabled={ssoLock.locked} value={ssoDraft.redirect_uri ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, redirect_uri: e.target.value })} /></div>
                  </div>
                )}
                {ssoEditor !== "saml" && ssoConfigs?.[ssoEditor]?.client_secret && (
                  <label className="df2-settings-field df2-mt-md" data-testid="sso-clear-secret">
                    <span>
                      <input id="sso-clear-secret" type="checkbox" disabled={ssoLock.locked} checked={ssoClearSecret} onChange={(e) => setSsoClearSecret(e.target.checked)} />{" "}
                      Remove the stored client secret
                    </span>
                  </label>
                )}
                <div className="df2-settings-field df2-settings-field--full df2-mt-md">
                  <label htmlFor="sso-email-domains">Email domains</label>
                  <input id="sso-email-domains" className="df2-input" disabled={ssoLock.locked} placeholder="example.com, corp.example.com" value={ssoDomainsText} onChange={(e) => setSsoDomainsText(e.target.value)} />
                  <p className="df2-label-hint">Sign-ins from these domains are sent to this provider. A domain belongs to one provider.</p>
                </div>
              </div>
              <div className="df2-card-footer">
                <button type="button" className="df2-btn df2-btn-ghost" onClick={() => void testSsoConfig(ssoEditor).then((r) => toast({ title: r.ok ? "SSO ready" : "SSO incomplete", message: r.message, tone: r.ok ? "success" : "warning" }))}>
                  Test configuration
                </button>
                {!ssoLock.locked && (
                  <button type="button" className="df2-btn df2-btn-primary" disabled={ssoSaving} onClick={() => void saveSsoConfig()}>
                    {ssoSaving ? "Saving…" : "Save SSO settings"}
                  </button>
                )}
              </div>
            </div>
          </div>
        )}

        {aiEditor && (
          <div className="dt-modal-overlay" onClick={() => setAiEditor(null)} role="presentation">
            <div className="dt-modal dt-modal-lg" onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
              <div className="dt-modal-header">
                <div>
                  <h2 className="dt-modal-title">{aiLock.locked ? "View" : "Configure"} {aiEditor}</h2>
                  <p className="dt-modal-subtitle" data-testid={aiLock.locked ? "ai-read-only-reason" : undefined}>
                    {aiLock.locked
                      ? `Read-only. ${aiLock.reason}.`
                      : "API keys are encrypted at rest. Leave key blank to keep the existing value."}
                  </p>
                </div>
                <button type="button" className="df2-close-btn" onClick={() => setAiEditor(null)} aria-label="Close">
                  <DtIcon name="x" size={16} />
                </button>
              </div>
              <div className="dt-modal-body">
                <div className="df2-settings-grid">
                  {aiEditor !== "ollama" && (
                    <div className="df2-settings-field df2-settings-field--full">
                      <label htmlFor="ai-provider-key">API key</label>
                      <input id="ai-provider-key" className="df2-input" type="password" disabled={aiLock.locked} placeholder="sk-… or leave blank to keep existing" value={aiDraft.api_key} onChange={(e) => setAiDraft({ ...aiDraft, api_key: e.target.value })} />
                    </div>
                  )}
                  <div className="df2-settings-field">
                    <label htmlFor="ai-provider-model">Model</label>
                    <input id="ai-provider-model" className="df2-input" disabled={aiLock.locked} value={aiDraft.model} onChange={(e) => setAiDraft({ ...aiDraft, model: e.target.value })} />
                  </div>
                  {aiEditor === "ollama" && (
                    <div className="df2-settings-field">
                      <label htmlFor="ai-provider-base-url">Base URL</label>
                      <input id="ai-provider-base-url" className="df2-input" disabled={aiLock.locked} value={aiDraft.base_url} onChange={(e) => setAiDraft({ ...aiDraft, base_url: e.target.value })} />
                    </div>
                  )}
                </div>
              </div>
              {!aiLock.locked && (
                <div className="df2-card-footer">
                  <button type="button" className="df2-btn df2-btn-primary" disabled={aiSaving} onClick={() => void saveAiProvider()}>
                    {aiSaving ? "Saving…" : "Save provider settings"}
                  </button>
                </div>
              )}
            </div>
          </div>
        )}
      </PageFrame>
    </PageShell>
  );
}
