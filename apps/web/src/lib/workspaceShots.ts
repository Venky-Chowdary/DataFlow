/**
 * Every marketing and docs screenshot, captured from the hosted workspace on a
 * real run: Orders Postgres (demo) `public.sales_orders` → Warehouse Snowflake
 * (demo) `DW_DEMO.SALES_ORDERS_REPORTING`, 5,100 rows, full checksum. Pages
 * import paths from here so a recapture changes one file, not six.
 */
const dir = "/docs/screenshots";

export const WORKSPACE_SHOT = {
  overview: `${dir}/app-overview.webp`,
  connectors: `${dir}/app-connectors.webp`,
  connectorsWizard: `${dir}/app-connectors-wizard.webp`,

  transferSource: `${dir}/app-transfer-source.webp`,
  transferDest: `${dir}/app-transfer-destination.webp`,
  transferSyncModes: `${dir}/app-transfer-sync-modes.webp`,
  transferTransform: `${dir}/app-transfer-transform.webp`,
  transferMap: `${dir}/app-transfer-map.webp`,
  transferMapWhy: `${dir}/app-transfer-map-why.webp`,
  transferMapApproved: `${dir}/app-transfer-map-approved.webp`,
  transferValidateBlocked: `${dir}/app-transfer-validate-blocked.webp`,
  transferValidate: `${dir}/app-transfer-validate.webp`,
  transferValidateGates: `${dir}/app-transfer-validate-gates.webp`,
  transferRun: `${dir}/app-transfer-run.webp`,
  transferRunLoad: `${dir}/app-transfer-run-load.webp`,
  transferRunComplete: `${dir}/app-transfer-run-complete.webp`,

  jobs: `${dir}/app-jobs.webp`,
  jobDetail: `${dir}/app-jobs-detail.webp`,
  jobMapping: `${dir}/app-jobs-mapping.webp`,
  jobTrust: `${dir}/app-jobs-trust.webp`,
  jobTrustEvidence: `${dir}/app-jobs-trust-evidence.webp`,
  jobSignoff: `${dir}/app-jobs-signoff.webp`,
  jobTrustExplained: `${dir}/app-jobs-trust-explained.webp`,
  jobTrustExplainedDiff: `${dir}/app-jobs-trust-explained-diff.webp`,
  jobReversibility: `${dir}/app-jobs-reversibility.webp`,
  jobReversibilityColumns: `${dir}/app-jobs-reversibility-columns.webp`,
  jobReversibilityPartial: `${dir}/app-jobs-reversibility-partial.webp`,
  jobReversibilityLossy: `${dir}/app-jobs-reversibility-lossy.webp`,

  query: `${dir}/app-query.webp`,
  querySource: `${dir}/app-query-source.webp`,
  queryReadonly: `${dir}/app-query-readonly.webp`,

  schedules: `${dir}/app-schedules.webp`,
  schedulesCreate: `${dir}/app-schedules-create.webp`,
  schedulesCadenceMode: `${dir}/app-schedules-cadence-mode.webp`,
  schedulesReversibilityRetries: `${dir}/app-schedules-reversibility-retries.webp`,
  schedulesDrawer: `${dir}/app-schedules-drawer.webp`,
  schedulesDrawerSchema: `${dir}/app-schedules-drawer-schema.webp`,
  schedulesDrawerConfig: `${dir}/app-schedules-drawer-config.webp`,
  schedulesHistory: `${dir}/app-schedules-history.webp`,

  contracts: `${dir}/app-contracts.webp`,
  contractsDetail: `${dir}/app-contracts-detail.webp`,
  contractsColumns: `${dir}/app-contracts-columns.webp`,
  exitReadiness: `${dir}/app-exit-readiness.webp`,
  exitReadinessDeclare: `${dir}/app-exit-readiness-declare.webp`,

  transforms: `${dir}/app-transforms.webp`,
  transformsDetail: `${dir}/app-transforms-detail.webp`,
  library: `${dir}/app-library.webp`,
  pilot: `${dir}/app-pilot.webp`,
  mcp: `${dir}/app-mcp.webp`,
  proofs: `${dir}/app-proofs.webp`,
  settings: `${dir}/app-settings.webp`,
  settingsSso: `${dir}/app-settings-sso.webp`,

  /* Panels cut from the captures above, so a small tile shows legible UI
     instead of a full window with a dimmed backdrop. */
  jobTrustCard: `${dir}/app-jobs-trust-card.webp`,
  jobReversibilityCard: `${dir}/app-jobs-reversibility-card.webp`,
  transferValidateVerdict: `${dir}/app-transfer-validate-verdict.webp`,
  schedulesCard: `${dir}/app-schedules-card.webp`,
  contractsCard: `${dir}/app-contracts-card.webp`,
  queryCard: `${dir}/app-query-card.webp`,
  pilotCard: `${dir}/app-pilot-card.webp`,
} as const;

export type WorkspaceShotKey = keyof typeof WORKSPACE_SHOT;

/** The route every Studio and Job space capture shows. */
export const DEMO_ROUTE = {
  source: "PostgreSQL · public.sales_orders",
  dest: "Snowflake · DW_DEMO.SALES_ORDERS_REPORTING",
} as const;

/** The schedule shown in the Schedules captures. */
export const DEMO_SCHEDULE_ROUTE = {
  source: "PostgreSQL · orders_feed",
  dest: "PostgreSQL · Analytics (demo)",
} as const;
