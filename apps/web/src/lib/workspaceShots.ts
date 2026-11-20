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
  transferSyncModesCard: `${dir}/app-transfer-sync-modes-card.webp`,
  transferTransform: `${dir}/app-transfer-transform.webp`,
  transferMap: `${dir}/app-transfer-map.webp`,
  transferMapStage: `${dir}/app-transfer-map-stage.webp`,
  transferMapWhy: `${dir}/app-transfer-map-why.webp`,
  transferMapWhyStage: `${dir}/app-transfer-map-why-stage.webp`,
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
  jobTrustStage: `${dir}/app-jobs-trust-stage.webp`,
  jobTrustEvidence: `${dir}/app-jobs-trust-evidence.webp`,
  jobSignoff: `${dir}/app-jobs-signoff.webp`,
  jobTrustExplained: `${dir}/app-jobs-trust-explained.webp`,
  jobTrustExplainedStage: `${dir}/app-jobs-trust-explained-stage.webp`,
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

  /* Marketing crops: a region of a 2x capture above, at its pixel size. */
  jobsEvidenceCrop: `${dir}/app-jobs-evidence-crop.webp`,
  transferWhyCrop: `${dir}/app-transfer-why-crop.webp`,
  queryRefusalCrop: `${dir}/app-query-refusal-crop.webp`,
  mcpSetupCrop: `${dir}/app-mcp-setup-crop.webp`,
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

/**
 * Intrinsic pixel size of every screenshot, so each image reserves its box
 * before it loads. A test reads the WebP headers and fails on drift.
 */
const SHOT_PIXELS: Record<string, [number, number]> = {
  "app-connectors-wizard.webp": [2880, 1800],
  "app-connectors.webp": [2880, 1800],
  "app-contracts-card.webp": [1400, 1372],
  "app-contracts-columns.webp": [2880, 1800],
  "app-contracts-detail.webp": [2880, 1800],
  "app-contracts.webp": [2880, 1800],
  "app-exit-readiness-declare.webp": [2880, 1800],
  "app-exit-readiness.webp": [2880, 1800],
  "app-jobs-detail.webp": [2880, 1800],
  "app-jobs-evidence-crop.webp": [1360, 750],
  "app-jobs-mapping.webp": [2880, 1800],
  "app-jobs-reversibility-card.webp": [1400, 1372],
  "app-jobs-reversibility-columns.webp": [2880, 1800],
  "app-jobs-reversibility-lossy.webp": [2880, 1800],
  "app-jobs-reversibility-partial.webp": [2880, 1800],
  "app-jobs-reversibility.webp": [2880, 1800],
  "app-jobs-signoff.webp": [2880, 1800],
  "app-jobs-trust-card.webp": [1400, 1372],
  "app-jobs-trust-evidence.webp": [2880, 1800],
  "app-jobs-trust-explained-diff.webp": [2880, 1800],
  "app-jobs-trust-explained-stage.webp": [1440, 1736],
  "app-jobs-trust-explained.webp": [2880, 1800],
  "app-jobs-trust-stage.webp": [1440, 1736],
  "app-jobs-trust.webp": [2880, 1800],
  "app-jobs.webp": [2880, 1800],
  "app-library.webp": [2880, 1800],
  "app-mcp-setup-crop.webp": [1080, 1286],
  "app-mcp.webp": [2880, 1800],
  "app-overview.webp": [2880, 1800],
  "app-pilot-card.webp": [1770, 1420],
  "app-pilot.webp": [2880, 1800],
  "app-proofs.webp": [2880, 1800],
  "app-query-card.webp": [2330, 1210],
  "app-query-readonly.webp": [2880, 1800],
  "app-query-refusal-crop.webp": [1740, 725],
  "app-query-source.webp": [2880, 1800],
  "app-query.webp": [2880, 1800],
  "app-schedules-cadence-mode.webp": [2880, 1800],
  "app-schedules-card.webp": [2330, 1160],
  "app-schedules-create.webp": [2880, 1800],
  "app-schedules-drawer-config.webp": [2880, 1800],
  "app-schedules-drawer-schema.webp": [2880, 1800],
  "app-schedules-drawer.webp": [2880, 1800],
  "app-schedules-history.webp": [2880, 1800],
  "app-schedules-reversibility-retries.webp": [2880, 1800],
  "app-schedules.webp": [2880, 1800],
  "app-settings-sso.webp": [2880, 1800],
  "app-settings.webp": [2880, 1800],
  "app-transfer-destination.webp": [2880, 1800],
  "app-transfer-map-approved.webp": [2880, 1800],
  "app-transfer-map-stage.webp": [2480, 1672],
  "app-transfer-map-why-stage.webp": [2480, 1672],
  "app-transfer-map-why.webp": [2880, 1800],
  "app-transfer-map.webp": [2880, 1800],
  "app-transfer-run-complete.webp": [2880, 1800],
  "app-transfer-run-load.webp": [2880, 1800],
  "app-transfer-run.webp": [2880, 1800],
  "app-transfer-source.webp": [2880, 1800],
  "app-transfer-sync-modes-card.webp": [1438, 1800],
  "app-transfer-sync-modes.webp": [2880, 1800],
  "app-transfer-transform.webp": [2880, 1800],
  "app-transfer-validate-blocked.webp": [2880, 1800],
  "app-transfer-validate-gates.webp": [2880, 1800],
  "app-transfer-validate-verdict.webp": [1460, 360],
  "app-transfer-validate.webp": [2880, 1800],
  "app-transfer-why-crop.webp": [1470, 425],
  "app-transforms-detail.webp": [2880, 1800],
  "app-transforms.webp": [2880, 1800],
};

/** Width and height of a shipped screenshot, by its public path. */
export function shotSize(src: string): { width: number; height: number } {
  const px = SHOT_PIXELS[src.split("?")[0].split("/").pop() ?? ""] ?? [2880, 1800];
  return { width: px[0], height: px[1] };
}

export { SHOT_PIXELS };

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
