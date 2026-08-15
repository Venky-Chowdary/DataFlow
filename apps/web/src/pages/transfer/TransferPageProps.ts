import type { Connector, PreflightResult } from "../../lib/types";

export interface TransferPageProps {
  connectors: Connector[];
  /** True while the first connectors fetch has not settled yet. */
  connectorsLoading?: boolean;
  onTransferComplete: () => void;
  onOpenSchedules?: () => void;
  /** Jump to Contracts after Save as contract so the draft is visible immediately. */
  onOpenContracts?: () => void;
  /** Remount studio and clear prior transfer cache (source, map, result). */
  onFreshTransfer?: () => void;
  /** Pre-select a saved connection as the Transfer Studio source (from Connectors drawer). */
  seedSourceConnector?: { connectorId: string; token: number } | null;
  /** Jobs → Studio deep-link: land on Validate/Map with optional repair + mappings. */
  seedStudioIntent?: {
    token: number;
    step?: "validate" | "map" | "source";
    repairProposalId?: string;
    jobId?: string;
    preflight?: PreflightResult;
    validationMode?: string;
    schemaPolicy?: string;
    mappings?: Array<{
      source?: string;
      destination?: string;
      destination_type?: string;
      target_type?: string;
      transform?: string;
      transforms?: { type?: string }[];
      [key: string]: unknown;
    }>;
  } | null;
}
