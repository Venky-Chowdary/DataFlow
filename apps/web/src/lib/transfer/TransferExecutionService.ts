import type { TransferTemplate } from "../transferModes";
import type { EndpointConfig } from "../types";
import type { TransferDraft } from "../../screens/TransferSelectScreen";
import {
  autoConnectDatabases,
  prepareApiToDatabase,
  prepareDatabaseToFile,
  prepareFileToDatabase,
  type AutoConnectResult,
} from "../autoDbSetup";
import { fetchSavedConnector } from "../api";
import { emptyCredentials, resolveConnectionString } from "../samples";
import type { CredentialFields } from "@dataflow/design-system";

export interface ConnectionContext {
  sourceConnStr: string;
  destConnStr: string;
  sourceCreds: CredentialFields;
  destCreds: CredentialFields;
}

/** Resolves connection strings from saved connectors or credential forms. */
export class EndpointConnectionResolver {
  async resolveFromDraft(
    draft: TransferDraft,
    ctx: ConnectionContext
  ): Promise<{ sourceConnStr: string; destConnStr: string }> {
    let sourceConnStr = resolveConnectionString(ctx.sourceConnStr, ctx.sourceCreds);
    let destConnStr = resolveConnectionString(ctx.destConnStr, ctx.destCreds);

    if (draft.destConnectorId && !destConnStr) {
      const conn = await fetchSavedConnector(draft.destConnectorId);
      destConnStr = conn.connection_string;
    }
    if (draft.sourceConnectorId && !sourceConnStr) {
      const conn = await fetchSavedConnector(draft.sourceConnectorId);
      sourceConnStr = conn.connection_string;
    }

    return { sourceConnStr, destConnStr };
  }

  emptyCredentials(type: string): CredentialFields {
    return emptyCredentials(type);
  }
}

/** Executes connect + semantic mapping for each transfer template. */
export class TransferExecutionService {
  constructor(private readonly resolver = new EndpointConnectionResolver()) {}

  async prepare(
    template: TransferTemplate,
    draft: TransferDraft,
    ctx: ConnectionContext,
    onProgress?: (message: string) => void
  ): Promise<AutoConnectResult & { error?: string }> {
    const report = (msg: string) => onProgress?.(msg);
    const { sourceConnStr, destConnStr } = await this.resolver.resolveFromDraft(draft, ctx);

    const progress = (p: { message: string }) => report(p.message);

    if (template.id === "file-db") {
      return prepareFileToDatabase(draft.source, destConnStr, { onProgress: (p) => progress(p) });
    }
    if (template.id === "db-db") {
      return autoConnectDatabases(sourceConnStr, destConnStr, { onProgress: (p) => progress(p) });
    }
    if (template.id === "db-file") {
      return prepareDatabaseToFile(sourceConnStr, { onProgress: (p) => progress(p) });
    }
    if (template.id === "api-db") {
      return prepareApiToDatabase(draft.apiUrl, destConnStr, { onProgress: (p) => progress(p) });
    }

    return {
      source: draft.source,
      destination: draft.destination,
      selectedTables: [],
      identityMappings: draft.source.file.columns.map((c) => ({
        source: c.name,
        target: c.name,
        confidence: 1,
        reasoning: "Format conversion",
      })),
    };
  }

  needsConnection(template: TransferTemplate): { source: boolean; dest: boolean } {
    return {
      source: template.sourceKind === "database",
      dest: template.destKind === "database",
    };
  }
}

export function applyPreparedEndpoints(
  draft: TransferDraft,
  result: AutoConnectResult
): { source: EndpointConfig; destination: EndpointConfig } {
  return {
    source: result.source.kind ? result.source : draft.source,
    destination: result.destination.kind ? result.destination : draft.destination,
  };
}
