import type { EndpointConfig, FileFormat } from "../types";
import type { TransferTemplate } from "../transferModes";

export interface TransferSelection {
  templateId: string;
  source: EndpointConfig;
  destination: EndpointConfig;
  exportFormat: FileFormat;
  apiUrl: string;
  sourceConnectorId: string;
  destConnectorId: string;
  destDbType: string;
  sourceDbType: string;
}

export interface ValidationResult {
  ok: boolean;
  message?: string;
}

/** Validates step 1 — selection only, no credentials required. */
export class TransferSelectionValidator {
  validateStep1(template: TransferTemplate, selection: TransferSelection): ValidationResult {
    if (template.sourceKind === "file" && !selection.source.file.fileName) {
      return { ok: false, message: "Upload a source file to continue." };
    }
    if (template.sourceKind === "database" && !selection.sourceDbType) {
      return { ok: false, message: "Select a source database type." };
    }
    if (template.sourceKind === "api" && !selection.apiUrl.trim()) {
      return { ok: false, message: "Enter an API endpoint URL." };
    }
    if (template.destKind === "database" && !selection.destConnectorId && !selection.destDbType) {
      return { ok: false, message: "Select a destination database or saved connector." };
    }
    if (template.destKind === "file" && !selection.exportFormat) {
      return { ok: false, message: "Select an export format." };
    }
    return { ok: true };
  }

  validateStep2Connections(
    template: TransferTemplate,
    sourceConnStr: string,
    destConnStr: string
  ): ValidationResult {
    if (template.sourceKind === "database" && !sourceConnStr.trim()) {
      return { ok: false, message: "Connect the source database to continue." };
    }
    if (template.destKind === "database" && !destConnStr.trim()) {
      return { ok: false, message: "Connect the destination database to continue." };
    }
    return { ok: true };
  }
}
