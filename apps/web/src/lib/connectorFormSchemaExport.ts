import { CONNECTOR_CATALOG } from "./types";
import { getConnectorFormConfig } from "./connectorFormConfig";
import { getConnectorSetupGuide } from "./connectorSetupGuide";
import { resolveCatalogIdToType } from "./connectorTypes";

/**
 * Serialisable view of the connector form (`connectorFormConfig.ts` is the
 * owner). `apps/api/data/connector_form_schema.json` is generated from this so
 * the API — Pilot's connect procedures in particular — describes the same
 * fields, labels and setup steps the form shows. Validators are left out;
 * `setup_steps` is empty when the engine only has the generic guide.
 */
export interface ExportedFormField {
  key: string;
  label: string;
  type: string;
  optional: boolean;
  sensitive: boolean;
  hint?: string;
  placeholder?: string;
}

export interface ExportedAuthMode {
  value: string;
  label: string;
  description: string;
  fields: ExportedFormField[];
}

export interface ExportedConnectorForm {
  label: string;
  default_auth_mode: string;
  auth_modes: ExportedAuthMode[];
  common_fields: ExportedFormField[];
  setup_steps: string[];
}

export type ConnectorFormSchema = Record<string, ExportedConnectorForm>;

function exportField(field: {
  key: string;
  label: string;
  type?: string;
  optional?: boolean;
  sensitive?: boolean;
  hint?: string;
  placeholder?: string;
}): ExportedFormField {
  const out: ExportedFormField = {
    key: field.key,
    label: field.label,
    type: field.type || "text",
    optional: Boolean(field.optional),
    sensitive: Boolean(field.sensitive),
  };
  if (field.hint) out.hint = field.hint;
  if (field.placeholder) out.placeholder = field.placeholder;
  return out;
}

export function exportConnectorForm(type: string): ExportedConnectorForm {
  const config = getConnectorFormConfig(type);
  const guide = getConnectorSetupGuide(type);
  const engineSpecific = guide !== getConnectorSetupGuide("__generic__");
  return {
    label: config.label,
    default_auth_mode: config.defaultAuthMode,
    auth_modes: config.authModes.map((mode) => ({
      value: mode.value,
      label: mode.label,
      description: mode.description || "",
      fields: mode.fields.map(exportField),
    })),
    common_fields: config.commonFields.map(exportField),
    setup_steps: engineSpecific ? guide.steps : [],
  };
}

/** One entry per resolved driver type in the catalog, keys sorted. */
export function exportConnectorFormSchema(): ConnectorFormSchema {
  const types = new Set<string>();
  for (const tile of CONNECTOR_CATALOG) types.add(resolveCatalogIdToType(tile.id));
  const out: ConnectorFormSchema = {};
  for (const type of [...types].sort()) out[type] = exportConnectorForm(type);
  return out;
}

export function serializeConnectorFormSchema(): string {
  return `${JSON.stringify(exportConnectorFormSchema(), null, 2)}\n`;
}
