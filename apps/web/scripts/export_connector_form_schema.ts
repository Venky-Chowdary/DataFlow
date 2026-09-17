/**
 * Regenerate apps/api/data/connector_form_schema.json from connectorFormConfig.ts.
 *
 * Run: npx tsx scripts/export_connector_form_schema.ts   (from apps/web)
 * `connectorFormSchemaExport.test.ts` fails when the committed file is stale.
 */
import { writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { serializeConnectorFormSchema } from "../src/lib/connectorFormSchemaExport";

const WEB = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
export const SCHEMA_PATH = path.join(WEB, "..", "api", "data", "connector_form_schema.json");

writeFileSync(SCHEMA_PATH, serializeConnectorFormSchema());
console.log(`wrote ${SCHEMA_PATH}`);
