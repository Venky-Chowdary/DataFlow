/**
 * Human labels for the engine's load-method tokens.
 *
 * These are internal identifiers that reached three different operator surfaces
 * verbatim. `copy_binary_server_to_server` in particular is the difference
 * between minutes and hours on a large table, and the raw token communicates
 * none of that — so the label and the reason it was chosen live in one place
 * rather than being re-spelled per page.
 */

export interface LoadMethodInfo {
  label: string;
  description: string;
}

const LOAD_METHODS: Record<string, LoadMethodInfo> = {
  copy_binary_server_to_server: {
    label: "Server-to-server COPY",
    description:
      "Rows streamed directly between the two engines in binary form, never "
      + "materialised in the transfer process. Taken when every mapped column has "
      + "the same declared type on both sides, so nothing in the path can change "
      + "a value.",
  },
  copy: { label: "COPY", description: "Bulk COPY into the destination." },
  bulk_copy: { label: "Bulk COPY", description: "Bulk COPY into the destination." },
  copy_text_pg_to_mysql_load_data: {
    label: "PostgreSQL COPY → MySQL LOAD DATA",
    description:
      "Identity append/overwrite: PostgreSQL COPY text piped into STRICT "
      + "LOAD DATA LOCAL INFILE. Dest COUNT(*) must equal the source snapshot.",
  },
  copy_text_pg_to_mysql_load_data_upsert: {
    label: "PostgreSQL COPY → MySQL upsert",
    description:
      "Identity upsert: LOAD DATA into a staging table, then INSERT ... ON "
      + "DUPLICATE KEY UPDATE. Proof is dest PK ⋈ staging, not dest COUNT(*).",
  },
  copy_binary_server_to_server_upsert: {
    label: "Server COPY then upsert",
    description:
      "Identity upsert: binary COPY into staging, then INSERT ... ON CONFLICT. "
      + "Proof is dest PK ⋈ staging, not dest COUNT(*).",
  },
  insert: { label: "Insert", description: "Row batches inserted into the destination." },
  upsert: { label: "Upsert", description: "Row batches merged on the identity key." },
  merge_batch: {
    label: "Merge",
    description: "Batches merged into the destination on the identity key.",
  },
  pgvector_upsert: {
    label: "Vector upsert",
    description: "Embedded chunks upserted into the vector store.",
  },
};

export function loadMethodLabel(method: string | null | undefined): string {
  const key = String(method || "").trim();
  if (!key) return "";
  return LOAD_METHODS[key]?.label ?? key;
}

export function loadMethodDescription(method: string | null | undefined): string {
  const key = String(method || "").trim();
  if (!key) return "";
  return LOAD_METHODS[key]?.description ?? `Load path for this job: ${key}.`;
}
