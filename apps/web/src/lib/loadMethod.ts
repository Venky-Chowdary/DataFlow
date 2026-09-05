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
  copy_binary_server_to_server_incremental_append: {
    label: "Server COPY incremental append",
    description:
      "Identity incremental: binary COPY of rows past the cursor watermark into "
      + "staging, then INSERT (duplicate PK fails closed). Dest COUNT(*) must "
      + "equal dest_before + staging.",
  },
  copy_binary_server_to_server_incremental_deduped: {
    label: "Server COPY incremental upsert",
    description:
      "Identity incremental: binary COPY of rows past the cursor watermark into "
      + "staging, then INSERT ... ON CONFLICT. Proof is dest PK ⋈ staging.",
  },
  copy_text_pg_to_mysql_load_data_incremental_append: {
    label: "PostgreSQL COPY → MySQL incremental append",
    description:
      "Identity incremental: COPY of rows past the cursor watermark into staging, "
      + "then INSERT (duplicate PK fails closed). Dest COUNT(*) must equal "
      + "dest_before + staging.",
  },
  copy_text_pg_to_mysql_load_data_incremental_deduped: {
    label: "PostgreSQL COPY → MySQL incremental upsert",
    description:
      "Identity incremental: COPY of rows past the cursor watermark into staging, "
      + "then INSERT ... ON DUPLICATE KEY UPDATE. Proof is dest PK ⋈ staging.",
  },
  copy_text_mysql_to_pg_stdin_incremental_append: {
    label: "MySQL → PostgreSQL incremental append",
    description:
      "Identity incremental: MySQL consistent-snapshot SELECT of rows past the "
      + "cursor watermark into staging COPY, then INSERT (duplicate PK fails closed).",
  },
  copy_text_mysql_to_pg_stdin_incremental_deduped: {
    label: "MySQL → PostgreSQL incremental upsert",
    description:
      "Identity incremental: MySQL consistent-snapshot SELECT of rows past the "
      + "cursor watermark into staging COPY, then INSERT ... ON CONFLICT.",
  },
  insert_select_mysql_same_instance_incremental_append: {
    label: "MySQL INSERT SELECT incremental append",
    description:
      "Identity incremental: same-instance INSERT SELECT of rows past the cursor "
      + "watermark into staging, then INSERT (duplicate PK fails closed).",
  },
  insert_select_mysql_same_instance_incremental_deduped: {
    label: "MySQL INSERT SELECT incremental upsert",
    description:
      "Identity incremental: same-instance INSERT SELECT of rows past the cursor "
      + "watermark into staging, then INSERT ... ON DUPLICATE KEY UPDATE.",
  },
  copy_text_mysql_to_mysql_load_data_incremental_append: {
    label: "MySQL LOAD DATA incremental append",
    description:
      "Identity incremental: cross-host LOAD DATA of rows past the cursor "
      + "watermark into staging, then INSERT (duplicate PK fails closed).",
  },
  copy_text_mysql_to_mysql_load_data_incremental_deduped: {
    label: "MySQL LOAD DATA incremental upsert",
    description:
      "Identity incremental: cross-host LOAD DATA of rows past the cursor "
      + "watermark into staging, then INSERT ... ON DUPLICATE KEY UPDATE.",
  },
  attach_insert_select_sqlite: {
    label: "SQLite ATTACH INSERT SELECT",
    description:
      "Identity append/overwrite: ATTACH the source file and INSERT SELECT. "
      + "Dest COUNT(*) must equal the source snapshot. Not .dump / .import.",
  },
  attach_insert_select_sqlite_incremental_append: {
    label: "SQLite incremental append",
    description:
      "Identity incremental: INSERT SELECT of rows past the cursor watermark "
      + "into staging, then INSERT (duplicate PK fails closed).",
  },
  attach_insert_select_sqlite_incremental_deduped: {
    label: "SQLite incremental upsert",
    description:
      "Identity incremental: INSERT SELECT of rows past the cursor watermark "
      + "into staging, then INSERT ... ON CONFLICT DO UPDATE.",
  },
  copy_text_pg_executemany_sqlite: {
    label: "PostgreSQL COPY → SQLite",
    description:
      "Identity append/overwrite: PostgreSQL COPY text piped into SQLite "
      + "executemany. DATE lands as TEXT. Dest COUNT(*) must equal the source snapshot.",
  },
  copy_text_pg_executemany_sqlite_incremental_append: {
    label: "PostgreSQL → SQLite incremental append",
    description:
      "Identity incremental: COPY of DATE rows past the cursor watermark into "
      + "staging, then INSERT (duplicate PK fails closed). TIMESTAMP is COPY-unsafe.",
  },
  copy_text_pg_executemany_sqlite_incremental_deduped: {
    label: "PostgreSQL → SQLite incremental upsert",
    description:
      "Identity incremental: COPY of DATE rows past the cursor watermark into "
      + "staging, then INSERT ... ON CONFLICT. TIMESTAMP is COPY-unsafe.",
  },
  select_mysql_executemany_sqlite: {
    label: "MySQL snapshot → SQLite",
    description:
      "Identity append/overwrite: MySQL consistent-snapshot SELECT into SQLite "
      + "executemany. DATETIME lands as TEXT. Dest COUNT(*) must equal the source snapshot.",
  },
  select_mysql_executemany_sqlite_incremental_append: {
    label: "MySQL → SQLite incremental append",
    description:
      "Identity incremental: MySQL consistent-snapshot SELECT of DATETIME rows "
      + "past the cursor watermark into staging, then INSERT (duplicate PK fails closed).",
  },
  select_mysql_executemany_sqlite_incremental_deduped: {
    label: "MySQL → SQLite incremental upsert",
    description:
      "Identity incremental: MySQL consistent-snapshot SELECT of DATETIME rows "
      + "past the cursor watermark into staging, then INSERT ... ON CONFLICT.",
  },
  select_sqlite_copy_from_stdin_pg: {
    label: "SQLite → PostgreSQL COPY",
    description:
      "Identity append/overwrite: SQLite SELECT encoded as PostgreSQL COPY FROM "
      + "STDIN. Dest COUNT(*) must equal the source snapshot. Not .dump.",
  },
  select_sqlite_copy_from_stdin_pg_incremental_append: {
    label: "SQLite → PostgreSQL incremental append",
    description:
      "Identity incremental: SQLite SELECT of TEXT rows past the cursor watermark "
      + "into staging COPY, then INSERT (duplicate PK fails closed). DATETIME is COPY-unsafe.",
  },
  select_sqlite_copy_from_stdin_pg_incremental_deduped: {
    label: "SQLite → PostgreSQL incremental upsert",
    description:
      "Identity incremental: SQLite SELECT of TEXT rows past the cursor watermark "
      + "into staging COPY, then INSERT ... ON CONFLICT. DATETIME is COPY-unsafe.",
  },
  select_sqlite_load_data_mysql: {
    label: "SQLite → MySQL LOAD DATA",
    description:
      "Identity append/overwrite: SQLite SELECT encoded as STRICT LOAD DATA. "
      + "Dest COUNT(*) must equal the source snapshot. Not .dump / sqlldr.",
  },
  select_sqlite_load_data_mysql_incremental_append: {
    label: "SQLite → MySQL incremental append",
    description:
      "Identity incremental: SQLite SELECT of TEXT rows past the cursor watermark "
      + "into staging LOAD DATA, then INSERT (duplicate PK fails closed). DATETIME is COPY-unsafe.",
  },
  select_sqlite_load_data_mysql_incremental_deduped: {
    label: "SQLite → MySQL incremental upsert",
    description:
      "Identity incremental: SQLite SELECT of TEXT rows past the cursor watermark "
      + "into staging LOAD DATA, then INSERT ... ON DUPLICATE KEY UPDATE. DATETIME is COPY-unsafe.",
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
