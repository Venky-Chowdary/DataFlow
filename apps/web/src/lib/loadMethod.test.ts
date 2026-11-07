/**
 * Run: npx --yes tsx --test apps/web/src/lib/loadMethod.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { loadMethodDescription, loadMethodLabel } from "./loadMethod.js";

describe("loadMethod labels", () => {
  it("names identity COPY and COPY upsert without raw tokens", () => {
    assert.equal(loadMethodLabel("copy_binary_server_to_server"), "Server-to-server COPY");
    assert.match(loadMethodLabel("copy_text_pg_to_mysql_load_data"), /LOAD DATA/);
    assert.match(loadMethodLabel("copy_text_pg_to_mysql_load_data_upsert"), /upsert/i);
    assert.match(loadMethodDescription("copy_text_pg_to_mysql_load_data_upsert"), /staging/);
    assert.match(
      loadMethodLabel("copy_text_pg_to_mysql_load_data_incremental_deduped"),
      /incremental/i,
    );
    assert.match(
      loadMethodDescription("copy_binary_server_to_server_incremental_append"),
      /watermark/,
    );
    assert.match(
      loadMethodLabel("copy_text_mysql_to_pg_stdin_incremental_deduped"),
      /incremental/i,
    );
    assert.match(
      loadMethodLabel("insert_select_mysql_same_instance_incremental_append"),
      /INSERT SELECT/i,
    );
    assert.match(
      loadMethodLabel("attach_insert_select_sqlite_incremental_deduped"),
      /incremental/i,
    );
    assert.match(
      loadMethodDescription("attach_insert_select_sqlite_incremental_append"),
      /watermark/,
    );
    assert.match(
      loadMethodLabel("copy_text_pg_executemany_sqlite_incremental_append"),
      /incremental/i,
    );
    assert.match(
      loadMethodDescription("copy_text_pg_executemany_sqlite_incremental_deduped"),
      /DATE/,
    );
    assert.match(
      loadMethodLabel("select_mysql_executemany_sqlite_incremental_deduped"),
      /incremental/i,
    );
    assert.match(
      loadMethodDescription("select_mysql_executemany_sqlite_incremental_append"),
      /DATETIME/,
    );
    assert.match(
      loadMethodLabel("select_sqlite_copy_from_stdin_pg_incremental_deduped"),
      /incremental/i,
    );
    assert.match(
      loadMethodDescription("select_sqlite_copy_from_stdin_pg_incremental_append"),
      /TEXT/,
    );
    assert.match(
      loadMethodLabel("select_sqlite_load_data_mysql_incremental_append"),
      /incremental/i,
    );
    assert.match(
      loadMethodDescription("select_sqlite_load_data_mysql_incremental_deduped"),
      /DATETIME/,
    );
    assert.match(loadMethodLabel("csv_executemany_sqlite_incremental_deduped"), /CSV/);
    assert.match(
      loadMethodDescription("csv_executemany_sqlite_incremental_append"),
      /watermark/,
    );
    assert.match(loadMethodLabel("csv_copy_from_stdin_pg_incremental_deduped"), /incremental/i);
    assert.match(
      loadMethodDescription("csv_load_data_mysql_incremental_append"),
      /LOAD DATA/,
    );
    assert.match(loadMethodLabel("yaml_records_executemany_sqlite_incremental_deduped"), /YAML/);
    assert.match(loadMethodLabel("json_records_copy_from_stdin_pg_incremental_deduped"), /JSON/);
    assert.match(loadMethodLabel("fwf_records_load_data_mysql_incremental_append"), /Fixed-width/);
    assert.match(loadMethodLabel("excel_records_executemany_sqlite_incremental_deduped"), /Excel/);
    assert.match(loadMethodLabel("xml_records_copy_from_stdin_pg_incremental_append"), /XML/);
    assert.match(loadMethodLabel("parquet_records_copy_from_stdin_pg_incremental_deduped"), /Parquet/);
    assert.match(loadMethodLabel("avro_records_executemany_sqlite_incremental_append"), /Avro/);
    assert.match(loadMethodLabel("orc_records_load_data_mysql_incremental_deduped"), /ORC/);
    assert.match(
      loadMethodDescription("yaml_records_executemany_sqlite_incremental_append"),
      /watermark/,
    );
  });
});
