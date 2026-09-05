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
  });
});
