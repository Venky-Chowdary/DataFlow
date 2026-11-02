/**
 * Connectors "last used" must join on saved-connector id, not table name.
 * Run: npx --yes tsx --test apps/web/src/lib/connectionWorkbench.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { jobsForConnector, lastUsedAtForConnector } from "./connectionWorkbench.js";
import type { Connector, TransferJob } from "./types.js";

function connector(partial: Partial<Connector> & Pick<Connector, "id" | "name" | "type">): Connector {
  return {
    host: "db.example",
    port: 3306,
    database: "railway",
    status: "healthy",
    created_at: "2026-01-01T00:00:00Z",
    ...partial,
  };
}

function job(partial: Partial<TransferJob> & Pick<TransferJob, "_id" | "source_name">): TransferJob {
  return {
    source_type: "database",
    destination_type: "snowflake",
    destination_database: "DATAFLOW",
    destination_collection: "AUDIT",
    status: "completed",
    records_processed: 10,
    created_at: "2026-08-01T00:00:00Z",
    ...partial,
  };
}

describe("last used joins on connector id", () => {
  it("matches jobs by source_connector_id even when source_name is the table", () => {
    const mysql = connector({ id: "mysql-venky", name: "MySQL venky2001", type: "mysql" });
    const jobs = [
      job({
        _id: "j1",
        source_name: "airports",
        source_connector_id: "mysql-venky",
        dest_connector_id: "sf-dest",
        created_at: "2026-08-15T12:00:00Z",
      }),
    ];
    assert.equal(jobsForConnector(mysql, jobs).length, 1);
    assert.equal(lastUsedAtForConnector(mysql, jobs), "2026-08-15T12:00:00Z");
  });

  it("does not treat a table name as the connector name", () => {
    const mysql = connector({ id: "mysql-venky", name: "MySQL venky2001", type: "mysql" });
    const jobs = [job({ _id: "j1", source_name: "airports", source_type: "database" })];
    assert.equal(jobsForConnector(mysql, jobs).length, 0);
    assert.equal(lastUsedAtForConnector(mysql, jobs), null);
  });

  it("prefers the stamped last_used_at when no job ids are present", () => {
    const mysql = connector({
      id: "mysql-venky",
      name: "MySQL venky2001",
      type: "mysql",
      last_used_at: "2026-08-14T09:00:00Z",
    });
    assert.equal(lastUsedAtForConnector(mysql, []), "2026-08-14T09:00:00Z");
  });

  it("picks the newest related job when the list is unsorted", () => {
    const sf = connector({ id: "sf-dest", name: "SnowFlake Dest", type: "snowflake" });
    const jobs = [
      job({
        _id: "old",
        source_name: "airports",
        dest_connector_id: "sf-dest",
        created_at: "2026-07-01T00:00:00Z",
      }),
      job({
        _id: "new",
        source_name: "airports",
        dest_connector_id: "sf-dest",
        created_at: "2026-08-15T18:00:00Z",
      }),
    ];
    assert.equal(lastUsedAtForConnector(sf, jobs), "2026-08-15T18:00:00Z");
    assert.equal(jobsForConnector(sf, jobs)[0]?._id, "new");
  });
});
