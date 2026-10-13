import { useCallback, useRef, type Dispatch, type SetStateAction } from "react";
import type { SourceKind } from "../ui/SourceKindTiles";
import type { SourceReadMode } from "../../lib/sourceReadMode";
import { appendSourceObject } from "../../lib/sourceObjectList";
import { TransferSchemaBrowser } from "./TransferSchemaBrowser";

/** SQL editor insert handle. The schema browser writes through the same ref. */
export function useSqlInsertTarget(): {
  bindSqlInsert: (insert: (text: string) => void) => void;
  insertSql: (text: string) => void;
} {
  const sqlInsertRef = useRef<((text: string) => void) | null>(null);
  const bindSqlInsert = useCallback((insert: (text: string) => void) => {
    sqlInsertRef.current = insert;
  }, []);
  const insertSql = useCallback((text: string) => {
    sqlInsertRef.current?.(text);
  }, []);
  return { bindSqlInsert, insertSql };
}

/**
 * Flagged schema browser beside the source combobox.
 * Off unless the studio flag, a database source, and a connector are all set.
 */
export function TransferSourceSchemaPanel({
  schemaBrowser,
  sourceKind,
  connectorId,
  connectorType,
  readMode,
  setSourceTable,
  setSourceCollection,
  onInsertText,
}: {
  schemaBrowser: boolean;
  sourceKind: SourceKind;
  connectorId: string;
  connectorType?: string;
  readMode: SourceReadMode;
  setSourceTable: Dispatch<SetStateAction<string>>;
  setSourceCollection: Dispatch<SetStateAction<string>>;
  onInsertText: (text: string) => void;
}) {
  if (!schemaBrowser || sourceKind !== "database" || !connectorId) return null;
  const mode = readMode === "query" ? "query" : readMode === "procedure" ? "procedure" : "table";
  const onUseObject = (name: string) => {
    const apply = (current: string) => appendSourceObject(current, name);
    if (connectorType === "mongodb") setSourceCollection(apply);
    else setSourceTable(apply);
  };
  return (
    <TransferSchemaBrowser
      connectorId={connectorId}
      mode={mode}
      onUseObject={onUseObject}
      onInsertText={onInsertText}
    />
  );
}
