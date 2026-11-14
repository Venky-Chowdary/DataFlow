/**
 * Export-option state for the Studio file_export destination and the Query
 * console export (G7-F2 / G7-F3), as hooks so the defaults and the payloads
 * they produce are tested on behaviour, not on page source.
 */
import { useCallback, useState } from "react";
import {
  FILE_EXPORT_FORMULA_PROTECTION_DEFAULT,
  TSV_NULL_DEFAULT,
  type TsvNull,
  fileExportDestExtra,
  fileExportEndpoint,
  objectStoreDestExtra,
} from "./fileExportDestination";

export interface FileExportOptionsState {
  formulaProtection: boolean;
  setFormulaProtection: (next: boolean) => void;
  tsvNull: TsvNull;
  setTsvNull: (next: TsvNull) => void;
  /** New transfer: protection back on, NULL marker back to the default. */
  reset: () => void;
  /** Reopen a saved job: exactly what it stored (absent protection = off). */
  restore: (saved: { formulaProtection: boolean; tsvNull: TsvNull }) => void;
  endpoint: (dest: { format: string; database: string; outputPath: string }) => Record<string, unknown>;
  destExtra: (format: string) => Record<string, unknown>;
  objectStoreExtra: (dest: { type: string; objectName: string }) => Record<string, unknown>;
}

/** Studio: protection ON for a new job, a saved job keeps what it stored. */
export function useFileExportOptions(): FileExportOptionsState {
  const [formulaProtection, setFormulaProtection] = useState(FILE_EXPORT_FORMULA_PROTECTION_DEFAULT);
  const [tsvNull, setTsvNull] = useState<TsvNull>(TSV_NULL_DEFAULT);
  const reset = useCallback(() => {
    setFormulaProtection(FILE_EXPORT_FORMULA_PROTECTION_DEFAULT);
    setTsvNull(TSV_NULL_DEFAULT);
  }, []);
  const restore = useCallback((saved: { formulaProtection: boolean; tsvNull: TsvNull }) => {
    setFormulaProtection(saved.formulaProtection);
    setTsvNull(saved.tsvNull);
  }, []);
  return {
    formulaProtection,
    setFormulaProtection,
    tsvNull,
    setTsvNull,
    reset,
    restore,
    endpoint: (dest) => fileExportEndpoint({ ...dest, formulaProtection, tsvNull }),
    destExtra: (format) => fileExportDestExtra({ format, formulaProtection, tsvNull }),
    objectStoreExtra: (dest) => objectStoreDestExtra({ ...dest, tsvNull }),
  };
}

export interface QueryExportOptionsState {
  formulaProtection: boolean;
  setFormulaProtection: (next: boolean) => void;
  tsvNull: TsvNull;
  setTsvNull: (next: TsvNull) => void;
  /** The writer options a Query console file export sends. */
  payload: (format: string) => { formula_protection: "prefix" | "off"; tsv_null: TsvNull };
}

/** Query console: protection ON by default; it also governs Copy as CSV. */
export function useQueryExportOptions(): QueryExportOptionsState {
  const [formulaProtection, setFormulaProtection] = useState(true);
  const [tsvNull, setTsvNull] = useState<TsvNull>(TSV_NULL_DEFAULT);
  return {
    formulaProtection,
    setFormulaProtection,
    tsvNull,
    setTsvNull,
    payload: (format) => ({
      formula_protection: formulaProtection ? "prefix" : "off",
      tsv_null: String(format).toLowerCase() === "tsv" ? tsvNull : TSV_NULL_DEFAULT,
    }),
  };
}
