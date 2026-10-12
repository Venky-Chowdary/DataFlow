import { useCallback, useEffect, useState } from "react";
import {
  fetchQuerySchema,
  fetchQuerySchemas,
  type QueryCatalogNodeInfo,
} from "../../lib/api";
import { filterCatalogNodes, firstExpanded, mergeExpandedObject, toSchemaObject } from "../../lib/querySchema";
import { querySchemaErrorCopy } from "../../lib/queryWorkspace";
import type { SchemaObject } from "../../lib/sqlIntel";
import { SchemaBrowser } from "../query/SchemaBrowser";

interface TransferSchemaBrowserProps {
  connectorId: string;
  /** Table mode writes the object name. Query mode inserts it into the editor. */
  mode: "table" | "query" | "procedure";
  onUseObject: (name: string) => void;
  onInsertText: (text: string) => void;
}

/**
 * Database → schema → the shared object tree, beside the source combobox.
 * The combobox stays for typed names. This browser does not invent a second one.
 */
export function TransferSchemaBrowser({
  connectorId,
  mode,
  onUseObject,
  onInsertText,
}: TransferSchemaBrowserProps) {
  const [nodes, setNodes] = useState<QueryCatalogNodeInfo[]>([]);
  const [catalogMessage, setCatalogMessage] = useState("");
  const [schemaName, setSchemaName] = useState("");
  const [objects, setObjects] = useState<SchemaObject[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [pending, setPending] = useState<string[]>([]);
  const [connected, setConnected] = useState<boolean | undefined>(undefined);
  const [typeSource, setTypeSource] = useState("");
  const [warnings, setWarnings] = useState<string[]>([]);
  const [truncated, setTruncated] = useState(false);
  const [filter, setFilter] = useState("");

  const load = useCallback(async (id: string) => {
    if (!id) {
      setNodes([]);
      setObjects([]);
      setConnected(undefined);
      setError("");
      return;
    }
    setLoading(true);
    setError("");
    try {
      const catalog = await fetchQuerySchemas({ connector_id: id });
      setNodes(catalog.nodes ?? []);
      setCatalogMessage(catalog.message ?? "");
      const firstSchema = firstSchemaName(catalog.nodes ?? []);
      setSchemaName(firstSchema);
      const data = await fetchQuerySchema({
        connector_id: id,
        schema_name: firstSchema || undefined,
      });
      setObjects(data.objects.map(toSchemaObject));
      setConnected(data.connected);
      setTypeSource(data.type_source ?? "");
      setWarnings(data.warnings ?? []);
      setTruncated(data.objects_truncated === true);
    } catch (err) {
      setObjects([]);
      setNodes([]);
      setConnected(false);
      setError(querySchemaErrorCopy((err as Error).message || "Schema introspection failed"));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load(connectorId);
  }, [connectorId, load]);

  const selectSchema = async (name: string) => {
    setSchemaName(name);
    setLoading(true);
    setError("");
    try {
      const data = await fetchQuerySchema({
        connector_id: connectorId,
        schema_name: name || undefined,
      });
      setObjects(data.objects.map(toSchemaObject));
      setTruncated(data.objects_truncated === true);
      setWarnings(data.warnings ?? []);
      setConnected(data.connected);
    } catch (err) {
      setError(querySchemaErrorCopy((err as Error).message || "Schema introspection failed"));
    } finally {
      setLoading(false);
    }
  };

  const expand = async (objectName: string) => {
    setPending((current) => [...current, objectName]);
    try {
      const data = await fetchQuerySchema({
        connector_id: connectorId,
        schema_name: schemaName || undefined,
        object_name: objectName,
      });
      setObjects((prev) => mergeExpandedObject(prev, objectName, firstExpanded(data.objects)));
      if (data.warnings?.length) setWarnings(data.warnings);
    } catch (err) {
      setError(querySchemaErrorCopy((err as Error).message || "Could not load columns"));
    } finally {
      setPending((current) => current.filter((name) => name !== objectName));
    }
  };

  const useObject = (name: string) => {
    if (mode === "table") onUseObject(name);
    else onInsertText(name);
  };

  const shownNodes = filterCatalogNodes(nodes, filter);

  return (
    <div className="df2-studio-next df2-transfer-schema" data-testid="studio-schema-browser">
      <div className="df2-transfer-schema-catalog">
        <label className="df2-label" htmlFor="transfer-schema-filter">Databases and schemas</label>
        <input
          id="transfer-schema-filter"
          className="df2-input df2-input-sm"
          value={filter}
          onChange={(event) => setFilter(event.target.value)}
          placeholder="Filter databases and schemas…"
          aria-label="Filter databases and schemas"
        />
        {catalogMessage && <p className="df2-label-hint">{catalogMessage}</p>}
        <ul className="df2-transfer-schema-nodes">
          {shownNodes.map((node) => (
            <CatalogNode
              key={`${node.kind}:${node.name}`}
              node={node}
              selected={schemaName}
              onSelect={(name) => void selectSchema(name)}
            />
          ))}
        </ul>
      </div>
      <SchemaBrowser
        density="studio"
        objects={objects}
        loading={loading}
        error={error}
        pending={pending}
        connected={connected}
        typeSource={typeSource}
        warnings={warnings}
        objectsTruncated={truncated}
        onExpand={(name) => void expand(name)}
        onRefresh={() => void load(connectorId)}
        onInsert={onInsertText}
        onPreview={(object) => useObject(object.name)}
        onUseObject={useObject}
      />
    </div>
  );
}

function firstSchemaName(nodes: QueryCatalogNodeInfo[]): string {
  for (const node of nodes) {
    if (node.kind === "schema" && node.name) return node.name;
    const nested = firstSchemaName(node.children ?? []);
    if (nested) return nested;
  }
  return "";
}

function CatalogNode({
  node,
  selected,
  onSelect,
}: {
  node: QueryCatalogNodeInfo;
  selected: string;
  onSelect: (name: string) => void;
}) {
  return (
    <li>
      {node.kind === "schema" ? (
        <button
          type="button"
          className={`df2-transfer-schema-node${selected === node.name ? " is-selected" : ""}`}
          aria-pressed={selected === node.name}
          onClick={() => onSelect(node.name)}
        >
          {node.name}
        </button>
      ) : (
        <span className="df2-transfer-schema-db">{node.name}</span>
      )}
      {(node.children ?? []).length > 0 && (
        <ul>
          {(node.children ?? []).map((child) => (
            <CatalogNode
              key={`${child.kind}:${child.name}`}
              node={child}
              selected={selected}
              onSelect={onSelect}
            />
          ))}
        </ul>
      )}
    </li>
  );
}
