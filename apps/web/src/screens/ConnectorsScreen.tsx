import {
  Button,
  ConnectorCatalogBrowser,
  LoadingState,
  SavedConnectorRegistry,
  useToast,
} from "@dataflow/design-system";
import { useCallback, useEffect, useState } from "react";
import {
  deleteSavedConnector,
  fetchSavedConnectors,
  testSavedConnector,
  type SavedConnector,
} from "../lib/api";
import { useConnectorCatalog } from "../lib/transfer/ConnectorCatalogClient";

interface ScreenCtaProps {
  onNewTransfer?: () => void;
}

type ConnectorsTab = "catalog" | "saved";

export function ConnectorsScreen({ onNewTransfer }: ScreenCtaProps) {
  const { toast } = useToast();
  const [tab, setTab] = useState<ConnectorsTab>("catalog");
  const [saved, setSaved] = useState<SavedConnector[]>([]);
  const [savedLoading, setSavedLoading] = useState(true);
  const [testingId, setTestingId] = useState<string | null>(null);

  const catalog = useConnectorCatalog();

  const loadSaved = useCallback(async () => {
    setSavedLoading(true);
    try {
      setSaved(await fetchSavedConnectors());
    } catch {
      setSaved([]);
    } finally {
      setSavedLoading(false);
    }
  }, []);

  useEffect(() => {
    loadSaved();
  }, [loadSaved]);

  async function handleTest(id: string) {
    setTestingId(id);
    try {
      const result = await testSavedConnector(id);
      toast({
        title: result.ok ? "Verified" : "Failed",
        message: result.message ?? result.error ?? "",
        tone: result.ok ? "success" : "error",
      });
      await loadSaved();
    } finally {
      setTestingId(null);
    }
  }

  async function handleDelete(id: string) {
    await deleteSavedConnector(id);
    toast({ title: "Removed", tone: "success" });
    await loadSaved();
  }

  return (
    <>
      <div className="df-connectors-header">
        <div>
          <h1 className="df-connectors-title">Connectors</h1>
          <p className="df-page-desc">
            {catalog.catalogTotal || "620"}+ integrations — search the catalog or manage saved connections.
          </p>
        </div>
        {onNewTransfer && (
          <Button variant="primary" onClick={onNewTransfer}>
            New transfer
          </Button>
        )}
      </div>

      <div className="df-connectors-tabs" role="tablist">
        <button
          type="button"
          role="tab"
          aria-selected={tab === "catalog"}
          className={["df-connectors-tab", tab === "catalog" ? "df-connectors-tab--active" : ""].filter(Boolean).join(" ")}
          onClick={() => setTab("catalog")}
        >
          Catalog ({catalog.catalogTotal || "…"})
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={tab === "saved"}
          className={["df-connectors-tab", tab === "saved" ? "df-connectors-tab--active" : ""].filter(Boolean).join(" ")}
          onClick={() => setTab("saved")}
        >
          Saved ({saved.length})
        </button>
      </div>

      {tab === "catalog" && (
        <section className="df-section-surface df-section-surface--padded">
          {catalog.error && (
            <p className="df-alert df-alert--danger">{catalog.error}</p>
          )}
          <ConnectorCatalogBrowser
            connectors={catalog.items}
            total={catalog.total}
            catalogTotal={catalog.catalogTotal}
            liveCount={catalog.liveCount}
            categories={catalog.categories}
            query={catalog.query}
            category={catalog.category}
            loading={catalog.loading}
            onQueryChange={catalog.setQuery}
            onCategoryChange={catalog.setCategory}
            onLoadMore={catalog.loadMore}
            hasMore={catalog.hasMore}
          />
        </section>
      )}

      {tab === "saved" && (
        <section className="df-section-surface df-section-surface--padded">
          <div className="df-section-head">
            <span className="df-section-title">Your saved connectors</span>
            <Button variant="ghost" onClick={loadSaved} disabled={savedLoading}>
              Refresh
            </Button>
          </div>
          <p className="df-field-hint">Add connectors during transfer step 2, or paste credentials when connecting.</p>
          {savedLoading ? (
            <LoadingState label="Loading…" compact />
          ) : (
            <SavedConnectorRegistry
              connectors={saved}
              onTest={handleTest}
              onDelete={handleDelete}
              testingId={testingId}
            />
          )}
        </section>
      )}
    </>
  );
}
