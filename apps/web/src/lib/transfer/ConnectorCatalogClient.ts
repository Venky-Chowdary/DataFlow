import { useCallback, useEffect, useRef, useState } from "react";
import { fetchConnectorCatalog, type CatalogConnector } from "../api";

export interface ConnectorCatalogState {
  items: CatalogConnector[];
  total: number;
  catalogTotal: number;
  liveCount: number;
  categories: string[];
  loading: boolean;
  error: string | null;
  hasMore: boolean;
}

const PAGE_SIZE = 60;

/** Debounced, paginated connector catalog — single responsibility for 600+ search. */
export class ConnectorCatalogClient {
  private offset = 0;
  private query = "";
  private category = "";

  constructor(private readonly pageSize = PAGE_SIZE) {}

  async fetchPage(
    params: { q?: string; category?: string; reset?: boolean } = {}
  ): Promise<{
    items: CatalogConnector[];
    total: number;
    catalogTotal: number;
    liveCount: number;
    categories: string[];
    hasMore: boolean;
  }> {
    if (params.reset || params.q !== undefined || params.category !== undefined) {
      this.offset = 0;
      if (params.q !== undefined) this.query = params.q;
      if (params.category !== undefined) this.category = params.category;
    }

    const data = await fetchConnectorCatalog({
      q: this.query,
      category: this.category || undefined,
      offset: this.offset,
      limit: this.pageSize,
    });

    this.offset += data.connectors.length;

    return {
      items: data.connectors,
      total: data.total,
      catalogTotal: data.catalog_total,
      liveCount: data.live_count,
      categories: data.categories,
      hasMore: this.offset < data.total,
    };
  }
}

export function useConnectorCatalog() {
  const clientRef = useRef(new ConnectorCatalogClient());
  const [state, setState] = useState<ConnectorCatalogState>({
    items: [],
    total: 0,
    catalogTotal: 0,
    liveCount: 0,
    categories: [],
    loading: true,
    error: null,
    hasMore: false,
  });
  const [query, setQuery] = useState("");
  const [category, setCategory] = useState("");

  const load = useCallback(async (reset = true) => {
    setState((s) => ({ ...s, loading: true, error: null }));
    try {
      const page = await clientRef.current.fetchPage({ q: query, category, reset });
      setState((s) => ({
        items: reset ? page.items : [...s.items, ...page.items],
        total: page.total,
        catalogTotal: page.catalogTotal,
        liveCount: page.liveCount,
        categories: page.categories,
        loading: false,
        error: null,
        hasMore: page.hasMore,
      }));
    } catch (e) {
      setState((s) => ({
        ...s,
        loading: false,
        error: e instanceof Error ? e.message : "Catalog unavailable — start the API server",
      }));
    }
  }, [query, category]);

  useEffect(() => {
    const t = setTimeout(() => load(true), query ? 280 : 0);
    return () => clearTimeout(t);
  }, [query, category, load]);

  const loadMore = useCallback(() => {
    if (!state.hasMore || state.loading) return;
    load(false);
  }, [state.hasMore, state.loading, load]);

  return { ...state, query, setQuery, category, setCategory, loadMore, refresh: () => load(true) };
}
