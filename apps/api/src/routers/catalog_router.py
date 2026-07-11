"""Connector catalog API — Airbyte-style searchable sources & destinations."""

from fastapi import APIRouter, Query

router = APIRouter(prefix="/catalog", tags=["Connector Catalog"])


@router.get("/connectors")
async def list_catalog_connectors(
    q: str = Query("", description="Search connectors"),
    role: str = Query("all", description="source | destination | all"),
    category: str = Query(""),
    status: str = Query(""),
    limit: int = Query(60, le=200),
    transfer_only: bool = Query(False, description="Only connectors with full transfer support"),
):
    from ..services.catalog_service import search_catalog
    return search_catalog(q, role, category, status, limit, transfer_only=transfer_only)


@router.get("/stats")
async def catalog_stats():
    from ..services.catalog_service import catalog_summary
    return catalog_summary()


@router.get("/connectors/{connector_id}")
async def get_catalog_connector(connector_id: str):
    from ..services.catalog_service import get_connector_by_id
    c = get_connector_by_id(connector_id)
    if not c:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Connector not found")
    return c


@router.get("/suggested/sources")
async def suggested_sources():
    from ..services.catalog_service import search_catalog
    return search_catalog(role="source", limit=16)


@router.get("/suggested/destinations")
async def suggested_destinations():
    from ..services.catalog_service import search_catalog
    return search_catalog(role="destination", limit=16)
