"""SCIM 2.0 User and Group endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Request, Response
from fastapi.routing import APIRoute
from fastapi.responses import JSONResponse

from services import scim_service
from services.workspace_access import actor_email


class ScimJSONResponse(JSONResponse):
    media_type = "application/scim+json"


class ScimRoute(APIRoute):
    def get_route_handler(self):
        original = super().get_route_handler()

        async def handler(request: Request):
            try:
                return await original(request)
            except scim_service.ScimError as exc:
                return ScimJSONResponse(
                    status_code=exc.status,
                    content=exc.to_dict(),
                )

        return handler


def _page_value(value: str, name: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise scim_service.ScimError(400, "invalidValue", f"{name} must be an integer") from exc


router = APIRouter(prefix="/scim/v2", tags=["SCIM"], route_class=ScimRoute)
_CORE = "urn:ietf:params:scim:schemas:core:2.0:"
_RESOURCE_TYPE_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:ResourceType"
_LIST_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:ListResponse"


@router.get("/ServiceProviderConfig", response_class=ScimJSONResponse)
def service_provider_config():
    return {
        "schemas": [_CORE + "ServiceProviderConfig"],
        "patch": {"supported": True},
        "bulk": {"supported": False, "maxOperations": 0, "maxPayloadSize": 0},
        "filter": {"supported": True, "maxResults": 200},
        "changePassword": {"supported": False},
        "sort": {"supported": False},
        "etag": {"supported": False},
        "authenticationSchemes": [
            {
                "type": "oauthbearertoken",
                "name": "OAuth Bearer Token",
                "description": "Bearer token provisioned by an administrator",
                "specUri": "https://www.rfc-editor.org/rfc/rfc6750",
                "primary": True,
            }
        ],
    }


@router.get("/ResourceTypes", response_class=ScimJSONResponse)
def resource_types():
    return {
        "schemas": [_LIST_SCHEMA],
        "totalResults": 2,
        "startIndex": 1,
        "itemsPerPage": 2,
        "Resources": [
            {
                "schemas": [_RESOURCE_TYPE_SCHEMA],
                "id": "User",
                "name": "User",
                "endpoint": "/Users",
                "schema": _CORE + "User",
            },
            {
                "schemas": [_RESOURCE_TYPE_SCHEMA],
                "id": "Group",
                "name": "Group",
                "endpoint": "/Groups",
                "schema": _CORE + "Group",
            },
        ],
    }


@router.get("/Schemas", response_class=ScimJSONResponse)
def schemas():
    return {
        "schemas": [_LIST_SCHEMA],
        "totalResults": 2,
        "startIndex": 1,
        "itemsPerPage": 2,
        "Resources": [
            {
                "schemas": [_CORE + "Schema"],
                "id": _CORE + "User",
                "name": "User",
                "attributes": [
                    {"name": "userName", "type": "string", "required": True, "mutability": "readWrite"},
                    {"name": "externalId", "type": "string", "required": False},
                    {"name": "displayName", "type": "string", "required": False},
                    {"name": "active", "type": "boolean", "required": False},
                    {"name": "emails", "type": "complex", "multiValued": True},
                ],
            },
            {
                "schemas": [_CORE + "Schema"],
                "id": _CORE + "Group",
                "name": "Group",
                "attributes": [
                    {"name": "displayName", "type": "string", "required": True},
                    {"name": "externalId", "type": "string", "required": False},
                    {"name": "members", "type": "complex", "multiValued": True},
                ],
            },
        ],
    }


@router.post("/Users", status_code=201, response_class=ScimJSONResponse)
def create_scim_user(request: Request, payload: dict[str, Any] = Body(...)):
    user = scim_service.create_user(payload, actor=actor_email(request))
    location = request.url_for("get_scim_user", user_id=user["id"])
    return ScimJSONResponse(
        status_code=201,
        content=user,
        headers={"Location": str(location)},
    )


@router.get("/Users", response_class=ScimJSONResponse)
def list_scim_users(
    filter: str | None = None,
    startIndex: str = "1",
    count: str = "100",
):
    return scim_service.list_users(
        filter=filter,
        start_index=_page_value(startIndex, "startIndex"),
        count=_page_value(count, "count"),
    )


@router.get("/Users/{user_id}", name="get_scim_user", response_class=ScimJSONResponse)
def get_scim_user(user_id: str):
    return scim_service.get_user(user_id)


@router.put("/Users/{user_id}", response_class=ScimJSONResponse)
def replace_scim_user(
    user_id: str,
    request: Request,
    payload: dict[str, Any] = Body(...),
):
    return scim_service.replace_user(user_id, payload, actor=actor_email(request))


@router.patch("/Users/{user_id}", response_class=ScimJSONResponse)
def patch_scim_user(
    user_id: str,
    request: Request,
    payload: dict[str, Any] = Body(...),
):
    return scim_service.patch_user(user_id, payload, actor=actor_email(request))


@router.delete("/Users/{user_id}", status_code=204)
def delete_scim_user(user_id: str, request: Request):
    scim_service.delete_user(user_id, actor=actor_email(request))
    return Response(status_code=204)


@router.post("/Groups", status_code=201, response_class=ScimJSONResponse)
def create_scim_group(request: Request, payload: dict[str, Any] = Body(...)):
    group = scim_service.create_group(payload, actor=actor_email(request))
    location = request.url_for("get_scim_group", group_id=group["id"])
    return ScimJSONResponse(
        status_code=201,
        content=group,
        headers={"Location": str(location)},
    )


@router.get("/Groups", response_class=ScimJSONResponse)
def list_scim_groups(
    filter: str | None = None,
    startIndex: str = "1",
    count: str = "100",
):
    return scim_service.list_groups(
        filter=filter,
        start_index=_page_value(startIndex, "startIndex"),
        count=_page_value(count, "count"),
    )


@router.get("/Groups/{group_id}", name="get_scim_group", response_class=ScimJSONResponse)
def get_scim_group(group_id: str):
    return scim_service.get_group(group_id)


@router.put("/Groups/{group_id}", response_class=ScimJSONResponse)
def replace_scim_group(
    group_id: str,
    request: Request,
    payload: dict[str, Any] = Body(...),
):
    return scim_service.replace_group(group_id, payload, actor=actor_email(request))


@router.patch("/Groups/{group_id}", response_class=ScimJSONResponse)
def patch_scim_group(
    group_id: str,
    request: Request,
    payload: dict[str, Any] = Body(...),
):
    return scim_service.patch_group(group_id, payload, actor=actor_email(request))


@router.delete("/Groups/{group_id}", status_code=204)
def delete_scim_group(group_id: str, request: Request):
    scim_service.delete_group(group_id, actor=actor_email(request))
    return Response(status_code=204)
