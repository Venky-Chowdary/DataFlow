"""A REST catalog server cannot redirect the catalog's requests off its own origin (follow-up review 3, M2).

pyiceberg's ``RestCatalog`` talks to the server through a ``requests.Session``
that follows redirects. ``iceberg_server_hosts.check_server_oauth_uri`` keeps
the OAuth2 token request on the catalog's origin, but a ``307`` / ``308``
answer to that POST re-sent its body (the connector's client secret) to any
host, and every catalog GET / POST followed a redirect to a host the
connector never named (cloud metadata, a private network, another tenant's
service) with the session's ``Authorization`` header rebuilt by the auth
adapter.

:func:`guard_session` adds a response hook to the session (``requests`` runs
it on every response before following a redirect, and copies it onto each
redirected request): a redirect whose target is not the catalog's own origin
(scheme, host and port of the connector's URI) is refused. A redirect on the
same origin is followed as before.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urljoin

from connectors.iceberg_server_hosts import _origin

_MESSAGE = (
    "The Iceberg REST catalog server redirected a request to another address (a different "
    "scheme, host or port than the catalog's own). Datawrap does not follow a catalog server "
    "off its own address: the connector's credential and requests go only to the catalog URI."
)


class CatalogRedirectRefused(ValueError):
    """A REST catalog response redirected off the catalog's origin."""


def same_origin_redirect_hook(catalog_uri: Any):
    """A ``requests`` response hook refusing a redirect off ``catalog_uri``'s origin."""
    origin = _origin(catalog_uri)

    def _hook(resp: Any, *_args: Any, **_kwargs: Any) -> Any:
        if getattr(resp, "is_redirect", False):
            target = urljoin(str(getattr(resp, "url", "") or ""), str(resp.headers.get("Location") or ""))
            if origin is None or _origin(target) != origin:
                raise CatalogRedirectRefused(_MESSAGE)
        return resp

    return _hook


def guard_session(session: Any, catalog_uri: Any) -> None:
    """Install the same-origin redirect check on a catalog's ``requests.Session``."""
    session.hooks.setdefault("response", []).append(same_origin_redirect_hook(catalog_uri))
