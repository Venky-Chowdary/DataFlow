"""Last-``@`` URL authority parse — passwords may contain ``@``.

stdlib ``urlparse`` and ``urllib.parse.urlsplit`` split userinfo on the
*first* ``@``. A Railway / Atlas / Snowflake-style paste
``scheme://user:p@ss@host/db`` then yields host ``ss@host`` and a wrong
password. Every connector that accepts a login URL must use this helper
instead of passing the raw string into a driver that is keyword-only or
that re-parses with first-``@`` rules.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import quote, unquote


@dataclass(frozen=True)
class UrlAuthority:
    scheme: str = ""
    user: str = ""
    password: str = ""
    host: str = ""
    port: int = 0
    path: str = ""
    query: str = ""
    fragment: str = ""


def _decode(value: str) -> str:
    if not value:
        return ""
    try:
        return unquote(value)
    except Exception:
        return value


def _split_host_port_path(after: str) -> tuple[str, int, str]:
    rest = after or ""
    if rest.startswith("["):
        end = rest.find("]")
        if end < 0:
            return rest, 0, ""
        host = rest[1:end]
        tail = rest[end + 1 :]
        port = 0
        path = ""
        if tail.startswith(":"):
            port_s, _, path_rest = tail[1:].partition("/")
            if port_s.isdigit():
                port = int(port_s)
                path = f"/{path_rest}" if path_rest or tail.endswith("/") else ""
            else:
                path = tail
        elif tail.startswith("/"):
            path = tail
        return host, port, path

    hostport, sep, path_rest = rest.partition("/")
    path = f"/{path_rest}" if sep else ""
    if hostport.count(":") == 1:
        host, port_s = hostport.rsplit(":", 1)
        if port_s.isdigit():
            return host, int(port_s), path
    return hostport, 0, path


def parse_url_authority(raw: str) -> UrlAuthority:
    """Parse ``scheme://user:pass@host:port/path?query`` using the last ``@``."""
    text = (raw or "").strip()
    if not text:
        return UrlAuthority()
    if text.lower().startswith("jdbc:"):
        text = text[5:].lstrip()

    fragment = ""
    if "#" in text:
        text, fragment = text.split("#", 1)

    query = ""
    if "?" in text:
        text, query = text.split("?", 1)

    if "://" in text:
        scheme, rest = text.split("://", 1)
    else:
        scheme, rest = "", text

    userinfo = ""
    if "@" in rest:
        userinfo, rest = rest.rsplit("@", 1)

    host, port, path = _split_host_port_path(rest)
    user = ""
    password = ""
    if userinfo:
        if ":" in userinfo:
            user, password = userinfo.split(":", 1)
            user = _decode(user)
            password = _decode(password)
        else:
            user = _decode(userinfo)

    return UrlAuthority(
        scheme=scheme,
        user=user,
        password=password,
        host=_decode(host),
        port=port,
        path=path,
        query=query,
        fragment=fragment,
    )


def http_service_base_url(host: str, port: int, ssl: bool, default_port: int) -> str:
    """``scheme://host:port`` for an HTTP service; a scheme typed into ``host`` wins.

    Operators paste ``http://host`` into the host field for HTTP engines; the
    old per-driver f-string dialed ``http://http://host:port``.
    """
    raw = (host or "").strip().rstrip("/")
    scheme = "https" if ssl else "http"
    if "://" in raw:
        url = parse_url_authority(raw)
        scheme = (url.scheme or scheme).lower()
        raw, port = url.host, url.port or port
    return f"{scheme}://{raw or 'localhost'}:{port or default_port}"


def looks_like_userinfo_host(raw: str) -> bool:
    """Scheme-less ``user:pass@host:port/db`` (Railway paste without scheme)."""
    text = (raw or "").strip()
    if not text or "://" in text or " " in text:
        return False
    if "@" not in text or ":" not in text.split("@", 1)[0]:
        return False
    parsed = parse_url_authority(text)
    return bool(parsed.host and parsed.user)


def rebuild_url(
    auth: UrlAuthority,
    *,
    user: str | None = None,
    password: str | None = None,
    scheme: str | None = None,
) -> str:
    """Rebuild a URL with percent-encoded userinfo so drivers parse it safely."""
    scheme_out = (scheme if scheme is not None else auth.scheme) or ""
    user_out = auth.user if user is None else user
    password_out = auth.password if password is None else password
    host = auth.host
    if host and ":" in host and not host.startswith("["):
        host = f"[{host}]"
    loc = host
    if auth.port:
        loc = f"{host}:{auth.port}"
    if user_out or password_out:
        encoded_user = quote(user_out or "", safe="")
        if password_out:
            loc = f"{encoded_user}:{quote(password_out, safe='')}@{loc}"
        elif user_out:
            loc = f"{encoded_user}@{loc}"
    path = auth.path or ""
    if path and not path.startswith("/"):
        path = f"/{path}"
    if scheme_out:
        url = f"{scheme_out}://{loc}{path}"
    else:
        url = f"{loc}{path}"
    if auth.query:
        url = f"{url}?{auth.query}"
    if auth.fragment:
        url = f"{url}#{auth.fragment}"
    return url


_HOST_SEGMENT_KEYS = ("server", "data source", "address", "addr", "host", "hostname")


def _connection_string_segments(raw: str) -> dict[str, str]:
    """``Key=Value;`` segments, lower-cased keys; a braced ODBC value may hold ``;``."""
    import re

    out: dict[str, str] = {}
    for m in re.finditer(r"(?:^|;)\s*([A-Za-z][A-Za-z0-9 _.-]*?)\s*=\s*(\{[^}]*\}|[^;]*)", raw):
        value = m.group(2).strip()
        if value.startswith("{") and value.endswith("}"):
            value = value[1:-1]
        out.setdefault(m.group(1).strip().lower(), value)
    return out


def connection_string_endpoint(raw: str) -> tuple[str, int] | None:
    """The ``(host, port)`` a connection string dials, or None when it names none.

    A URL dials its authority. A ``Key=Value;`` string (Azure storage, ODBC,
    ADO.NET) is not a URL: read as one, the whole string — AccountKey and Pwd
    included — came back as the "host" and was echoed on every connector
    brief (D264). Only a host the string actually names is ever returned.
    """
    text = (raw or "").strip()
    if not text:
        return None
    if "://" in text.split(";", 1)[0]:
        url = parse_url_authority(text)
        return (url.host, url.port) if url.host else None
    seg = _connection_string_segments(text)
    if not seg:
        return None
    blob = seg.get("blobendpoint") or seg.get("dfsendpoint")
    if blob:
        url = parse_url_authority(blob)
        return (url.host, url.port) if url.host else None
    account = seg.get("accountname")
    if account:
        suffix = seg.get("endpointsuffix") or "core.windows.net"
        return f"{account}.blob.{suffix}", 0
    for key in _HOST_SEGMENT_KEYS:
        value = seg.get(key)
        if not value:
            continue
        value = value.split(":", 1)[1] if value.lower().startswith(("tcp:", "np:", "lpc:")) else value
        host, _, port_s = value.partition(",")
        host = host.strip()
        port = int(port_s.strip()) if port_s.strip().isdigit() else 0
        if not port and host.count(":") == 1:
            h, p = host.rsplit(":", 1)
            if p.isdigit():
                host, port = h, int(p)
        return (host, port) if host else None
    return None
