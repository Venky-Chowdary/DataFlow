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
