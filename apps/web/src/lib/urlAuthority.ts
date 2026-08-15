/**
 * Last-@ URL authority parse — passwords may contain @.
 * Mirrors apps/api/connectors/url_authority.py.
 */

export interface UrlAuthority {
  scheme: string;
  user: string;
  password: string;
  host: string;
  port: number;
  path: string;
  query: string;
}

function decodePart(value: string): string {
  if (!value) return "";
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function splitHostPortPath(after: string): { host: string; port: number; path: string } {
  const rest = after || "";
  if (rest.startsWith("[")) {
    const end = rest.indexOf("]");
    if (end < 0) return { host: rest, port: 0, path: "" };
    const host = rest.slice(1, end);
    const tail = rest.slice(end + 1);
    if (tail.startsWith(":")) {
      const [portS, ...pathParts] = tail.slice(1).split("/");
      if (/^\d+$/.test(portS)) {
        return { host, port: parseInt(portS, 10), path: pathParts.length ? `/${pathParts.join("/")}` : "" };
      }
    }
    if (tail.startsWith("/")) return { host, port: 0, path: tail };
    return { host, port: 0, path: "" };
  }
  const slash = rest.indexOf("/");
  const hostport = slash >= 0 ? rest.slice(0, slash) : rest;
  const path = slash >= 0 ? rest.slice(slash) : "";
  if ((hostport.match(/:/g) || []).length === 1) {
    const [host, portS] = hostport.split(":");
    if (/^\d+$/.test(portS)) return { host, port: parseInt(portS, 10), path };
  }
  return { host: hostport, port: 0, path };
}

export function parseUrlAuthority(raw: string): UrlAuthority {
  let text = (raw || "").trim();
  if (!text) return { scheme: "", user: "", password: "", host: "", port: 0, path: "", query: "" };
  if (text.toLowerCase().startsWith("jdbc:")) text = text.slice(5).trim();

  if (text.includes("#")) text = text.slice(0, text.indexOf("#"));
  let query = "";
  const qIdx = text.indexOf("?");
  if (qIdx >= 0) {
    query = text.slice(qIdx + 1);
    text = text.slice(0, qIdx);
  }

  let scheme = "";
  let rest = text;
  if (text.includes("://")) {
    const parts = text.split("://");
    scheme = parts[0];
    rest = parts.slice(1).join("://");
  }

  let userinfo = "";
  const at = rest.lastIndexOf("@");
  if (at >= 0) {
    userinfo = rest.slice(0, at);
    rest = rest.slice(at + 1);
  }

  const { host, port, path } = splitHostPortPath(rest);
  let user = "";
  let password = "";
  if (userinfo) {
    const colon = userinfo.indexOf(":");
    if (colon >= 0) {
      user = decodePart(userinfo.slice(0, colon));
      password = decodePart(userinfo.slice(colon + 1));
    } else {
      user = decodePart(userinfo);
    }
  }

  return { scheme, user, password, host: decodePart(host), port, path, query };
}

export function looksLikeUserinfoHost(raw: string): boolean {
  const text = (raw || "").trim();
  if (!text || text.includes("://") || text.includes(" ")) return false;
  if (!text.includes("@") || !text.split("@")[0].includes(":")) return false;
  const parsed = parseUrlAuthority(text);
  return Boolean(parsed.host && parsed.user);
}
