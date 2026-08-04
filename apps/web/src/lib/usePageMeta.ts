import { useEffect } from "react";
import {
  DEFAULT_KEYWORDS,
  formatDocumentTitle,
  resolveSiteUrl,
  type PageMeta,
} from "./seo";

function upsertMeta(attr: "name" | "property", key: string, content: string) {
  if (!content) return;
  let el = document.head.querySelector(`meta[${attr}="${key}"]`) as HTMLMetaElement | null;
  if (!el) {
    el = document.createElement("meta");
    el.setAttribute(attr, key);
    document.head.appendChild(el);
  }
  el.content = content;
}

function upsertLink(rel: string, href: string, extra?: Record<string, string>) {
  if (!href) return;
  let el = document.head.querySelector(`link[rel="${rel}"]`) as HTMLLinkElement | null;
  if (!el) {
    el = document.createElement("link");
    el.rel = rel;
    document.head.appendChild(el);
  }
  el.href = href;
  if (extra) {
    for (const [k, v] of Object.entries(extra)) {
      el.setAttribute(k, v);
    }
  }
}

/** Apply document title, favicon links, and SEO meta tags for the active view. */
export function applyPageMeta(meta: PageMeta) {
  const siteUrl = resolveSiteUrl();
  const title = formatDocumentTitle(meta.title);
  const description = meta.description;
  const keywords = meta.keywords || DEFAULT_KEYWORDS;
  const robots = meta.robots ?? "index, follow";
  const ogType = meta.ogType ?? "website";
  const ogImage = `${siteUrl}/og-image.png`;
  const pageUrl = meta.canonicalPath
    ? meta.canonicalPath.startsWith("#")
      ? `${siteUrl}/${meta.canonicalPath}`
      : meta.canonicalPath.startsWith("http")
        ? meta.canonicalPath
        : `${siteUrl}${meta.canonicalPath.startsWith("/") ? "" : "/"}${meta.canonicalPath}`
    : siteUrl;

  document.title = title;
  document.documentElement.lang = "en";

  upsertMeta("name", "description", description);
  upsertMeta("name", "keywords", keywords);
  upsertMeta("name", "robots", robots);
  upsertMeta("name", "author", "Datawrap");
  upsertMeta("name", "application-name", "Datawrap");
  upsertMeta("name", "apple-mobile-web-app-title", "Datawrap");
  upsertMeta("name", "theme-color", "#0f766e");

  upsertMeta("property", "og:site_name", "Datawrap");
  upsertMeta("property", "og:title", title);
  upsertMeta("property", "og:description", description);
  upsertMeta("property", "og:type", ogType);
  upsertMeta("property", "og:url", pageUrl);
  upsertMeta("property", "og:image", ogImage);
  upsertMeta("property", "og:image:alt", "Datawrap — Universal data transfer platform");
  upsertMeta("property", "og:locale", "en_US");
  upsertMeta("property", "og:image:type", "image/png");

  upsertMeta("name", "twitter:card", "summary_large_image");
  upsertMeta("name", "twitter:title", title);
  upsertMeta("name", "twitter:description", description);
  upsertMeta("name", "twitter:image", ogImage);
  upsertMeta("name", "twitter:image:alt", "Datawrap — Universal data transfer platform");

  upsertLink("canonical", pageUrl);
  upsertLink("icon", "/favicon.svg", { type: "image/svg+xml" });
  upsertLink("apple-touch-icon", "/apple-touch-icon.png");
  upsertLink("manifest", "/site.webmanifest");

  upsertJsonLd(meta, siteUrl, title, description);
}

const JSON_LD_ID = "datawrap-jsonld";

function upsertJsonLd(meta: PageMeta, siteUrl: string, title: string, description: string) {
  const existing = document.getElementById(JSON_LD_ID);
  // Remove legacy id from pre-rebrand sessions.
  document.getElementById("dataflow-jsonld")?.remove();
  if (meta.robots?.includes("noindex")) {
    existing?.remove();
    return;
  }

  const pageUrl = meta.canonicalPath
    ? `${siteUrl}${meta.canonicalPath.startsWith("/") || meta.canonicalPath.startsWith("#") ? "" : "/"}${meta.canonicalPath}`
    : siteUrl;
  const absolutePage =
    pageUrl.includes("#")
      ? pageUrl
      : pageUrl.startsWith("http")
        ? pageUrl
        : `${siteUrl}${pageUrl}`;

  const payload = {
    "@context": "https://schema.org",
    "@graph": [
      {
        "@type": "WebSite",
        "@id": `${siteUrl}/#website`,
        url: siteUrl,
        name: "Datawrap",
        alternateName: ["Datawrap Pilot", "Datawrap Transfer Studio"],
        description:
          "Integrity-first data transfer platform — migrate databases, sync files, and move data with AI semantic mapping, CDC, quarantine, and 8 preflight gates.",
        inLanguage: "en-US",
        publisher: { "@id": `${siteUrl}/#organization` },
      },
      {
        "@type": "WebPage",
        "@id": `${absolutePage}#webpage`,
        url: absolutePage,
        name: title,
        description,
        isPartOf: { "@id": `${siteUrl}/#website` },
        inLanguage: "en-US",
      },
      {
        "@type": "SoftwareApplication",
        "@id": `${siteUrl}/#software`,
        name: "Datawrap",
        applicationCategory: "BusinessApplication",
        operatingSystem: "Web",
        description,
        url: siteUrl,
        image: `${siteUrl}/datawrap-mark.png`,
        offers: {
          "@type": "Offer",
          price: "0",
          priceCurrency: "USD",
        },
        featureList: [
          "Database migration",
          "File to database transfer",
          "Semantic column mapping",
          "8 preflight validation gates",
          "Scheduled pipelines",
          "MCP server integration",
          "Datawrap Pilot natural-language ops",
        ],
      },
      {
        "@type": "Organization",
        "@id": `${siteUrl}/#organization`,
        name: "Datawrap",
        url: siteUrl,
        logo: {
          "@type": "ImageObject",
          url: `${siteUrl}/datawrap-mark.png`,
        },
        sameAs: [],
      },
    ],
  };

  let el = existing as HTMLScriptElement | null;
  if (!el) {
    el = document.createElement("script");
    el.id = JSON_LD_ID;
    el.type = "application/ld+json";
    document.head.appendChild(el);
  }
  el.textContent = JSON.stringify(payload);
}

export function usePageMeta(meta: PageMeta) {
  useEffect(() => {
    applyPageMeta(meta);
  }, [meta.title, meta.description, meta.keywords, meta.robots, meta.ogType, meta.canonicalPath]);
}
