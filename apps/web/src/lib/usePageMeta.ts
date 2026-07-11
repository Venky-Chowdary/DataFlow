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
  const ogImage = `${siteUrl}/og-image.svg`;

  document.title = title;
  document.documentElement.lang = "en";

  upsertMeta("name", "description", description);
  upsertMeta("name", "keywords", keywords);
  upsertMeta("name", "robots", robots);
  upsertMeta("name", "application-name", "DataFlow");
  upsertMeta("name", "apple-mobile-web-app-title", "DataFlow");
  upsertMeta("name", "theme-color", "#0f766e");

  upsertMeta("property", "og:site_name", "DataFlow");
  upsertMeta("property", "og:title", title);
  upsertMeta("property", "og:description", description);
  upsertMeta("property", "og:type", ogType);
  upsertMeta("property", "og:url", siteUrl);
  upsertMeta("property", "og:image", ogImage);
  upsertMeta("property", "og:image:alt", "DataFlow — Universal data transfer platform");
  upsertMeta("property", "og:locale", "en_US");

  upsertMeta("name", "twitter:card", "summary_large_image");
  upsertMeta("name", "twitter:title", title);
  upsertMeta("name", "twitter:description", description);
  upsertMeta("name", "twitter:image", ogImage);
  upsertMeta("name", "twitter:image:alt", "DataFlow — Universal data transfer platform");

  upsertLink("canonical", siteUrl);
  upsertLink("icon", "/favicon.svg", { type: "image/svg+xml" });
  upsertLink("apple-touch-icon", "/apple-touch-icon.svg");
  upsertLink("manifest", "/site.webmanifest");

  upsertJsonLd(meta, siteUrl, title, description);
}

const JSON_LD_ID = "dataflow-jsonld";

function upsertJsonLd(meta: PageMeta, siteUrl: string, title: string, description: string) {
  const existing = document.getElementById(JSON_LD_ID);
  if (meta.robots?.includes("noindex")) {
    existing?.remove();
    return;
  }

  const payload = {
    "@context": "https://schema.org",
    "@graph": [
      {
        "@type": "WebSite",
        "@id": `${siteUrl}/#website`,
        url: siteUrl,
        name: "DataFlow",
        description,
        inLanguage: "en-US",
      },
      {
        "@type": "SoftwareApplication",
        "@id": `${siteUrl}/#software`,
        name: "DataFlow",
        applicationCategory: "BusinessApplication",
        operatingSystem: "Web",
        description,
        url: siteUrl,
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
        ],
      },
      {
        "@type": "Organization",
        "@id": `${siteUrl}/#organization`,
        name: "DataFlow",
        url: siteUrl,
        logo: `${siteUrl}/favicon.svg`,
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
  }, [meta.title, meta.description, meta.keywords, meta.robots, meta.ogType]);
}
