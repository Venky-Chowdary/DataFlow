// Scratch-store gate for the signup screenshot harness.
//
// The harness writes real rows into whatever store the API resolves —
// otp_challenges, pending_signups, mail_outbox, users. It must only ever
// run against a local/scratch Mongo, never production or a shared
// staging store.
//
// Local/scratch means: localhost, a 127.x/::1/0.0.0.0 address, a
// *.localhost name, or a single-label docker/compose service name
// ("mongo", "df-mongo"). Anything else refuses, unless the operator
// explicitly acknowledges a dedicated scratch store with
// DATAWRAP_SCREENSHOT_SCRATCH=1.

export function mongoUriIsLocal(uri) {
  const hostList = String(uri || "")
    .replace(/^[a-z][a-z0-9+.-]*:\/\//i, "")
    .split("/")[0]
    .split("@").pop()
    .split(",")
    .map((h) => {
      const v6 = h.match(/^\[([^\]]+)\]/);   // [::1]:port
      return (v6 ? v6[1] : h.split(":")[0]).toLowerCase();
    });
  const isLocal = (h) =>
    h === "localhost" || h === "::1" || h === "0.0.0.0" ||
    /^127\./.test(h) || h.endsWith(".localhost") ||
    (h !== "" && !h.includes("."));   // docker/compose service name
  return hostList.length > 0 && hostList.every(isLocal);
}

export function assertScratchMongo(env = process.env) {
  if (env.DATAWRAP_SCREENSHOT_SCRATCH === "1") return;
  const uri = env.MONGODB_URI || env.MONGO_URL
    || "mongodb://localhost:27017/";
  if (!mongoUriIsLocal(uri)) {
    console.error(
      "refusing to run: Mongo URI host(s) are not local/scratch. The " +
      "screenshot harness writes live challenge and outbox rows; point " +
      "MONGODB_URI at a local instance, or set " +
      "DATAWRAP_SCREENSHOT_SCRATCH=1 to acknowledge a dedicated " +
      "scratch store. Offending URI: " + uri);
    process.exit(1);
  }
}
