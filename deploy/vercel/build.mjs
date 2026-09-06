import { mkdir, writeFile } from "node:fs/promises";

// Build-time configuration keeps the destination fixed for every request.
// Use the HTTPS origin of an already-running RPC Proxy, without a trailing slash.
const backendOrigin = process.env.RPC_PROXY_ORIGIN;
if (!backendOrigin) {
  throw new Error("RPC_PROXY_ORIGIN is required; deploy the RPC Proxy backend first");
}

let backendURL;
try {
  backendURL = new URL(backendOrigin);
} catch {
  throw new Error("RPC_PROXY_ORIGIN must be a valid HTTPS origin");
}

if (backendURL.protocol !== "https:" || backendOrigin !== backendURL.origin) {
  throw new Error("RPC_PROXY_ORIGIN must be an HTTPS origin without credentials, a path, a trailing slash, a query, or a fragment");
}

// Vercel's Build Output API emits routing only: the Go coordinator and Redis
// remain on Render, Railway, or Kubernetes. Never cache head-sensitive RPCs.
const deploymentConfig = {
  version: 3,
  routes: [
    {
      src: "^/rpc/([^/]+)$",
      methods: ["POST"],
      dest: `${backendOrigin}/rpc/$1`,
      headers: {
        "Cache-Control": "no-store",
        "CDN-Cache-Control": "no-store",
        "Vercel-CDN-Cache-Control": "no-store",
        "x-vercel-enable-rewrite-caching": "0",
      },
    },
    // Do not forward admin endpoints, arbitrary paths, or WebSocket upgrades.
    { src: "^/.*$", status: 404 },
  ],
};

await mkdir(".vercel/output/static", { recursive: true });
await writeFile(".vercel/output/config.json", `${JSON.stringify(deploymentConfig, null, 2)}\n`);
