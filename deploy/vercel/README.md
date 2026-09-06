# RPC Proxy on Vercel

This template forwards `POST /rpc/{chain}` to an **existing RPC Proxy backend** on Render, Railway, or Kubernetes. It does not run the Go server, Redis, or background head coordinators. Use the backend's `wss://` URL directly for `/ws/{chain}` subscriptions.

[![Deploy with Vercel](https://vercel.com/button)](https://vercel.com/new/clone?repository-url=https%3A%2F%2Fgithub.com%2Ffiberevm%2Frpc-proxy%2Ftree%2Fmain%2Fdeploy%2Fvercel&env=RPC_PROXY_ORIGIN&envDescription=HTTPS%20origin%20of%20your%20running%20RPC%20Proxy%20backend%2C%20without%20a%20trailing%20slash&project-name=rpc-proxy-http&repository-name=rpc-proxy-http)

1. Deploy the backend and check its private `/health/ready` endpoint.
2. Click the button and set `RPC_PROXY_ORIGIN` to the HTTPS origin, such as `https://your-rpc-proxy.onrender.com`, without a trailing slash, path, query, or credentials.
3. Send JSON-RPC POST requests to `https://your-project.vercel.app/rpc/ethereum`. All other paths and methods return 404. This template has no homepage.

When importing the full repository manually, set the Vercel project root directory to `deploy/vercel` and framework to **Other**. The button clones this subdirectory automatically.

The build emits [Vercel routing configuration](https://vercel.com/docs/build-output-api/configuration), with CDN caching disabled. Request bodies and the backend's response headers pass through the external rewrite. No provider credentials or Redis connection are needed on Vercel. Redeploy after changing `RPC_PROXY_ORIGIN`.

Local build check (Node.js required):

```sh
RPC_PROXY_ORIGIN=https://your-rpc-proxy.onrender.com node build.mjs
```

Put authentication and rate limits at your ingress and protect the backend's public hostname as well. The template does not add either. Vercel's [external rewrite limits](https://vercel.com/docs/rewrites) apply; use the backend directly when its full HTTP or WebSocket behavior is needed.
