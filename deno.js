// A lightweight Deno WebSocket gateway service.
// Provides realtime streaming between WebSocket clients and backend services,
// plus an HTTP fetch helper. Configure with AUTH_TOKEN and ALLOWED_DOMAINS.
//
// Env:
//   AUTH_TOKEN       - shared secret required for all requests (required)
//   ALLOWED_DOMAINS  - comma-separated parent domains permitted as backends
//                      (default: common Google service domains)
//   PORT             - http listen port (injected by the platform)

const DEFAULT_DOMAINS = "google.com,googleapis.com,googleusercontent.com,gstatic.com";
const AUTH_TOKEN = Deno.env.get("AUTH_TOKEN") || "";
const ALLOWED_DOMAINS = (Deno.env.get("ALLOWED_DOMAINS") || DEFAULT_DOMAINS)
  .split(",").map(s => s.trim().toLowerCase()).filter(Boolean);

function domainAllowed(host) {
  host = (host || "").toLowerCase();
  return ALLOWED_DOMAINS.some(d => host === d || host.endsWith("." + d));
}

function json(obj, status) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json" },
  });
}

Deno.serve({ port: Number(Deno.env.get("PORT") || 8080) }, async (req) => {
  const url = new URL(req.url);
  const upgrade = req.headers.get("upgrade") || "";

  // ---- plain HTTP paths ----
  if (upgrade.toLowerCase() !== "websocket") {
    // health / info
    if (url.pathname === "/") {
      return json({ ok: true, service: "deno-realtime-gateway", backends: ALLOWED_DOMAINS.length }, 200);
    }
    // HTTP fetch helper: GET /fetch?k=...&url=...
    if (url.pathname === "/fetch") {
      const token = url.searchParams.get("k") || "";
      const target = url.searchParams.get("url") || "";
      if (!AUTH_TOKEN || token !== AUTH_TOKEN) return json({ error: "unauthorized" }, 401);
      if (!/^https?:\/\//.test(target)) return json({ error: "invalid url" }, 400);
      const host = new URL(target).hostname || "";
      if (!domainAllowed(host)) return json({ error: "domain not allowed: " + host }, 403);
      const h = new Headers(req.headers);
      for (const k of ["host", "cookie", "connection", "keep-alive", "transfer-encoding", "accept-encoding"]) h.delete(k);
      try {
        const r = await fetch(target, {
          method: req.method,
          headers: h,
          body: ["GET", "HEAD"].includes(req.method) ? undefined : await req.arrayBuffer(),
          redirect: "manual",
        });
        const oh = new Headers(r.headers);
        for (const k of ["content-encoding", "content-length", "transfer-encoding"]) oh.delete(k);
        return new Response(r.body, { status: r.status, headers: oh });
      } catch (e) {
        return json({ error: String(e).slice(0, 200) }, 502);
      }
    }
    return new Response("gateway online", { status: 404 });
  }

  // ---- WebSocket streaming endpoint: /stream (filtered) or /all (any backend) ----
  if (url.pathname !== "/stream" && url.pathname !== "/all") return new Response("not found", { status: 404 });
  const allowAll = url.pathname === "/all";
  const { socket: ws, response } = Deno.upgradeWebSocket(req);
  let backend = null;

  const cleanup = () => {
    try { backend && backend.close(); } catch {}
    try { ws.close(); } catch {}
  };

  ws.onmessage = async (ev) => {
    if (!backend) {
      // first frame: { k: token, h: host, p: port }
      if (typeof ev.data !== "string") return cleanup();
      let init;
      try { init = JSON.parse(ev.data); } catch { return cleanup(); }
      if (!AUTH_TOKEN || init.k !== AUTH_TOKEN || (!allowAll && !domainAllowed(init.h))) {
        ws.send(JSON.stringify({ ok: false, error: "unauthorized" }));
        return cleanup();
      }
      try {
        backend = await Deno.connect({ hostname: init.h, port: Number(init.p) || 443 });
      } catch (e) {
        ws.send(JSON.stringify({ ok: false, error: "connect failed: " + String(e).slice(0, 120) }));
        return cleanup();
      }
      ws.send(JSON.stringify({ ok: true }));
      streamToClient(backend, ws);
      return;
    }
    // subsequent frames: raw bytes toward backend
    if (ev.data instanceof ArrayBuffer) {
      try { backend.write(new Uint8Array(ev.data)); } catch { cleanup(); }
    } else if (ev.data instanceof Uint8Array) {
      try { backend.write(ev.data); } catch { cleanup(); }
    }
  };

  ws.onerror = cleanup;
  ws.onclose = cleanup;

  async function streamToClient(backend, ws) {
    const buf = new Uint8Array(65536);
    try {
      while (true) {
        const n = await backend.read(buf);
        if (n === null) break;
        ws.send(buf.slice(0, n));
      }
    } catch {}
    cleanup();
  }

  return response;
});
