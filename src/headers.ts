// Header handling for the /via proxy path.
//
// Hop-by-hop headers are connection-scoped (RFC 9110 7.6.1): they describe the single TCP hop
// between the caller and the gate, not the message, so they must never be copied onto the
// upstream request or back onto the client response.

export const HOP_BY_HOP: ReadonlySet<string> = new Set([
  "connection",
  "keep-alive",
  "transfer-encoding",
  "te",
  "trailer",
  "upgrade",
  "host",
]);

/** True for hop-by-hop headers, including the whole `proxy-*` family. */
export function isHopByHop(name: string): boolean {
  const n = name.toLowerCase();
  return HOP_BY_HOP.has(n) || n.startsWith("proxy-");
}

/** Copy of `headers` with every hop-by-hop header removed. Input is not mutated. */
export function stripHopByHop(headers: Headers): Headers {
  const out = new Headers();
  headers.forEach((value, name) => {
    if (!isHopByHop(name)) out.append(name, value);
  });
  return out;
}

/**
 * Headers for the upstream request: caller's headers minus hop-by-hop, minus content-length
 * (the body is re-framed by fetch), plus the X-Forwarded-* trio.
 *
 * `accept-encoding: identity` is forced so the upstream never compresses. The gate re-emits the
 * upstream body verbatim; asking for identity removes any chance of a content-encoding header
 * surviving a body that the runtime already decoded.
 */
export function buildForwardHeaders(inbound: Headers, originalUrl: URL): Headers {
  const out = stripHopByHop(inbound);
  out.delete("content-length");
  out.set("accept-encoding", "identity");

  // the proxy (the proxy host) is the real edge and already sets these. Preserve what it sent; only fill gaps.
  const priorFor = inbound.get("x-forwarded-for");
  const peer = inbound.get("cf-connecting-ip") ?? inbound.get("x-real-ip");
  const forwardedFor = priorFor ?? peer;
  if (forwardedFor) out.set("x-forwarded-for", forwardedFor);

  out.set("x-forwarded-proto", inbound.get("x-forwarded-proto") ?? originalUrl.protocol.replace(":", ""));
  out.set("x-forwarded-host", inbound.get("x-forwarded-host") ?? originalUrl.host);
  return out;
}

/**
 * Headers for the response sent back to the caller: upstream headers minus hop-by-hop and
 * content-length (the actor runtime strips content-length from Worker responses anyway - see
 * the runtime's compatibility notes - so never let a stale one through).
 * Set-Cookie is re-appended one value at a time so multiple cookies are not comma-folded.
 */
export function buildResponseHeaders(upstream: Headers): Headers {
  const out = new Headers();
  upstream.forEach((value, name) => {
    const n = name.toLowerCase();
    if (isHopByHop(n) || n === "content-length" || n === "set-cookie") return;
    out.append(name, value);
  });
  const getSetCookie = (upstream as unknown as { getSetCookie?: () => string[] }).getSetCookie;
  const cookies = typeof getSetCookie === "function" ? getSetCookie.call(upstream) : [];
  if (cookies.length > 0) {
    for (const c of cookies) out.append("set-cookie", c);
  } else {
    const single = upstream.get("set-cookie");
    if (single) out.append("set-cookie", single);
  }
  return out;
}

/** Plain object form of a Headers, for storing a dedupe response in DO storage. */
export function headersToObject(headers: Headers): Record<string, string> {
  const out: Record<string, string> = {};
  headers.forEach((value, name) => {
    out[name] = out[name] ? `${out[name]}, ${value}` : value;
  });
  return out;
}
