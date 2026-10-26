import { test } from "node:test";
import assert from "node:assert/strict";

import { HOP_BY_HOP, isHopByHop, stripHopByHop, buildForwardHeaders, buildResponseHeaders, headersToObject } from "../src/headers.ts";

test("the hop-by-hop set is exactly the connection-scoped headers", () => {
  assert.deepEqual([...HOP_BY_HOP].sort(), ["connection", "host", "keep-alive", "te", "trailer", "transfer-encoding", "upgrade"]);
});

test("isHopByHop is case-insensitive and covers the whole proxy-* family", () => {
  assert.equal(isHopByHop("Connection"), true);
  assert.equal(isHopByHop("TRANSFER-ENCODING"), true);
  assert.equal(isHopByHop("Proxy-Authorization"), true);
  assert.equal(isHopByHop("proxy-connection"), true);
  assert.equal(isHopByHop("Authorization"), false);
  assert.equal(isHopByHop("x-api-key"), false);
});

test("stripHopByHop drops connection-scoped headers and keeps everything else", () => {
  const inbound = new Headers({
    "connection": "keep-alive",
    "keep-alive": "timeout=5",
    "transfer-encoding": "chunked",
    "te": "trailers",
    "trailer": "Expires",
    "upgrade": "websocket",
    "proxy-authorization": "Basic zzz",
    "host": "gate.example.com",
    "authorization": "Bearer token",
    "x-api-key": "tracker-key",
    "content-type": "application/json",
  });

  const out = stripHopByHop(inbound);
  for (const name of ["connection", "keep-alive", "transfer-encoding", "te", "trailer", "upgrade", "proxy-authorization", "host"]) {
    assert.equal(out.get(name), null, `${name} must not reach the upstream`);
  }
  assert.equal(out.get("authorization"), "Bearer token");
  assert.equal(out.get("x-api-key"), "tracker-key");
  assert.equal(out.get("content-type"), "application/json");
  assert.equal(inbound.get("connection"), "keep-alive", "the caller's headers are not mutated");
});

test("buildForwardHeaders re-frames the body and forces identity encoding", () => {
  const inbound = new Headers({ "content-length": "42", "transfer-encoding": "chunked", "accept-encoding": "gzip, br" });
  const out = buildForwardHeaders(inbound, new URL("https://gate.example.com/via/tracker/api/v1/"));
  assert.equal(out.get("content-length"), null);
  assert.equal(out.get("transfer-encoding"), null);
  assert.equal(out.get("accept-encoding"), "identity");
});

test("buildForwardHeaders preserves the proxy's X-Forwarded-For and fills the gaps", () => {
  const withPrior = buildForwardHeaders(
    new Headers({ "x-forwarded-for": "198.51.100.3", "x-forwarded-proto": "https", "x-forwarded-host": "tracker.example.com" }),
    new URL("http://192.0.2.61:8080/via/tracker/api/v1/"),
  );
  assert.equal(withPrior.get("x-forwarded-for"), "198.51.100.3");
  assert.equal(withPrior.get("x-forwarded-proto"), "https");
  assert.equal(withPrior.get("x-forwarded-host"), "tracker.example.com");

  const direct = buildForwardHeaders(new Headers({ "x-real-ip": "192.0.2.163" }), new URL("http://192.0.2.61:8080/via/tracker/api/v1/"));
  assert.equal(direct.get("x-forwarded-for"), "192.0.2.163");
  assert.equal(direct.get("x-forwarded-proto"), "http");
  assert.equal(direct.get("x-forwarded-host"), "192.0.2.61:8080");

  const anonymous = buildForwardHeaders(new Headers(), new URL("http://192.0.2.61:8080/via/tracker/x"));
  assert.equal(anonymous.get("x-forwarded-for"), null, "never invent a client address");
});

test("buildResponseHeaders drops hop-by-hop and the stale content-length", () => {
  const upstream = new Headers({
    "content-type": "application/json",
    "content-length": "128",
    "connection": "close",
    "transfer-encoding": "chunked",
    "x-tracker-request-id": "abc",
  });
  const out = buildResponseHeaders(upstream);
  assert.equal(out.get("content-length"), null, "the actor runtime strips content-length from Worker responses anyway");
  assert.equal(out.get("connection"), null);
  assert.equal(out.get("transfer-encoding"), null);
  assert.equal(out.get("content-type"), "application/json");
  assert.equal(out.get("x-tracker-request-id"), "abc");
});

test("multiple Set-Cookie values survive as separate headers", () => {
  const upstream = new Headers();
  upstream.append("set-cookie", "a=1; Path=/");
  upstream.append("set-cookie", "b=2; Path=/");
  const out = buildResponseHeaders(upstream);
  const cookies = (out as unknown as { getSetCookie?: () => string[] }).getSetCookie?.() ?? [];
  assert.deepEqual(cookies, ["a=1; Path=/", "b=2; Path=/"]);
});

test("headersToObject flattens for DO storage", () => {
  const h = new Headers({ "content-type": "application/json", "x-a": "1" });
  assert.deepEqual(headersToObject(h), { "content-type": "application/json", "x-a": "1" });
});
